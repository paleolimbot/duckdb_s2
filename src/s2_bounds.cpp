

#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/s2earth.h"
#include "s2/s2region_coverer.h"
#include "s2geography/accessors.h"

#include "s2/s2cell_union.h"
#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

#include "function_builder.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

struct S2Covering {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_covering", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(Types::S2_CELL_UNION());
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(R"(
Returns the S2 cell covering of the geography.

A covering is a deterministic S2_CELL_UNION (i.e., list of S2_CELLs) that
completely covers a geography. This is useful as a compact approximation
of a geography that can be used to select possible candidates for intersection.

Note that an S2_CELL_UNION is a thin wrapper around a LIST of S2_CELL, such
that DuckDB LIST functions can be used to unnest, extract, or otherwise
interact with the result.

See the [Cell Operators](#cellops) section for ways to interact with cells.
)");
          func.SetExample(R"(
SELECT s2_covering(s2_data_country('Germany')) AS covering;
----
-- Find countries that might contain Berlin
SELECT name as country, cell FROM (
  SELECT name, UNNEST(s2_covering(geog)) as cell
  FROM s2_data_countries()
) WHERE
s2_cell_contains(cell, s2_data_city('Berlin')::S2_CELL_CENTER::S2_CELL);
)");

          func.SetTag("ext", "geography");
          func.SetTag("category", "bounds");
        });

    FunctionBuilder::RegisterScalar(
        instance, "s2_covering_fixed_level", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.AddParameter("fixed_level", LogicalType::INTEGER);
            variant.SetReturnType(Types::S2_CELL_UNION());
            variant.SetFunction(ExecuteFnFixedLevel);
          });

          func.SetDescription(
              R"(
Returns the S2 cell covering of the geography with a fixed level.

See `[s2_covering](#s2_covering)` for further detail and examples.
)");
          func.SetExample(R"(
SELECT s2_covering_fixed_level(s2_data_country('Germany'), 3) AS covering;
----
SELECT s2_covering_fixed_level(s2_data_country('Germany'), 4) AS covering;
          )");

          func.SetTag("ext", "geography");
          func.SetTag("category", "bounds");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    S2RegionCoverer coverer;
    Execute(args.data[0], result, args.size(), coverer);
  }

  static inline void ExecuteFnFixedLevel(DataChunk& args, ExpressionState& state,
                                         Vector& result) {
    Vector& max_cells_param = args.data[1];
    if (max_cells_param.GetVectorType() != VectorType::CONSTANT_VECTOR) {
      throw InvalidInputException("s2_covering_fixed_level(): level must be a constant");
    }

    int fixed_level = max_cells_param.GetValue(0).GetValue<int>();
    if (fixed_level < 0 || fixed_level > S2CellId::kMaxLevel) {
      throw InvalidInputException(
          "s2_covering_fixed_level(): level must be between 0 and 30");
    }

    S2RegionCoverer coverer;
    coverer.mutable_options()->set_fixed_level(fixed_level);
    Execute(args.data[0], result, args.size(), coverer);
  }

  static void Execute(Vector& source, Vector& result, idx_t count,
                      S2RegionCoverer& coverer) {
    ListVector::Reserve(result, count * coverer.options().max_cells());
    uint64_t offset = 0;

    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, list_entry_t>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);
          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return list_entry_t{0, 0};
          }

          switch (decoder.tag.kind) {
            case s2geography::GeographyKind::CELL_CENTER: {
              decoder.DecodeTagAndCovering(geog_str);
              S2CellId cell_id =
                  decoder.covering[0].parent(coverer.options().max_level());
              ListVector::PushBack(result, Value::UBIGINT(cell_id.id()));
              list_entry_t out{offset, 1};
              offset += 1;
              return out;
            }

            default: {
              auto geog = decoder.Decode(geog_str);
              S2CellUnion covering = coverer.GetCovering(*geog->Region());
              for (const auto cell_id : covering) {
                ListVector::PushBack(result, Value::UBIGINT(cell_id.id()));
              }

              list_entry_t out{offset, covering.size()};
              offset += out.length;
              return out;
            }
          }
        });
  }
};

struct S2BoundsRect {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_bounds_rect", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(Types::BOX_LNGLAT());
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(
              R"(
Returns the bounds of the input geography as a box with Cartesian edges.

The output xmin may be greater than xmax if the geography crosses the
antimeridian.
)");
          func.SetExample(R"(
SELECT s2_bounds_rect(s2_data_country('Germany')) as rect;
----
SELECT s2_bounds_rect(s2_data_country('Fiji')) as rect;
          )");

          func.SetTag("ext", "geography");
          func.SetTag("category", "bounds");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    auto count = args.size();
    auto& input = args.data[0];
    auto& struct_vec = StructVector::GetEntries(result);
    auto min_x_data = FlatVector::GetData<double>(*struct_vec[0]);
    auto min_y_data = FlatVector::GetData<double>(*struct_vec[1]);
    auto max_x_data = FlatVector::GetData<double>(*struct_vec[2]);
    auto max_y_data = FlatVector::GetData<double>(*struct_vec[3]);

    GeographyDecoder decoder;

    UnifiedVectorFormat input_vdata;
    input.ToUnifiedFormat(count, input_vdata);
    auto input_data = UnifiedVectorFormat::GetData<string_t>(input_vdata);

    for (idx_t i = 0; i < count; i++) {
      auto row_idx = input_vdata.sel->get_index(i);
      if (input_vdata.validity.RowIsValid(row_idx)) {
        auto& blob = input_data[row_idx];

        decoder.DecodeTag(blob);
        if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
          // Empty input, return null. This ensures that we never have to check
          // for nan, nan, nan, nan before doing anything with a (non null)
          // value.
          FlatVector::SetNull(result, i, true);
        } else if (decoder.tag.kind == s2geography::GeographyKind::CELL_CENTER) {
          uint64_t cell_id = LittleEndian::Load64(blob.GetData() + 4);
          S2CellId cell(cell_id);
          S2LatLng pt = cell.ToLatLng();
          min_x_data[i] = pt.lng().degrees();
          min_y_data[i] = pt.lat().degrees();
          max_x_data[i] = pt.lng().degrees();
          max_y_data[i] = pt.lat().degrees();
        } else {
          auto geog = decoder.Decode(blob);
          S2LatLngRect rect = geog->Region()->GetRectBound();
          min_x_data[i] = rect.lng_lo().degrees();
          min_y_data[i] = rect.lat_lo().degrees();
          max_x_data[i] = rect.lng_hi().degrees();
          max_y_data[i] = rect.lat_hi().degrees();
        }
      } else {
        // Null input, return null
        FlatVector::SetNull(result, i, true);
      }
    }

    if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
      result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
  }
};

struct BoundsAggState {
  GeographyDecoder decoder;
  S2LatLngRect rect;
};

struct S2BoundsRectAgg {
  template <class STATE>
  static void Initialize(STATE& state) {
    state.rect = S2LatLngRect::Empty();
  }

  template <class STATE, class OP>
  static void Combine(const STATE& source, STATE& target, AggregateInputData&) {
    target.rect = target.rect.Union(source.rect);
  }

  template <class INPUT_TYPE, class STATE, class OP>
  static void Operation(STATE& state, const INPUT_TYPE& input, AggregateUnaryInput&) {
    state.decoder.DecodeTag(input);
    if (state.decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
      return;
    }

    if (state.decoder.tag.kind == s2geography::GeographyKind::CELL_CENTER) {
      uint64_t cell_id = LittleEndian::Load64(input.GetData() + 4);
      S2CellId cell(cell_id);
      S2LatLng pt = cell.ToLatLng();
      S2LatLngRect rect(pt, pt);
      state.rect = state.rect.Union(rect);
    } else {
      auto geog = state.decoder.Decode(input);
      S2LatLngRect rect = geog->Region()->GetRectBound();
      state.rect = state.rect.Union(rect);
    }
  }

  template <class INPUT_TYPE, class STATE, class OP>
  static void ConstantOperation(STATE& state, const INPUT_TYPE& input,
                                AggregateUnaryInput& agg, idx_t) {
    Operation<INPUT_TYPE, STATE, OP>(state, input, agg);
  }

  template <class T, class STATE>
  static void Finalize(STATE& state, T& target, AggregateFinalizeData& finalize_data) {
    if (state.rect.is_empty()) {
      finalize_data.ReturnNull();
    } else {
      S2LatLngRect out = state.rect;
      std::string out_str = std::string("[") + std::to_string(out.lng_lo().degrees()) +
                            ", " + std::to_string(out.lat_lo().degrees()) + ", " +
                            std::to_string(out.lng_hi().degrees()) + ", " +
                            std::to_string(out.lat_hi().degrees()) + "]";

      target = StringVector::AddString(finalize_data.result, out_str);
    }
  }

  static bool IgnoreNull() { return true; }
};

void RegisterAgg(DatabaseInstance& instance) {
  auto function = AggregateFunction::UnaryAggregate<BoundsAggState, string_t, string_t,
                                                    S2BoundsRectAgg>(
      Types::GEOGRAPHY(), LogicalType::VARCHAR);

  // Register the function
  function.name = "s2_bounds_rect_agg";
  ExtensionUtil::RegisterFunction(instance, function);
}

}  // namespace

void RegisterS2GeographyBounds(DatabaseInstance& instance) {
  S2Covering::Register(instance);
  S2BoundsRect::Register(instance);
  RegisterAgg(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
