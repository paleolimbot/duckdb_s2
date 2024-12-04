

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

          func.SetDescription("Returns the S2 cell covering of the geography.");
          func.SetExample("SELECT s2_covering('POINT(0 0)') AS covering;");

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
              "Returns the S2 cell covering of the geography with a fixed level.");
          func.SetExample(R"(
            SELECT s2_covering_fixed_level('POINT(0 0)', 4) AS covering;
            ----
            SELECT s2_covering_fixed_level('POINT(0 0)', 5) AS covering;
          )"
          );

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
}  // namespace

void RegisterS2GeographyBounds(DatabaseInstance& instance) {
  S2Covering::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
