

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
}  // namespace

void RegisterS2GeographyBounds(DatabaseInstance& instance) {
  S2Covering::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
