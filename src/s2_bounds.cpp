

#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/s2earth.h"
#include "s2/s2region_coverer.h"
#include "s2geography/accessors.h"

#include "s2/s2cell_union.h"
#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

struct S2Covering {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_covering", {Types::GEOGRAPHY()}, Types::S2_CELL_UNION(),
                             ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& source, Vector& result, idx_t count) {
    // Should make this a function parameter
    int max_cells = 8;

    ListVector::Reserve(result, count * max_cells);
    uint64_t offset = 0;

    GeographyDecoder decoder;
    S2RegionCoverer coverer;
    coverer.mutable_options()->set_max_cells(max_cells);

    UnaryExecutor::Execute<string_t, list_entry_t>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);
          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return list_entry_t{0, 0};
          }

          switch (decoder.tag.kind) {
            case s2geography::GeographyKind::CELL_CENTER: {
              decoder.DecodeTagAndCovering(geog_str);
              ListVector::PushBack(result, Value::UBIGINT(decoder.covering[0].id()));
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
