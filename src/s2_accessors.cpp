
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/s2cell_union.h"
#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

struct S2IsEmpty {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_isempty", {Types::GEOGRAPHY()}, LogicalType::BOOLEAN,
                             ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, bool>(source, result, count, [&](string_t geog_str) {
      decoder.DecodeTag(geog_str);
      return decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty;
    });
  }
};

}

void RegisterS2GeographyAccessors(DatabaseInstance& instance) {
  S2IsEmpty::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
