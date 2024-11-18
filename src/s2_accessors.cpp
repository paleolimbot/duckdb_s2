
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/s2cell_union.h"
#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

struct S2IsEmtpy {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_isempty", {Types::GEOGRAPHY()}, LogicalType::BOOLEAN,
                             ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& geogs, Vector& result, idx_t count) {}
};

void RegisterS2GeographyPredicates(DatabaseInstance& instance) {}

}  // namespace duckdb_s2
}  // namespace duckdb