
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/s2cell_union.h"
#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

struct S2MayIntersect {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_mayintersect", {Types::GEOGRAPHY(), Types::GEOGRAPHY()},
                             LogicalType::BOOLEAN, ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], args.data[1], result, args.size());
  }

  static inline void Execute(Vector& lhs, Vector& rhs, Vector& result, idx_t count) {
    GeographyDecoder lhs_decoder;
    GeographyDecoder rhs_decoder;
    std::vector<S2CellId> intersection;

    BinaryExecutor::Execute<string_t, string_t, bool>(
        lhs, rhs, result, count, [&](string_t lhs_str, string_t rhs_str) {
          lhs_decoder.DecodeTagAndCovering(lhs_str);
          if (lhs_decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return false;
          }

          rhs_decoder.DecodeTagAndCovering(rhs_str);
          if (rhs_decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return false;
          }

          // We don't currently omit coverings but in case we do by accident,
          // an omitted covering *might* intersect since it was just not generated.
          if (lhs_decoder.covering.empty() || rhs_decoder.covering.empty()) {
            return true;
          }

          S2CellUnion::GetIntersection(lhs_decoder.covering, rhs_decoder.covering,
                                       &intersection);
          return !intersection.empty();
        });
  }
};

void RegisterS2GeographyPredicates(DatabaseInstance& instance) {
  S2MayIntersect::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
