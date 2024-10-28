
#include "s2_point_ops.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2geography/op/cell.h"
#include "s2geography/op/point.h"

#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

struct S2CellFromPoint {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_cell_from_point", {Types::S2_POINT()}, Types::S2_CELL(),
                             Execute);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    using s2geography::op::point::Point;
    s2geography::op::cell::FromPoint op;

    auto& children = StructVector::GetEntries(args.data[0]);

    TernaryExecutor::Execute<double, double, double, int64_t>(
        *children[0], *children[1], *children[2], result, args.size(),
        [&](double x, double y, double z) {
          Point pt{x, y, z};
          return static_cast<int64_t>(op.ExecuteScalar(pt));
        });
  }
};

template <typename Op>
struct S2CellToString {
  static void Register(DatabaseInstance& instance, const char* name) {
    auto fn = ScalarFunction(name, {Types::S2_CELL()}, LogicalType::VARCHAR, Execute);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    Op op;
    UnaryExecutor::Execute<int64_t, string_t>(
        args.data[0], result, args.size(), [&](int64_t arg0) {
          std::string_view str = op.ExecuteScalar(arg0);
          return StringVector::AddString(
              result, string_t{str.data(), static_cast<uint32_t>(str.size())});
        });
  }
};

template <typename Op>
struct S2CellToDouble {
  static void Register(DatabaseInstance& instance, const char* name) {
    auto fn = ScalarFunction(name, {Types::S2_CELL()}, LogicalType::DOUBLE, Execute);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    Op op;
    UnaryExecutor::Execute<int64_t, double>(
        args.data[0], result, args.size(),
        [&](int64_t arg0) { return op.ExecuteScalar(arg0); });
  }
};

template <typename Op>
struct S2CellToInt8 {
  static void Register(DatabaseInstance& instance, const char* name) {
    auto fn = ScalarFunction(name, {Types::S2_CELL()}, LogicalType::TINYINT, Execute);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    Op op;
    UnaryExecutor::Execute<int64_t, int8_t>(
        args.data[0], result, args.size(),
        [&](int64_t arg0) { return op.ExecuteScalar(arg0); });
  }
};

}  // namespace

void RegisterS2CellOps(DatabaseInstance& instance) {
  using namespace s2geography::op::cell;

  S2CellFromPoint::Register(instance);
  S2CellToString<ToDebugString>::Register(instance, "s2_cell_debug_string");
  S2CellToString<ToToken>::Register(instance, "s2_cell_token");
  S2CellToDouble<Area>::Register(instance, "s2_cell_area");
  S2CellToDouble<AreaApprox>::Register(instance, "s2_cell_area_approx");
  S2CellToInt8<Level>::Register(instance, "s2_cell_level");
}

}  // namespace duckdb_s2
}  // namespace duckdb
