
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

struct S2CellCenter {
  static void Register(DatabaseInstance& instance) {
    auto fn =
        ScalarFunction("s2_cell_center", {Types::S2_CELL()}, Types::S2_POINT(), Execute);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    using s2geography::op::point::Point;
    s2geography::op::cell::CellCenter op;
    idx_t count = args.size();

    auto& cell_id = args.data[0];
    cell_id.Flatten(count);
    auto cell_ids = reinterpret_cast<uint64_t*>(cell_id.GetData());

    auto& children = StructVector::GetEntries(result);
    auto& x_child = children[0];
    auto& y_child = children[1];
    auto& z_child = children[2];

    for (idx_t i = 0; i < count; i++) {
      Point pt = op.ExecuteScalar(cell_ids[i]);
      x_child->SetValue(i, pt[0]);
      y_child->SetValue(i, pt[1]);
      z_child->SetValue(i, pt[2]);
    }

    if (count == 1) {
      result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
  }
};

struct S2CellVertex {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_cell_vertex", {Types::S2_CELL(), LogicalType::TINYINT},
                             Types::S2_POINT(), Execute);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    using s2geography::op::point::Point;
    s2geography::op::cell::CellVertex op;
    idx_t count = args.size();

    auto& cell_id = args.data[0];
    cell_id.Flatten(count);
    auto cell_ids = reinterpret_cast<uint64_t*>(cell_id.GetData());

    auto& vertex = args.data[1];
    vertex.Flatten(count);
    auto vertices = reinterpret_cast<int8_t*>(vertex.GetData());

    auto& children = StructVector::GetEntries(result);
    auto& x_child = children[0];
    auto& y_child = children[1];
    auto& z_child = children[2];

    for (idx_t i = 0; i < count; i++) {
      Point pt = op.ExecuteScalar(cell_ids[i], vertices[i]);
      x_child->SetValue(i, pt[0]);
      y_child->SetValue(i, pt[1]);
      z_child->SetValue(i, pt[2]);
    }

    if (count == 1) {
      result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
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
struct S2CellFromString {
  static void Register(DatabaseInstance& instance, const char* name) {
    auto fn = ScalarFunction(name, {LogicalType::VARCHAR}, Types::S2_CELL(), Execute);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    Op op;
    UnaryExecutor::Execute<string_t, int64_t>(
        args.data[0], result, args.size(), [&](string_t arg0) {
          return static_cast<int64_t>(op.ExecuteScalar({arg0.GetData(), arg0.GetSize()}));
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

template <typename Op>
struct S2BinaryCellPredicate {
  static void Register(DatabaseInstance& instance, const char* name) {
    auto fn = ScalarFunction(name, {Types::S2_CELL(), Types::S2_CELL()},
                             LogicalType::BOOLEAN, Execute);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    Op op;
    BinaryExecutor::Execute<int64_t, int64_t, bool>(
        args.data[0], args.data[1], result, args.size(),
        [&](int64_t arg0, int64_t arg1) { return op.ExecuteScalar(arg0, arg1); });
  }
};

template <typename Op>
struct S2CellToCell {
  static void Register(DatabaseInstance& instance, const char* name) {
    auto fn = ScalarFunction(name, {Types::S2_CELL(), LogicalType::TINYINT},
                             Types::S2_CELL(), Execute);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    Op op;
    BinaryExecutor::Execute<int64_t, int8_t, int64_t>(
        args.data[0], args.data[1], result, args.size(), [&](int64_t arg0, int8_t arg1) {
          return static_cast<int64_t>(op.ExecuteScalar(arg0, arg1));
        });
  }
};

}  // namespace

void RegisterS2CellOps(DatabaseInstance& instance) {
  using namespace s2geography::op::cell;

  // Some or all of these should probably be casts
  S2CellFromPoint::Register(instance);
  S2CellToString<ToDebugString>::Register(instance, "s2_cell_debug_string");
  S2CellToString<ToToken>::Register(instance, "s2_cell_token");
  S2CellFromString<FromDebugString>::Register(instance, "s2_cell_from_debug_string");
  S2CellFromString<FromToken>::Register(instance, "s2_cell_from_token");

  S2CellToDouble<Area>::Register(instance, "s2_cell_area");
  S2CellToDouble<AreaApprox>::Register(instance, "s2_cell_area_approx");
  S2CellToInt8<Level>::Register(instance, "s2_cell_level");

  S2CellCenter::Register(instance);
  S2CellVertex::Register(instance);

  S2BinaryCellPredicate<Contains>::Register(instance, "s2_cell_contains");
  S2BinaryCellPredicate<MayIntersect>::Register(instance, "s2_cell_may_intersect");
  S2CellToCell<Child>::Register(instance, "s2_cell_child");
  S2CellToCell<Parent>::Register(instance, "s2_cell_parent");
  S2CellToCell<EdgeNeighbor>::Register(instance, "s2_cell_edge_neighbor");
}

}  // namespace duckdb_s2
}  // namespace duckdb
