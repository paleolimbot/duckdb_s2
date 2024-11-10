
#include "s2_point_ops.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2geography/op/cell.h"
#include "s2geography/op/point.h"

#include "s2_geography_serde.hpp"
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

struct S2CellFromGeography {
  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, int64_t>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);

          // If we already have a snapped cell center encoding, the last 8 inlined
          // bytes are the little endian cell id as a uint64_t
          if (decoder.tag.kind == s2geography::GeographyKind::CELL_CENTER &&
              decoder.tag.covering_size == 1) {
            uint64_t cell_id = LittleEndian::Load64(geog_str.GetData() + 4);
            return static_cast<int64_t>(cell_id);
          }

          // Otherwise, we just need to load the geography
          std::unique_ptr<s2geography::Geography> geog = decoder.Decode(geog_str);

          // Use the Shape interface, which should work for PointGeography
          // and EncodedShapeIndex geography. A single shape with a single
          // edge always works here.
          if (geog->num_shapes() != 1) {
            return static_cast<int64_t>(S2CellId::Sentinel().id());
          }

          std::unique_ptr<S2Shape> shape = geog->Shape(0);
          if (shape->num_edges() != 1 || shape->dimension() != 0) {
            return static_cast<int64_t>(S2CellId::Sentinel().id());
          }

          S2CellId cell(shape->edge(0).v0);
          return static_cast<int64_t>(cell.id());
        });
  }
};

struct S2CellToGeography {
  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    // Most cells will get serialized as a tag + cell_id
    Encoder non_empty;
    s2geography::EncodeTag tag;
    tag.kind = s2geography::GeographyKind::CELL_CENTER;
    tag.covering_size = 1;
    tag.Encode(&non_empty);
    non_empty.Ensure(sizeof(uint64_t));
    non_empty.put64(0);
    string_t non_empty_str{non_empty.base(), static_cast<uint32_t>(non_empty.length())};
    char* non_empty_cell_id = non_empty_str.GetPrefixWriteable() + 4;

    // Invalid cells will get serialized as an empty point
    Encoder empty;
    tag.kind = s2geography::GeographyKind::POINT;
    tag.covering_size = 0;
    tag.flags |= s2geography::EncodeTag::kFlagEmpty;
    tag.Encode(&empty);
    string_t empty_str{empty.base(), static_cast<uint32_t>(empty.length())};

    UnaryExecutor::Execute<int64_t, string_t>(source, result, count, [&](int64_t arg0) {
      S2CellId cell(arg0);
      if (cell.is_valid()) {
        std::memcpy(non_empty_cell_id, &arg0, sizeof(arg0));
        return non_empty_str;
      } else {
        return empty_str;
      }
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
    auto fn = ScalarFunction(name, {Types::S2_CELL()}, LogicalType::VARCHAR, ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    return Execute(args.data[0], result, args.size());
  }

  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    Op op;
    UnaryExecutor::Execute<int64_t, string_t>(source, result, count, [&](int64_t arg0) {
      std::string_view str = op.ExecuteScalar(arg0);
      return StringVector::AddString(
          result, string_t{str.data(), static_cast<uint32_t>(str.size())});
    });
  }
};

template <typename Op>
struct S2CellFromString {
  static void Register(DatabaseInstance& instance, const char* name) {
    auto fn = ScalarFunction(name, {LogicalType::VARCHAR}, Types::S2_CELL(), ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    return Execute(args.data[0], result, args.size());
  }

  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    Op op;
    UnaryExecutor::Execute<string_t, int64_t>(source, result, count, [&](string_t arg0) {
      std::string_view item_view{arg0.GetData(), static_cast<size_t>(arg0.GetSize())};
      return static_cast<int64_t>(op.ExecuteScalar(item_view));
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

  // Explicit casts to/from string handle the debug string (better for printing)
  ExtensionUtil::RegisterCastFunction(
      instance, Types::S2_CELL(), LogicalType::VARCHAR,
      BoundCastInfo(S2CellToString<ToDebugString>::ExecuteCast), 1);
  ExtensionUtil::RegisterCastFunction(
      instance, LogicalType::VARCHAR, Types::S2_CELL(),
      BoundCastInfo(S2CellFromString<FromDebugString>::ExecuteCast), 1);

  // Explicit from geography, but implicit cast *to* geography
  ExtensionUtil::RegisterCastFunction(instance, Types::S2_CELL(), Types::GEOGRAPHY(),
                                      BoundCastInfo(S2CellToGeography::ExecuteCast), 1);
  ExtensionUtil::RegisterCastFunction(instance, Types::GEOGRAPHY(), Types::S2_CELL(),
                                      BoundCastInfo(S2CellFromGeography::ExecuteCast), 1);

  S2CellFromPoint::Register(instance);
  S2CellToString<ToToken>::Register(instance, "s2_cell_token");
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
