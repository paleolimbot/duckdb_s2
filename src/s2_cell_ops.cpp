
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2geography/op/cell.h"
#include "s2geography/op/point.h"

#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

struct S2CellCenterFromGeography {
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

// Experimental version of a WKB parser that only handles points (or multipoints
// with a single point). If the s2geography WKB parser were faster this probably
// wouldn't be needed.
struct S2CellCenterFromWKB {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_cellfromwkb", {LogicalType::BLOB},
                             Types::S2_CELL_CENTER(), ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    return Execute(args.data[0], result, args.size());
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    uint32_t geometry_type;
    double lnglat[2];

    UnaryExecutor::Execute<string_t, int64_t>(source, result, count, [&](string_t wkb) {
      if (wkb.GetSize() < min_valid_size) {
        return invalid_id;
      }

      Decoder decoder(wkb.GetData(), wkb.GetSize());
      if (decoder.avail() < min_valid_size) {
        return invalid_id;
      }

      uint8_t little_endian = decoder.get8();
      do {
        if (decoder.avail() < sizeof(uint32_t)) {
          return invalid_id;
        }

        if (little_endian) {
          geometry_type = LittleEndian::Load32(decoder.skip(sizeof(uint32_t)));
        } else {
          geometry_type = BigEndian::Load32(decoder.skip(sizeof(uint32_t)));
        }

        if (geometry_type & ewkb_srid_bit) {
          if (decoder.avail() < sizeof(uint32_t)) {
            return invalid_id;
          }

          decoder.skip(sizeof(uint32_t));
        }

        geometry_type &= ~(ewkb_srid_bit | ewkb_zm_bits);
        switch (geometry_type % 1000) {
          case 1: {
            if (decoder.avail() < (2 * sizeof(double))) {
              return invalid_id;
            }

            if (little_endian) {
              lnglat[0] = LittleEndian::Load<double>(decoder.skip(sizeof(double)));
              lnglat[1] = LittleEndian::Load<double>(decoder.skip(sizeof(double)));
            } else {
              lnglat[0] = BigEndian::Load<double>(decoder.skip(sizeof(double)));
              lnglat[1] = BigEndian::Load<double>(decoder.skip(sizeof(double)));
            }

            S2Point pt =
                S2LatLng::FromDegrees(lnglat[1], lnglat[0]).Normalized().ToPoint();
            return static_cast<int64_t>(S2CellId(pt).id());
          }

          case 4: {
            // MULTIPOINT type
            uint32_t num_points;
            if (decoder.avail() < (sizeof(uint32_t) + sizeof(uint8_t))) {
              return invalid_id;
            }

            if (little_endian) {
              num_points = LittleEndian::Load32(decoder.skip(sizeof(uint32_t)));
            } else {
              num_points = BigEndian::Load32(decoder.skip(sizeof(uint32_t)));
            }

            if (num_points != 1) {
              return invalid_id;
            }

            little_endian = decoder.get8();
            break;
          }

          default:
            return invalid_id;
        }
      } while (decoder.avail() > 0);

      return invalid_id;
    });
  }

  static constexpr uint32_t ewkb_srid_bit = 0x20000000;
  static constexpr uint32_t ewkb_zm_bits = 0x40000000 | 0x80000000;
  static constexpr uint32_t min_valid_size = sizeof(uint8_t) + sizeof(uint32_t);
  static constexpr int64_t invalid_id =
      static_cast<int64_t>(s2geography::op::cell::kCellIdSentinel);
};

struct S2CellCenterToGeography {
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

struct S2CellVertex {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_cell_vertex", {Types::S2_CELL(), LogicalType::TINYINT},
                             Types::GEOGRAPHY(), Execute);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    using s2geography::op::point::Point;
    s2geography::op::cell::CellVertex op;
    GeographyEncoder encoder;

    BinaryExecutor::Execute<int64_t, uint8_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](int64_t cell_id, int8_t vertex_id) {
          Point pt = op.ExecuteScalar(cell_id, vertex_id);
          s2geography::PointGeography geog({pt[0], pt[1], pt[2]});
          return StringVector::AddStringOrBlob(result, encoder.Encode(geog));
        });
  }
};

template <typename Op>
struct S2CellToString {
  static void Register(DatabaseInstance& instance, const char* name) {
    auto fn = ScalarFunction(name, {Types::S2_CELL()}, LogicalType::VARCHAR, ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);

    // Both of these functions apply to cells and cell centers
    auto fn_center =
        ScalarFunction(name, {Types::S2_CELL_CENTER()}, LogicalType::VARCHAR, ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn_center);
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
  static void Register(DatabaseInstance& instance, const char* name,
                       LogicalType arg_type) {
    auto fn = ScalarFunction(name, {arg_type}, LogicalType::TINYINT, Execute);
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

bool ExecuteNoopCast(Vector& source, Vector& result, idx_t count,
                     CastParameters& parameters) {
  result.Reference(source);
  return true;
}

}  // namespace

void RegisterS2CellOps(DatabaseInstance& instance) {
  using namespace s2geography::op::cell;

  // Explicit casts to/from string handle the debug string (better for printing)
  // We use the same character representation for both cells and centers.
  ExtensionUtil::RegisterCastFunction(
      instance, Types::S2_CELL(), LogicalType::VARCHAR,
      BoundCastInfo(S2CellToString<ToDebugString>::ExecuteCast), 1);
  ExtensionUtil::RegisterCastFunction(
      instance, LogicalType::VARCHAR, Types::S2_CELL(),
      BoundCastInfo(S2CellFromString<FromDebugString>::ExecuteCast), 1);
  ExtensionUtil::RegisterCastFunction(
      instance, Types::S2_CELL_CENTER(), LogicalType::VARCHAR,
      BoundCastInfo(S2CellToString<ToDebugString>::ExecuteCast), 1);
  ExtensionUtil::RegisterCastFunction(
      instance, LogicalType::VARCHAR, Types::S2_CELL_CENTER(),
      BoundCastInfo(S2CellFromString<FromDebugString>::ExecuteCast), 1);

  // s2_cell_center to geography can be implicit (never fails for valid input)
  ExtensionUtil::RegisterCastFunction(
      instance, Types::S2_CELL_CENTER(), Types::GEOGRAPHY(),
      BoundCastInfo(S2CellCenterToGeography::ExecuteCast), 0);

  // geography to s2_cell_center must be explicit (can move a point up to 1 cm,
  // fails for input that is not a single point)
  ExtensionUtil::RegisterCastFunction(
      instance, Types::GEOGRAPHY(), Types::S2_CELL_CENTER(),
      BoundCastInfo(S2CellCenterFromGeography::ExecuteCast), 1);

  // Explicit casts: s2_cell to/from s2_cell_center
  ExtensionUtil::RegisterCastFunction(instance, Types::S2_CELL_CENTER(), Types::S2_CELL(),
                                      BoundCastInfo(ExecuteNoopCast), 1);
  ExtensionUtil::RegisterCastFunction(instance, Types::S2_CELL(), Types::S2_CELL_CENTER(),
                                      BoundCastInfo(ExecuteNoopCast), 1);

  S2CellCenterFromWKB::Register(instance);
  S2CellToString<ToToken>::Register(instance, "s2_cell_token");
  S2CellFromString<FromToken>::Register(instance, "s2_cell_from_token");

  S2CellToDouble<Area>::Register(instance, "s2_cell_area");
  S2CellToDouble<AreaApprox>::Register(instance, "s2_cell_area_approx");
  S2CellToInt8<Level>::Register(instance, "s2_cell_level", Types::S2_CELL());
  S2CellToInt8<Level>::Register(instance, "s2_cell_level", Types::S2_CELL_CENTER());

  S2CellVertex::Register(instance);

  S2BinaryCellPredicate<Contains>::Register(instance, "s2_cell_contains");
  S2BinaryCellPredicate<MayIntersect>::Register(instance, "s2_cell_may_intersect");
  S2CellToCell<Child>::Register(instance, "s2_cell_child");
  S2CellToCell<Parent>::Register(instance, "s2_cell_parent");
  S2CellToCell<EdgeNeighbor>::Register(instance, "s2_cell_edge_neighbor");
}

}  // namespace duckdb_s2
}  // namespace duckdb
