
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/s2cell.h"
#include "s2/s2cell_union.h"
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

          // Empties are always translated as invalid regardless of type
          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return static_cast<int64_t>(S2CellId::Sentinel().id());
          }

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
            throw InvalidInputException(
                std::string("Can't convert geography that is not empty nor a single "
                            "point to S2_CELL_CENTER"));
          }

          S2CellId cell(shape->edge(0).v0);
          return static_cast<int64_t>(cell.id());
        });
  }
};

struct S2CellUnionFromS2Cell {
  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    ListVector::Reserve(result, count);
    uint64_t offset = 0;

    UnaryExecutor::Execute<int64_t, list_entry_t>(
        source, result, count, [&](int64_t cell_id) {
          S2CellId cell(cell_id);
          if (!cell.is_valid()) {
            return list_entry_t{0, 0};
          } else {
            ListVector::PushBack(result, Value::UBIGINT(cell_id));
            return list_entry_t{offset++, 1};
          }
        });
  }
};

// Normalize storage on the cast in to the type
struct S2CellUnionFromStorage {
  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    ListVector::Reserve(result, count);
    vector<S2CellId> cell_ids;
    // Not sure if this is the appropriate way to handle list child data
    // in the presence of a possible dictionary vector as a source
    auto child_ids = reinterpret_cast<uint64_t*>(ListVector::GetEntry(source).GetData());

    uint64_t offset = 0;

    UnaryExecutor::Execute<list_entry_t, list_entry_t>(
        source, result, count, [&](list_entry_t item) {
          cell_ids.resize(static_cast<size_t>(item.length));
          for (uint64_t i = 0; i < item.length; i++) {
            cell_ids[i] = S2CellId(child_ids[item.offset + i]);
            if (!cell_ids[i].is_valid()) {
              throw InvalidInputException(
                  std::string("Cell not valid <" + cell_ids[i].ToString() + ">"));
            }
          }

          S2CellUnion::Normalize(&cell_ids);
          for (const auto cell_id : cell_ids) {
            ListVector::PushBack(result, Value::UBIGINT(cell_id.id()));
          }

          list_entry_t out{offset, cell_ids.size()};
          offset += out.length;
          return out;
        });
  }
};

struct S2CellUnionToGeography {
  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyEncoder encoder;
    vector<S2CellId> cell_ids;
    // Not sure if this is the appropriate way to handle list child data
    // in the presence of a possible dictionary vector as a source
    auto child_ids = reinterpret_cast<uint64_t*>(ListVector::GetEntry(source).GetData());

    UnaryExecutor::Execute<list_entry_t, string_t>(
        source, result, count, [&](list_entry_t item) {
          cell_ids.resize(static_cast<size_t>(item.length));
          for (uint64_t i = 0; i < item.length; i++) {
            cell_ids[i] = S2CellId(child_ids[item.offset + i]);
          }

          auto cells = S2CellUnion::FromNormalized(std::move(cell_ids));
          auto poly = make_uniq<S2Polygon>();
          poly->InitToCellUnionBorder(cells);
          s2geography::PolygonGeography geog(std::move(poly));
          cell_ids = cells.Release();

          // Would be nice if we could set the covering here since we already
          // know exactly what it is!
          return StringVector::AddStringOrBlob(result, encoder.Encode(geog));
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

struct S2CellCenterFromLonLat {
  static void Register(DatabaseInstance& instance) {
    auto fn =
        ScalarFunction("s2_cellfromlonlat", {LogicalType::DOUBLE, LogicalType::DOUBLE},
                       Types::S2_CELL_CENTER(), ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    return Execute(args.data[0], args.data[1], result, args.size());
  }

  static inline void Execute(Vector& src_lon, Vector& src_lat, Vector& result,
                             idx_t count) {
    BinaryExecutor::Execute<double, double, int64_t>(
        src_lon, src_lat, result, count, [&](double lon, double lat) {
          if (std::isnan(lon) && std::isnan(lat)) {
            return static_cast<int64_t>(S2CellCenterFromWKB::invalid_id);
          }

          auto latlng = S2LatLng::FromDegrees(lat, lon);
          S2CellId cell_id(latlng.ToPoint());
          return static_cast<int64_t>(cell_id.id());
        });
  }
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

struct S2CellToGeography {
  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyEncoder encoder;

    UnaryExecutor::Execute<int64_t, string_t>(source, result, count, [&](int64_t arg0) {
      S2CellId cell(arg0);
      if (!cell.is_valid()) {
        s2geography::PolygonGeography geog;
        return StringVector::AddStringOrBlob(result, encoder.Encode(geog));
      }

      auto loop = make_uniq<S2Loop>(S2Cell(cell));
      auto poly = make_uniq<S2Polygon>(std::move(loop));
      s2geography::PolygonGeography geog(std::move(poly));
      return StringVector::AddStringOrBlob(result, encoder.Encode(geog));
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
  result.Reinterpret(source);
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

  // s2_cell to geography can be implicit (never fails for valid input)
  ExtensionUtil::RegisterCastFunction(instance, Types::S2_CELL(), Types::GEOGRAPHY(),
                                      BoundCastInfo(S2CellToGeography::ExecuteCast), 0);

  // s2_cell_union to geography can be implicit
  ExtensionUtil::RegisterCastFunction(
      instance, Types::S2_CELL_UNION(), Types::GEOGRAPHY(),
      BoundCastInfo(S2CellUnionToGeography::ExecuteCast), 0);

  // s2_cell to s2_cell_union can be implicit
  ExtensionUtil::RegisterCastFunction(instance, Types::S2_CELL(), Types::S2_CELL_UNION(),
                                      BoundCastInfo(S2CellUnionFromS2Cell::ExecuteCast),
                                      0);

  // s2_cell union from storage is explicit
  ExtensionUtil::RegisterCastFunction(
      instance, LogicalType::LIST(Types::S2_CELL()), Types::S2_CELL_UNION(),
      BoundCastInfo(S2CellUnionFromStorage::ExecuteCast), 1);
  ExtensionUtil::RegisterCastFunction(
      instance, LogicalType::LIST(LogicalType::UBIGINT), Types::S2_CELL_UNION(),
      BoundCastInfo(S2CellUnionFromStorage::ExecuteCast), 1);
  ExtensionUtil::RegisterCastFunction(
      instance, LogicalType::LIST(LogicalType::BIGINT), Types::S2_CELL_UNION(),
      BoundCastInfo(S2CellUnionFromStorage::ExecuteCast), 1);

  // Explicit casts: s2_cell to/from s2_cell_center
  ExtensionUtil::RegisterCastFunction(instance, Types::S2_CELL_CENTER(), Types::S2_CELL(),
                                      BoundCastInfo(ExecuteNoopCast), 1);
  ExtensionUtil::RegisterCastFunction(instance, Types::S2_CELL(), Types::S2_CELL_CENTER(),
                                      BoundCastInfo(ExecuteNoopCast), 1);

  S2CellCenterFromWKB::Register(instance);
  S2CellCenterFromLonLat::Register(instance);
  S2CellToString<ToToken>::Register(instance, "s2_cell_token");
  S2CellFromString<FromToken>::Register(instance, "s2_cell_from_token");

  S2CellToInt8<Level>::Register(instance, "s2_cell_level", Types::S2_CELL());

  S2CellVertex::Register(instance);

  S2BinaryCellPredicate<Contains>::Register(instance, "s2_cell_contains");
  S2BinaryCellPredicate<MayIntersect>::Register(instance, "s2_cell_intersects");
  S2CellToCell<Child>::Register(instance, "s2_cell_child");
  S2CellToCell<Parent>::Register(instance, "s2_cell_parent");
  S2CellToCell<EdgeNeighbor>::Register(instance, "s2_cell_edge_neighbor");
}

}  // namespace duckdb_s2
}  // namespace duckdb
