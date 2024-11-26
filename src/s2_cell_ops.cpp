
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/s2cell.h"
#include "s2/s2cell_union.h"
#include "s2geography/op/cell.h"
#include "s2geography/op/point.h"

#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

#include "function_builder.hpp"

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
// with a single point). Also, includes an implementation of the s2ified
// GEOSHilbertCode_r() (which helpfully does not require a previously calculated extent).
struct S2CellCenterFromWKB {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_cellfromwkb", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("wkb", LogicalType::BLOB);
            variant.SetReturnType(Types::S2_CELL_CENTER());
            variant.SetFunction(ExecutePointFn);
          });

          func.SetDescription("Convert a WKB point directly to S2_CELL_CENTER");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });

    FunctionBuilder::RegisterScalar(
        instance, "s2_arbitrarycellfromwkb", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("wkb", LogicalType::BLOB);
            variant.SetReturnType(Types::S2_CELL_CENTER());
            variant.SetFunction(ExecuteArbitraryFn);
          });

          func.SetDescription("Convert the first vertex to S2_CELL_CENTER for sorting.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
  }

  static inline void ExecutePointFn(DataChunk& args, ExpressionState& state,
                                    Vector& result) {
    return ExecutePoint(args.data[0], result, args.size());
  }

  static inline void ExecuteArbitraryFn(DataChunk& args, ExpressionState& state,
                                        Vector& result) {
    return ExecuteArbitrary(args.data[0], result, args.size());
  }

  // Here the goal is to parse POINT (x x) or MULTIPOINT ((x x)) into a cell id
  // and error for anything else. EMPTY input goes to Sentinel().
  static inline void ExecutePoint(Vector& source, Vector& result, idx_t count) {
    UnaryExecutor::Execute<string_t, int64_t>(source, result, count, [&](string_t wkb) {
      Decoder decoder(wkb.GetData(), wkb.GetSize());
      S2CellId cell_id = S2CellId::Sentinel();
      VisitGeometry(
          &decoder,
          [&cell_id](uint32_t geometry_type, S2LatLng pt) {
            // If this point didn't come from a point, we need to error
            if ((geometry_type % 1000) != 1) {
              throw InvalidInputException(
                  "Can't parse WKB with non-point input to S2_CELL_CENTER");
            }

            // If we've already seen a point, we also need to error
            if (cell_id != S2CellId::Sentinel()) {
              throw InvalidInputException(
                  "Can't parse WKB with more than one point to S2_CELL_CENTER");
            }

            cell_id = S2CellId(pt.ToPoint());
            return true;
          },
          [](void) { throw InvalidInputException("Invalid WKB"); });

      return static_cast<int64_t>(cell_id.id());
    });
  }

  // Here the goal is just to get any arbitrary cell from the first lon/lat value we
  // find. This does have to assume that the WKB is lon/lat. Great for sorting!
  // Would be much improved if we could also reproject the first xy value we find
  // so that nobody has to parse WKB just to do a vague spatial sort.
  static inline void ExecuteArbitrary(Vector& source, Vector& result, idx_t count) {
    UnaryExecutor::Execute<string_t, int64_t>(source, result, count, [&](string_t wkb) {
      Decoder decoder(wkb.GetData(), wkb.GetSize());
      S2CellId cell_id = S2CellId::Sentinel();
      VisitGeometry(
          &decoder,
          [&cell_id](uint32_t geometry_type, S2LatLng pt) {
            // We don't care about geometry type here either, but we do want
            // to stop after the first point has been reached.
            cell_id = S2CellId(pt.ToPoint());
            return false;
          },
          // We don't care about invalid input here
          [](void) {});

      return static_cast<int64_t>(cell_id.id());
    });
  }

  template <typename LatLngCallback, typename ErrorCallback>
  static bool VisitGeometry(Decoder* decoder, LatLngCallback on_point,
                            ErrorCallback on_error) {
    if (decoder->avail() < sizeof(uint8_t)) {
      on_error();
      return false;
    }
    uint8_t le = decoder->get8();

    if (decoder->avail() < sizeof(uint32_t)) {
      on_error();
      return false;
    }

    uint32_t geometry_type;
    if (le) {
      geometry_type = LittleEndian::Load32(decoder->skip(sizeof(uint32_t)));
    } else {
      geometry_type = BigEndian::Load32(decoder->skip(sizeof(uint32_t)));
    }

    if (geometry_type & ewkb_srid_bit) {
      if (decoder->avail() < sizeof(uint32_t)) {
        on_error();
        return false;
      }

      decoder->skip(sizeof(uint32_t));
    }

    geometry_type &= ~(ewkb_srid_bit | ewkb_zm_bits);
    switch (geometry_type % 1000) {
      case 1:
        return VisitPoint(decoder, le, geometry_type, on_point, on_error);
      case 2:
        return VisitSequence(decoder, le, geometry_type, on_point, on_error);
      case 3:
        return VisitPolygon(decoder, le, geometry_type, on_point, on_error);
      case 4:
      case 5:
      case 6:
      case 7:
        return VisitCollection(decoder, le, on_point, on_error);
      default:
        on_error();
        return false;
    }
  }

  template <typename LatLngCallback, typename ErrorCallback>
  static bool VisitCollection(Decoder* decoder, bool le, LatLngCallback on_point,
                              ErrorCallback on_error) {
    if (decoder->avail() < sizeof(uint32_t)) {
      on_error();
      return false;
    }

    uint32_t n;
    if (le) {
      n = LittleEndian::Load32(decoder->skip(sizeof(uint32_t)));
    } else {
      n = BigEndian::Load32(decoder->skip(sizeof(uint32_t)));
    }

    for (uint32_t i = 0; i < n; i++) {
      bool keep_going = VisitGeometry(decoder, on_point, on_error);
      if (!keep_going) {
        return false;
      }
    }

    return true;
  }

  template <typename LatLngCallback, typename ErrorCallback>
  static bool VisitPolygon(Decoder* decoder, bool le, uint32_t geometry_type,
                           LatLngCallback on_point, ErrorCallback on_error) {
    if (decoder->avail() < sizeof(uint32_t)) {
      on_error();
      return false;
    }

    uint32_t n;
    if (le) {
      n = LittleEndian::Load32(decoder->skip(sizeof(uint32_t)));
    } else {
      n = BigEndian::Load32(decoder->skip(sizeof(uint32_t)));
    }

    for (uint32_t i = 0; i < n; i++) {
      bool keep_going = VisitSequence(decoder, le, geometry_type, on_point, on_error);
      if (!keep_going) {
        return false;
      }
    }

    return true;
  }

  template <typename LatLngCallback, typename ErrorCallback>
  static bool VisitSequence(Decoder* decoder, bool le, uint32_t geometry_type,
                            LatLngCallback on_point, ErrorCallback on_error) {
    if (decoder->avail() < sizeof(uint32_t)) {
      on_error();
      return false;
    }

    uint32_t n;
    if (le) {
      n = LittleEndian::Load32(decoder->skip(sizeof(uint32_t)));
    } else {
      n = BigEndian::Load32(decoder->skip(sizeof(uint32_t)));
    }

    for (uint32_t i = 0; i < n; i++) {
      bool keep_going = VisitPoint(decoder, le, geometry_type, on_point, on_error);
      if (!keep_going) {
        return false;
      }
    }

    return true;
  }

  template <typename LatLngCallback, typename ErrorCallback>
  static bool VisitPoint(Decoder* decoder, bool le, uint32_t geometry_type,
                         LatLngCallback on_point, ErrorCallback on_error) {
    if (decoder->avail() < (2 * sizeof(double))) {
      on_error();
      return false;
    }

    double lnglat[2];
    if (le) {
      lnglat[0] = LittleEndian::Load<double>(decoder->skip(sizeof(double)));
      lnglat[1] = LittleEndian::Load<double>(decoder->skip(sizeof(double)));
    } else {
      lnglat[0] = BigEndian::Load<double>(decoder->skip(sizeof(double)));
      lnglat[1] = BigEndian::Load<double>(decoder->skip(sizeof(double)));
    }

    if (std::isnan(lnglat[0]) || std::isnan(lnglat[1])) {
      return true;
    }

    auto latlng = S2LatLng::FromDegrees(lnglat[1], lnglat[0]);
    return on_point(geometry_type, latlng);
  }

  static constexpr uint32_t ewkb_srid_bit = 0x20000000;
  static constexpr uint32_t ewkb_zm_bits = 0x40000000 | 0x80000000;
};

struct S2CellCenterFromLonLat {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_cellfromlonlat", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("lon", LogicalType::DOUBLE);
            variant.AddParameter("lat", LogicalType::DOUBLE);
            variant.SetReturnType(Types::S2_CELL_CENTER());
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription("Convert a lon/lat pair to S2_CELL_CENTER");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    return Execute(args.data[0], args.data[1], result, args.size());
  }

  static inline void Execute(Vector& src_lon, Vector& src_lat, Vector& result,
                             idx_t count) {
    BinaryExecutor::Execute<double, double, int64_t>(
        src_lon, src_lat, result, count, [&](double lon, double lat) {
          if (std::isnan(lon) && std::isnan(lat)) {
            return static_cast<int64_t>(S2CellId::Sentinel().id());
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
    FunctionBuilder::RegisterScalar(
        instance, "s2_cell_vertex", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("cell_id", Types::S2_CELL());
            variant.AddParameter("vertex_id", LogicalType::TINYINT);
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(Execute);
          });

          func.SetDescription("Returns the vertex of the S2 cell.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "cellops");
        });
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
    FunctionBuilder::RegisterScalar(instance, name, [](ScalarFunctionBuilder& func) {
      func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("cell", Types::S2_CELL());
        variant.SetReturnType(LogicalType::VARCHAR);
        variant.SetFunction(ExecuteFn);
      });

      // TODO: Description
      // TODO: Example

      func.SetTag("ext", "geography");
      func.SetTag("category", "cellops");
    });
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
    FunctionBuilder::RegisterScalar(instance, name, [](ScalarFunctionBuilder& func) {
      func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("text", LogicalType::VARCHAR);
        variant.SetReturnType(Types::S2_CELL());
        variant.SetFunction(ExecuteFn);
      });

      // TODO: Description
      // TODO: Example

      func.SetTag("ext", "geography");
      func.SetTag("category", "cellops");
    });
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
    FunctionBuilder::RegisterScalar(instance, name, [](ScalarFunctionBuilder& func) {
      func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("cell", Types::S2_CELL());
        variant.SetReturnType(LogicalType::DOUBLE);
        variant.SetFunction(Execute);
      });

      // TODO: Description
      // TODO: Example

      func.SetTag("ext", "geography");
      func.SetTag("category", "cellops");
    });
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
    FunctionBuilder::RegisterScalar(instance, name, [&](ScalarFunctionBuilder& func) {
      func.AddVariant([&](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("cell", arg_type);
        variant.SetReturnType(LogicalType::TINYINT);
        variant.SetFunction(Execute);
      });

      // TODO: Description
      // TODO: Example

      func.SetTag("ext", "geography");
      func.SetTag("category", "cellops");
    });
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
    FunctionBuilder::RegisterScalar(instance, name, [&](ScalarFunctionBuilder& func) {
      func.AddVariant([&](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("cell1", Types::S2_CELL());
        variant.AddParameter("cell2", Types::S2_CELL());
        variant.SetReturnType(LogicalType::BOOLEAN);
        variant.SetFunction(Execute);
      });
      // TODO: Description
      // TODO: Example

      func.SetTag("ext", "geography");
      func.SetTag("category", "cellops");
    });
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
    FunctionBuilder::RegisterScalar(instance, name, [&](ScalarFunctionBuilder& func) {
      func.AddVariant([&](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("cell", Types::S2_CELL());
        variant.AddParameter("index", LogicalType::TINYINT);
        variant.SetReturnType(Types::S2_CELL());
        variant.SetFunction(Execute);
      });

      // TODO: Description
      // TODO: Example

      func.SetTag("ext", "geography");
      func.SetTag("category", "cellops");
    });
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
