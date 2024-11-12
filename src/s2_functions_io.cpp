
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/encoded_s2shape_index.h"
#include "s2/s2shape_index_region.h"
#include "s2/s2shapeutil_coding.h"
#include "s2geography/geography.h"

#include "s2_geography_serde.hpp"
#include "s2_types.hpp"
#include "s2geography/geoarrow.h"
#include "s2geography/wkt-reader.h"
#include "s2geography/wkt-writer.h"

namespace duckdb {

namespace duckdb_s2 {

struct S2GeogFromText {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_geogfromtext", {LogicalType::VARCHAR},
                             Types::GEOGRAPHY(), ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
    ExtensionUtil::RegisterCastFunction(instance, LogicalType::VARCHAR,
                                        Types::GEOGRAPHY(), BoundCastInfo(ExecuteCast),
                                        1);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyEncoder encoder;
    s2geography::WKTReader reader;

    UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t wkt) {
      auto geog = reader.read_feature(wkt.GetData(), wkt.GetSize());
      return StringVector::AddStringOrBlob(result, encoder.Encode(*geog));
    });
  }
};

struct S2AsText {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_astext", {Types::GEOGRAPHY()}, LogicalType::VARCHAR,
                             ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);

    auto fn_format =
        ScalarFunction("s2_format", {Types::GEOGRAPHY(), LogicalType::TINYINT},
                       LogicalType::VARCHAR, ExecuteFnPrec);
    ExtensionUtil::RegisterFunction(instance, fn_format);

    ExtensionUtil::RegisterCastFunction(instance, Types::GEOGRAPHY(),
                                        LogicalType::VARCHAR, BoundCastInfo(ExecuteCast),
                                        1);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static inline void ExecuteFnPrec(DataChunk& args, ExpressionState& state,
                                   Vector& result) {
    Vector& precision = args.data[1];
    if (precision.GetVectorType() != VectorType::CONSTANT_VECTOR) {
      throw InvalidInputException("Can't use s2_format() with non-constant precision");
    }

    Execute(args.data[0], result, args.size(), precision.GetValue(0).GetValue<int8_t>());
  }

  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count,
                             int8_t precision = -1) {
    GeographyDecoder decoder;
    s2geography::WKTWriter writer(precision);

    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);
          if (decoder.tag.kind == s2geography::GeographyKind::SHAPE_INDEX) {
            return StringVector::AddString(
                result, std::string("<S2ShapeIndex ") +
                            std::to_string(geog_str.GetSize()) + " b>");
          }
          auto geog = decoder.Decode(geog_str);
          std::string wkt = writer.write_feature(*geog);
          return StringVector::AddString(result, wkt);
        });
  }
};

struct S2GeogFromWKB {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_geogfromwkb", {LogicalType::BLOB}, Types::GEOGRAPHY(),
                             ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
    ExtensionUtil::RegisterCastFunction(instance, LogicalType::BLOB, Types::GEOGRAPHY(),
                                        BoundCastInfo(ExecuteCast), 1);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static inline bool ExecuteCast(Vector& source, Vector& result, idx_t count,
                                 CastParameters& parameters) {
    Execute(source, result, count);
    return true;
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyEncoder encoder;

    // Hack to get around lack of WKBReader at the moment
    s2geography::geoarrow::Reader reader;
    reader.Init(s2geography::geoarrow::Reader::InputType::kWKB,
                s2geography::geoarrow::ImportOptions());
    std::vector<std::unique_ptr<s2geography::Geography>> geogs;
    int32_t offsets[] = {0, 0};
    const void* buffers[] = {nullptr, offsets, nullptr};
    ArrowArray array{};
    array.length = 1;
    array.null_count = 0;
    array.offset = 0;

    array.n_buffers = 3;
    array.n_children = 0;
    array.buffers = buffers;

    array.children = nullptr;
    array.dictionary = nullptr;
    array.release = [](ArrowArray*) -> void {};
    array.private_data = nullptr;

    UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t wkb) {
      buffers[2] = wkb.GetData();
      offsets[1] = static_cast<int32_t>(wkb.GetSize());
      geogs.clear();
      reader.ReadGeography(&array, 0, 1, &geogs);
      return StringVector::AddStringOrBlob(result, encoder.Encode(*geogs[0]));
    });
  }
};

struct S2GeogPrepare {
  static void Register(DatabaseInstance& instance) {
    auto fn =
        ScalarFunction("s2_prepare", {Types::GEOGRAPHY()}, Types::GEOGRAPHY(), ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;
    GeographyEncoder encoder;

    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);

          // For small geographies or something that is already prepared, don't
          // trigger a new index. 64 bytes is arbitrary here (should be tuned).
          if (decoder.tag.kind == s2geography::GeographyKind::SHAPE_INDEX ||
              geog_str.GetSize() < 64) {
            // Maybe a way to avoid copying geog_str?
            return StringVector::AddStringOrBlob(result, geog_str);
          }

          std::unique_ptr<s2geography::Geography> geog = decoder.Decode(geog_str);
          s2geography::ShapeIndexGeography index_geog(*geog);
          return StringVector::AddStringOrBlob(result, encoder.Encode(index_geog));
        });
  }
};

void RegisterS2GeographyFunctionsIO(DatabaseInstance& instance) {
  S2GeogFromText::Register(instance);
  S2GeogFromWKB::Register(instance);
  S2AsText::Register(instance);
  S2GeogPrepare::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
