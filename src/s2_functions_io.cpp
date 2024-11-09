
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
    ExtensionUtil::RegisterCastFunction(instance, Types::GEOGRAPHY(),
                                        LogicalType::VARCHAR, BoundCastInfo(ExecuteCast),
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
    GeographyDecoder decoder;
    s2geography::WKTWriter writer;

    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count, [&](string_t geog_str) {
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

void RegisterS2GeographyOps(DatabaseInstance& instance) {
  S2GeogFromText::Register(instance);
  S2GeogFromWKB::Register(instance);
  S2AsText::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
