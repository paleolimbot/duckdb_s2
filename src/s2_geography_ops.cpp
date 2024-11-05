
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/encoded_s2shape_index.h"
#include "s2/s2shape_index_region.h"
#include "s2/s2shapeutil_coding.h"
#include "s2geography/geography.h"

#include "s2_types.hpp"
#include "s2geography/wkt-reader.h"
#include "s2geography/wkt-writer.h"

namespace duckdb {

namespace duckdb_s2 {

struct S2GeogFromText {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_geogfromtext", {LogicalType::VARCHAR},
                             Types::S2_GEOGRAPHY(), Execute);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    s2geography::WKTReader reader;
    Encoder encoder{};

    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(), [&](string_t wkt) {
          auto geog = reader.read_feature(wkt.GetData(), wkt.GetSize());
          encoder.Resize(0);
          geog->EncodeTagged(&encoder);
          return StringVector::AddStringOrBlob(
              result, string_t{encoder.base(), static_cast<uint32_t>(encoder.length())});
        });
  }
};

struct S2AsText {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_astext", {Types::S2_GEOGRAPHY()}, LogicalType::VARCHAR,
                             Execute);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    s2geography::WKTWriter writer;

    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(), [&](string_t geog_str) {
          Decoder decoder{geog_str.GetData(), geog_str.GetSize()};
          std::unique_ptr<s2geography::Geography> geog =
              s2geography::Geography::DecodeTagged(&decoder);
          std::string wkt = writer.write_feature(*geog);
          return StringVector::AddString(result, wkt);
        });
  }
};

void RegisterS2GeographyOps(DatabaseInstance& instance) {
  S2GeogFromText::Register(instance);
  S2AsText::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
