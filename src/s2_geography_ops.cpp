
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/encoded_s2shape_index.h"
#include "s2/s2shape_index_region.h"
#include "s2/s2shapeutil_coding.h"
#include "s2geography/geography.h"

#include "s2_types.hpp"
#include "s2geography/wkt-reader.h"

namespace duckdb {

namespace duckdb_s2 {

struct S2GeogFromWKT {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_geog_from_wkt", {LogicalType::VARCHAR},
                             Types::S2_GEOGRAPHY(), Execute);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    s2geography::WKTReader reader;

    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(), [&](string_t wkt) {
          auto geog = reader.read_feature(wkt.GetData(), wkt.GetSize());
          Encoder encoder{};
          geog->EncodeTagged(&encoder);
          return StringVector::AddStringOrBlob(
              result, string_t{encoder.base(), static_cast<uint32_t>(encoder.length())});
        });
  }
};

void RegisterS2GeographyOps(DatabaseInstance& instance) {
  S2GeogFromWKT::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
