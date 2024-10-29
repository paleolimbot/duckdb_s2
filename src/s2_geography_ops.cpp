
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

class GeographyEncoder {
 public:
  string_t Encode(Vector& vector, const s2geography::ShapeIndexGeography& geog) {
    encoder_.reset();
    encoder_.Ensure(8);
    encoder_.putn("S2GEOG00", 8);
    s2shapeutil::CompactEncodeTaggedShapes(geog.ShapeIndex(), &encoder_);
    geog.ShapeIndex().Encode(&encoder_);
    return StringVector::AddStringOrBlob(vector, encoder_.base(),
                                         static_cast<uint32_t>(encoder_.length()));
  }

  string_t Encode(Vector& vector, const s2geography::Geography& geog) {
    s2geography::ShapeIndexGeography index(geog);
    return Encode(vector, index);
  }

 private:
  Encoder encoder_;
};

class EncodedShapeIndexGeography : public s2geography::Geography {
  class ShapeIndexGeography : public Geography {
   public:
    void Init(const char* buf, size_t buf_size) {
      if (buf_size < 8) {
        throw InvalidInputException("serialized geography must be at least 8 bytes");
      }
      Decoder decoder(buf, buf_size);
      error_.Clear();

      char magic[9];
      decoder.getn(magic, 8);
      magic[8] = '\0';
      if (std::string("S2GEOG00") != magic) {
        throw InvalidInputException("Invalid serialized geography prefix");
      }

      bool success = shape_index_.Init(
          &decoder, s2shapeutil::LazyDecodeShapeFactory(&decoder, error_));
      if (!success || !error_.ok()) {
        throw InvalidInputException("Decoding error: " + error_.text());
      }
    }

    int num_shapes() const { return shape_index_.num_shape_ids(); }
    std::unique_ptr<S2Shape> Shape(int id) const {
      s2shapeutil::WrappedShapeFactory factory(&shape_index_);
      return factory[id];
    }

    std::unique_ptr<S2Region> Region() const {
      return absl::make_unique<S2ShapeIndexRegion<EncodedS2ShapeIndex>>(&shape_index_);
    }

    const EncodedS2ShapeIndex& ShapeIndex() const { return shape_index_; }

   private:
    EncodedS2ShapeIndex shape_index_;
    S2Error error_;
  };
};

struct S2GeogFromWKT {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_geog_from_wkt", {LogicalType::VARCHAR},
                             Types::S2_GEOGRAPHY(), Execute);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void Execute(DataChunk& args, ExpressionState& state, Vector& result) {
    s2geography::WKTReader reader;
    GeographyEncoder encoder;

    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(), [&](string_t wkt) {
          auto geog = reader.read_feature(wkt.GetData(), wkt.GetSize());
          return encoder.Encode(result, *geog);
        });
  }
};

void RegisterS2GeographyOps(DatabaseInstance& instance) {
  S2GeogFromWKT::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
