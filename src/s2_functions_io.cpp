
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/encoded_s2shape_index.h"
#include "s2/s2shape_index_region.h"
#include "s2/s2shapeutil_coding.h"
#include "s2geography/geography.h"

#include "s2_geography_serde.hpp"
#include "s2_types.hpp"
#include "s2geography/geoarrow.h"
#include "s2geography/wkb.h"
#include "s2geography/wkt-reader.h"
#include "s2geography/wkt-writer.h"

#include "function_builder.hpp"

namespace duckdb {

namespace duckdb_s2 {

struct S2GeogFromText {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_geogfromtext", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("wkt", LogicalType::VARCHAR);
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription("Returns the geography from a WKT string.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "conversion");
        });

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
    FunctionBuilder::RegisterScalar(
        instance, "s2_astext", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::VARCHAR);
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription("Returns the WKT string of the geography.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "conversion");
        });

    FunctionBuilder::RegisterScalar(
        instance, "s2_format", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.AddParameter("precision", LogicalType::TINYINT);
            variant.SetReturnType(LogicalType::VARCHAR);
            variant.SetFunction(ExecuteFnPrec);
          });

          func.SetDescription(
              "Returns the WKT string of the geography with a given precision.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "conversion");
        });

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
    FunctionBuilder::RegisterScalar(
        instance, "s2_geogfromwkb", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("wkb", LogicalType::BLOB);
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription("Converts a WKB blob to a geography.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "conversion");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    s2geography::WKBReader reader;
    GeographyEncoder encoder;

    UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t wkb) {
      std::unique_ptr<s2geography::Geography> geog =
          reader.ReadFeature(std::string_view(wkb.GetData(), wkb.GetSize()));
      return StringVector::AddStringOrBlob(result, encoder.Encode(*geog));
    });
  }
};

struct S2AsWKB {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_aswkb", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::BLOB);
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription("Returns the WKB blob of the geography.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "conversion");
        });
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static inline void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;
    s2geography::WKBWriter writer;

    UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t wkb) {
      std::unique_ptr<s2geography::Geography> geog = decoder.Decode(wkb);
      return StringVector::AddStringOrBlob(result, writer.WriteFeature(*geog));
    });
  }
};

struct S2GeogPrepare {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_prepare", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog", Types::GEOGRAPHY());
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(ExecuteFn);
          });

          func.SetDescription(
              "Prepares a geography for faster predicate and overlay operations.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "conversion");
        });
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
  S2AsWKB::Register(instance);
  S2GeogPrepare::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
