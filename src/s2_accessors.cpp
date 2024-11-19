
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/s2earth.h"
#include "s2geography/accessors.h"

#include "s2/s2cell_union.h"
#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

struct S2IsEmpty {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_isempty", {Types::GEOGRAPHY()}, LogicalType::BOOLEAN,
                             ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, bool>(source, result, count, [&](string_t geog_str) {
      decoder.DecodeTag(geog_str);
      return decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty;
    });
  }
};

struct S2Area {
  static void Register(DatabaseInstance& instance) {
    auto fn =
        ScalarFunction("s2_area", {Types::GEOGRAPHY()}, LogicalType::DOUBLE, ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, double>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);

          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return 0.0;
          }

          switch (decoder.tag.kind) {
            case s2geography::GeographyKind::CELL_CENTER:
            case s2geography::GeographyKind::POINT:
            case s2geography::GeographyKind::POLYLINE:
              return 0.0;
            default: {
              auto geog = decoder.Decode(geog_str);
              return s2geography::s2_area(*geog) * S2Earth::RadiusMeters() *
                     S2Earth::RadiusMeters();
            }
          }
        });
  }
};

struct S2Perimieter {
  static void Register(DatabaseInstance& instance) {
    auto fn = ScalarFunction("s2_perimeter", {Types::GEOGRAPHY()}, LogicalType::DOUBLE,
                             ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, double>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);
          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return 0.0;
          }

          switch (decoder.tag.kind) {
            case s2geography::GeographyKind::CELL_CENTER:
            case s2geography::GeographyKind::POINT:
            case s2geography::GeographyKind::POLYLINE:
              return 0.0;
            default: {
              auto geog = decoder.Decode(geog_str);
              return s2geography::s2_perimeter(*geog) * S2Earth::RadiusMeters();
            }
          }
        });
  }
};

struct S2Length {
  static void Register(DatabaseInstance& instance) {
    auto fn =
        ScalarFunction("s2_length", {Types::GEOGRAPHY()}, LogicalType::DOUBLE, ExecuteFn);
    ExtensionUtil::RegisterFunction(instance, fn);
  }

  static inline void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(args.data[0], result, args.size());
  }

  static void Execute(Vector& source, Vector& result, idx_t count) {
    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, double>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);

          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return 0.0;
          }

          switch (decoder.tag.kind) {
            case s2geography::GeographyKind::CELL_CENTER:
            case s2geography::GeographyKind::POINT:
            case s2geography::GeographyKind::POLYGON:
              return 0.0;
            default: {
              auto geog = decoder.Decode(geog_str);
              return s2geography::s2_length(*geog) * S2Earth::RadiusMeters();
            }
          }
        });
  }
};

}  // namespace

void RegisterS2GeographyAccessors(DatabaseInstance& instance) {
  S2IsEmpty::Register(instance);
  S2Area::Register(instance);
  S2Perimieter::Register(instance);
  S2Length::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
