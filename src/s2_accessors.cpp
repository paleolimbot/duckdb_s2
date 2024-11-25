
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

struct S2XY {
  static void Register(DatabaseInstance& instance) {
    auto fn_x =
        ScalarFunction("s2_x", {Types::GEOGRAPHY()}, LogicalType::DOUBLE, ExecuteFnX);
    ExtensionUtil::RegisterFunction(instance, fn_x);

    auto fn_y =
        ScalarFunction("s2_y", {Types::GEOGRAPHY()}, LogicalType::DOUBLE, ExecuteFnY);
    ExtensionUtil::RegisterFunction(instance, fn_y);
  }

  static inline void ExecuteFnX(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(
        args.data[0], result, args.size(), [](S2LatLng ll) { return ll.lng().degrees(); },
        [](const s2geography::Geography& geog) { return s2_x(geog); });
  }

  static inline void ExecuteFnY(DataChunk& args, ExpressionState& state, Vector& result) {
    Execute(
        args.data[0], result, args.size(), [](S2LatLng ll) { return ll.lat().degrees(); },
        [](const s2geography::Geography& geog) { return s2_y(geog); });
  }

  template <typename HandleLatLng, typename HandleGeog>
  static void Execute(Vector& source, Vector& result, idx_t count,
                      HandleLatLng&& handle_latlng, HandleGeog&& handle_geog) {
    GeographyDecoder decoder;

    UnaryExecutor::Execute<string_t, double>(
        source, result, count, [&](string_t geog_str) {
          decoder.DecodeTag(geog_str);

          if (decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return static_cast<double>(NAN);
          }

          switch (decoder.tag.kind) {
            case s2geography::GeographyKind::CELL_CENTER: {
              decoder.DecodeTagAndCovering(geog_str);
              S2Point center = decoder.covering[0].ToPoint();
              return handle_latlng(S2LatLng(center));
            }

            default: {
              auto geog = decoder.Decode(geog_str);
              return handle_geog(*geog);
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
  S2XY::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
