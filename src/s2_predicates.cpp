
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/s2cell_union.h"
#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

struct S2Predicates {
  static void Register(DatabaseInstance& instance) {
    auto mayintersect =
        ScalarFunction("s2_mayintersect", {Types::GEOGRAPHY(), Types::GEOGRAPHY()},
                       LogicalType::BOOLEAN, ExecuteMayIntersectFn);
    ExtensionUtil::RegisterFunction(instance, mayintersect);

    auto intersects =
        ScalarFunction("s2_intersects", {Types::GEOGRAPHY(), Types::GEOGRAPHY()},
                       LogicalType::BOOLEAN, ExecuteIntersectsFn);
    ExtensionUtil::RegisterFunction(instance, intersects);

    auto contains =
        ScalarFunction("s2_contains", {Types::GEOGRAPHY(), Types::GEOGRAPHY()},
                       LogicalType::BOOLEAN, ExecuteMayIntersectFn);
    ExtensionUtil::RegisterFunction(instance, contains);

    auto equals = ScalarFunction("s2_equals", {Types::GEOGRAPHY(), Types::GEOGRAPHY()},
                                 LogicalType::BOOLEAN, ExecuteEqualsFn);
    ExtensionUtil::RegisterFunction(instance, equals);
  }

  using UniqueGeography = std::unique_ptr<s2geography::Geography>;

  static void ExecuteMayIntersectFn(DataChunk& args, ExpressionState& state,
                                    Vector& result) {
    return ExecuteFn(args, state, result,
                     [](UniqueGeography lhs, UniqueGeography rhs) { return true; });
  }

  static void ExecuteIntersectsFn(DataChunk& args, ExpressionState& state,
                                  Vector& result) {
    S2BooleanOperation::Options options;
    options.set_polygon_model(S2BooleanOperation::PolygonModel::CLOSED);
    options.set_polyline_model(S2BooleanOperation::PolylineModel::CLOSED);

    return ExecuteFn(
        args, state, result, [&options](UniqueGeography lhs, UniqueGeography rhs) {
          return DispatchShapeIndexFilter(std::move(lhs), std::move(rhs), options,
                                          S2BooleanOperation::Intersects);
        });
  }

  static void ExecuteContainsFn(DataChunk& args, ExpressionState& state, Vector& result) {
    // Note: Polygon containment when there is a partial shared edge might
    // need to be calculated differently.
    S2BooleanOperation::Options options;
    options.set_polygon_model(S2BooleanOperation::PolygonModel::CLOSED);
    options.set_polyline_model(S2BooleanOperation::PolylineModel::CLOSED);

    return ExecuteFn(
        args, state, result, [&options](UniqueGeography lhs, UniqueGeography rhs) {
          return DispatchShapeIndexFilter(std::move(rhs), std::move(rhs), options,
                                          S2BooleanOperation::Contains);
        });
  }

  static void ExecuteEqualsFn(DataChunk& args, ExpressionState& state, Vector& result) {
    S2BooleanOperation::Options options;
    return ExecuteFn(
        args, state, result, [&options](UniqueGeography lhs, UniqueGeography rhs) {
          return DispatchShapeIndexFilter(std::move(rhs), std::move(rhs), options,
                                          S2BooleanOperation::Equals);
        });
  }

  template <typename Filter>
  static void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result,
                        Filter&& filter) {
    Execute(args.data[0], args.data[1], result, args.size(), filter);
  }

  template <typename Filter>
  static void Execute(Vector& lhs, Vector& rhs, Vector& result, idx_t count,
                      Filter&& filter) {
    GeographyDecoder lhs_decoder;
    GeographyDecoder rhs_decoder;
    std::vector<S2CellId> intersection;

    BinaryExecutor::Execute<string_t, string_t, bool>(
        lhs, rhs, result, count, [&](string_t lhs_str, string_t rhs_str) {
          lhs_decoder.DecodeTagAndCovering(lhs_str);
          if (lhs_decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return false;
          }

          rhs_decoder.DecodeTagAndCovering(rhs_str);
          if (rhs_decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return false;
          }

          if (!CoveringMayIntersect(lhs_decoder, rhs_decoder, &intersection)) {
            return false;
          }

          return filter(lhs_decoder.Decode(lhs_str), rhs_decoder.Decode(rhs_str));
        });
  }

  static bool CoveringMayIntersect(const GeographyDecoder& lhs,
                                   const GeographyDecoder& rhs,
                                   std::vector<S2CellId>* intersection_scratch) {
    // We don't currently omit coverings but in case we do by accident,
    // an omitted covering *might* intersect since it was just not generated.
    if (lhs.covering.empty() || rhs.covering.empty()) {
      return true;
    }

    S2CellUnion::GetIntersection(lhs.covering, rhs.covering, intersection_scratch);
    return !intersection_scratch->empty();
  }

  template <typename ShapeIndexFilter>
  static bool DispatchShapeIndexFilter(UniqueGeography lhs, UniqueGeography rhs,
                                       const S2BooleanOperation::Options options,
                                       ShapeIndexFilter&& filter) {
    if (lhs->kind() == s2geography::GeographyKind::ENCODED_SHAPE_INDEX &&
        rhs->kind() == s2geography::GeographyKind::ENCODED_SHAPE_INDEX) {
      auto lhs_index =
          reinterpret_cast<s2geography::EncodedShapeIndexGeography*>(lhs.get());
      auto rhs_index =
          reinterpret_cast<s2geography::EncodedShapeIndexGeography*>(rhs.get());
      return filter(lhs_index->ShapeIndex(), rhs_index->ShapeIndex(), options);
    } else if (lhs->kind() == s2geography::GeographyKind::ENCODED_SHAPE_INDEX) {
      auto lhs_index =
          reinterpret_cast<s2geography::EncodedShapeIndexGeography*>(lhs.get());
      s2geography::ShapeIndexGeography rhs_index(*rhs);
      return filter(lhs_index->ShapeIndex(), rhs_index.ShapeIndex(), options);
    } else if (rhs->kind() == s2geography::GeographyKind::ENCODED_SHAPE_INDEX) {
      s2geography::ShapeIndexGeography lhs_index(*lhs);
      auto rhs_index =
          reinterpret_cast<s2geography::EncodedShapeIndexGeography*>(rhs.get());
      return filter(lhs_index.ShapeIndex(), rhs_index->ShapeIndex(), options);
    } else {
      s2geography::ShapeIndexGeography lhs_index(*lhs);
      s2geography::ShapeIndexGeography rhs_index(*rhs);
      return filter(lhs_index.ShapeIndex(), rhs_index.ShapeIndex(), options);
    }
  }
};

void RegisterS2GeographyPredicates(DatabaseInstance& instance) {
  S2Predicates::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
