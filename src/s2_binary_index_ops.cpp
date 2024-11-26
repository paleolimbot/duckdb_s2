
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2/s2cell_union.h"
#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

#include "s2geography/build.h"

#include "function_builder.hpp"
#include "global_options.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

struct S2BinaryIndexOp {
  static void Register(DatabaseInstance& instance) {
    FunctionBuilder::RegisterScalar(
        instance, "s2_mayintersect", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::BOOLEAN);
            variant.SetFunction(ExecuteMayIntersectFn);
          });

          func.SetDescription("Returns true if the two geographies may intersect.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "predicate");
        });

    FunctionBuilder::RegisterScalar(
        instance, "s2_intersects", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::BOOLEAN);
            variant.SetFunction(ExecuteIntersectsFn);
          });

          func.SetDescription("Returns true if the two geographies intersect.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "predicate");
        });

    FunctionBuilder::RegisterScalar(
        instance, "s2_contains", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::BOOLEAN);
            variant.SetFunction(ExecuteContainsFn);
          });

          func.SetDescription("Returns true if the first geography contains the second.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "predicate");
        });

    FunctionBuilder::RegisterScalar(
        instance, "s2_equals", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(LogicalType::BOOLEAN);
            variant.SetFunction(ExecuteEqualsFn);
          });

          func.SetDescription("Returns true if the two geographies are equal.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "predicate");
        });

    FunctionBuilder::RegisterScalar(
        instance, "s2_intersection", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(ExecuteIntersectionFn);
          });

          func.SetDescription("Returns the intersection of two geographies.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "overlay");
        });

    FunctionBuilder::RegisterScalar(
        instance, "s2_difference", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(ExecuteDifferenceFn);
          });

          func.SetDescription("Returns the difference of two geographies.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "overlay");
        });

    FunctionBuilder::RegisterScalar(
        instance, "s2_union", [](ScalarFunctionBuilder& func) {
          func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
            variant.AddParameter("geog1", Types::GEOGRAPHY());
            variant.AddParameter("geog2", Types::GEOGRAPHY());
            variant.SetReturnType(Types::GEOGRAPHY());
            variant.SetFunction(ExecuteUnionFn);
          });

          func.SetDescription("Returns the union of two geographies.");
          // TODO: Example

          func.SetTag("ext", "geography");
          func.SetTag("category", "overlay");
        });
  }

  using UniqueGeography = std::unique_ptr<s2geography::Geography>;

  static void ExecuteMayIntersectFn(DataChunk& args, ExpressionState& state,
                                    Vector& result) {
    return ExecutePredicateFn(
        args, state, result,
        [](UniqueGeography lhs, UniqueGeography rhs) { return true; });
  }

  // Handle the case where we've already computed the index on one or both
  // of the sides in advance
  template <typename ShapeIndexFilter>
  static auto DispatchShapeIndexFilter(UniqueGeography lhs, UniqueGeography rhs,
                                       ShapeIndexFilter&& filter) {
    if (lhs->kind() == s2geography::GeographyKind::ENCODED_SHAPE_INDEX &&
        rhs->kind() == s2geography::GeographyKind::ENCODED_SHAPE_INDEX) {
      auto lhs_index =
          reinterpret_cast<s2geography::EncodedShapeIndexGeography*>(lhs.get());
      auto rhs_index =
          reinterpret_cast<s2geography::EncodedShapeIndexGeography*>(rhs.get());
      return filter(lhs_index->ShapeIndex(), rhs_index->ShapeIndex());
    } else if (lhs->kind() == s2geography::GeographyKind::ENCODED_SHAPE_INDEX) {
      auto lhs_index =
          reinterpret_cast<s2geography::EncodedShapeIndexGeography*>(lhs.get());
      s2geography::ShapeIndexGeography rhs_index(*rhs);
      return filter(lhs_index->ShapeIndex(), rhs_index.ShapeIndex());
    } else if (rhs->kind() == s2geography::GeographyKind::ENCODED_SHAPE_INDEX) {
      s2geography::ShapeIndexGeography lhs_index(*lhs);
      auto rhs_index =
          reinterpret_cast<s2geography::EncodedShapeIndexGeography*>(rhs.get());
      return filter(lhs_index.ShapeIndex(), rhs_index->ShapeIndex());
    } else {
      s2geography::ShapeIndexGeography lhs_index(*lhs);
      s2geography::ShapeIndexGeography rhs_index(*rhs);
      return filter(lhs_index.ShapeIndex(), rhs_index.ShapeIndex());
    }
  }

  static void ExecuteIntersectsFn(DataChunk& args, ExpressionState& state,
                                  Vector& result) {
    S2BooleanOperation::Options options;
    InitBooleanOperationOptions(&options);

    return ExecutePredicateFn(
        args, state, result, [&options](UniqueGeography lhs, UniqueGeography rhs) {
          return DispatchShapeIndexFilter(
              std::move(lhs), std::move(rhs),
              [&options](const S2ShapeIndex& lhs_index, const S2ShapeIndex& rhs_index) {
                return S2BooleanOperation::Intersects(lhs_index, rhs_index, options);
              });
        });
  }

  static void ExecuteContainsFn(DataChunk& args, ExpressionState& state, Vector& result) {
    // Note: Polygon containment when there is a partial shared edge might
    // need to be calculated differently.
    S2BooleanOperation::Options options;
    InitBooleanOperationOptions(&options);

    return ExecutePredicateFn(
        args, state, result, [&options](UniqueGeography lhs, UniqueGeography rhs) {
          return DispatchShapeIndexFilter(
              std::move(lhs), std::move(rhs),
              [&options](const S2ShapeIndex& lhs_index, const S2ShapeIndex& rhs_index) {
                return S2BooleanOperation::Contains(lhs_index, rhs_index, options);
              });
        });
  }

  static void ExecuteEqualsFn(DataChunk& args, ExpressionState& state, Vector& result) {
    S2BooleanOperation::Options options;
    InitBooleanOperationOptions(&options);

    return ExecutePredicateFn(
        args, state, result, [&options](UniqueGeography lhs, UniqueGeography rhs) {
          return DispatchShapeIndexFilter(
              std::move(lhs), std::move(rhs),
              [&options](const S2ShapeIndex& lhs_index, const S2ShapeIndex& rhs_index) {
                return S2BooleanOperation::Equals(lhs_index, rhs_index, options);
              });
        });
  }

  template <typename Filter>
  static void ExecutePredicateFn(DataChunk& args, ExpressionState& state, Vector& result,
                                 Filter&& filter) {
    ExecutePredicate(args.data[0], args.data[1], result, args.size(), filter);
  }

  template <typename Filter>
  static void ExecutePredicate(Vector& lhs, Vector& rhs, Vector& result, idx_t count,
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

  static void ExecuteIntersectionFn(DataChunk& args, ExpressionState& state,
                                    Vector& result) {
    ExecuteIntersection(args.data[0], args.data[1], result, args.size());
  }

  static void ExecuteDifferenceFn(DataChunk& args, ExpressionState& state,
                                  Vector& result) {
    ExecuteDifference(args.data[0], args.data[1], result, args.size());
  }

  static void ExecuteUnionFn(DataChunk& args, ExpressionState& state, Vector& result) {
    ExecuteUnion(args.data[0], args.data[1], result, args.size());
  }

  static void ExecuteIntersection(Vector& lhs, Vector& rhs, Vector& result, idx_t count) {
    GeographyDecoder lhs_decoder;
    GeographyDecoder rhs_decoder;
    GeographyEncoder encoder;
    std::vector<S2CellId> intersection;

    s2geography::GlobalOptions options;
    InitGlobalOptions(&options);

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        lhs, rhs, result, count, [&](string_t lhs_str, string_t rhs_str) {
          lhs_decoder.DecodeTagAndCovering(lhs_str);

          // If the lefthand side is empty, the intersection is the righthand side
          if (lhs_decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return StringVector::AddStringOrBlob(result, rhs_str);
          }

          // If the righthand side is empty, the intersection is the lefthand side
          rhs_decoder.DecodeTagAndCovering(rhs_str);
          if (rhs_decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return StringVector::AddStringOrBlob(result, lhs_str);
          }

          // For definitely disjoint input, the intersection is empty
          if (!CoveringMayIntersect(lhs_decoder, rhs_decoder, &intersection)) {
            auto geog = make_uniq<s2geography::GeographyCollection>();
            return StringVector::AddStringOrBlob(result, encoder.Encode(*geog));
          }

          auto geog = DispatchShapeIndexFilter(
              lhs_decoder.Decode(lhs_str), rhs_decoder.Decode(rhs_str),
              [&options](const S2ShapeIndex& lhs_index, const S2ShapeIndex& rhs_index) {
                return s2geography::s2_boolean_operation(
                    lhs_index, rhs_index, S2BooleanOperation::OpType::INTERSECTION,
                    options);
              });

          return StringVector::AddStringOrBlob(result, encoder.Encode(*geog));
        });
  }

  static void ExecuteDifference(Vector& lhs, Vector& rhs, Vector& result, idx_t count) {
    GeographyDecoder lhs_decoder;
    GeographyDecoder rhs_decoder;
    GeographyEncoder encoder;
    std::vector<S2CellId> intersection;

    s2geography::GlobalOptions options;
    InitGlobalOptions(&options);

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        lhs, rhs, result, count, [&](string_t lhs_str, string_t rhs_str) {
          lhs_decoder.DecodeTagAndCovering(lhs_str);

          // If the lefthand side is empty, the difference is also empty
          if (lhs_decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            auto geog = make_uniq<s2geography::GeographyCollection>();
            return StringVector::AddStringOrBlob(result, encoder.Encode(*geog));
          }

          // If the righthand side is empty, the difference is the lefthand side
          rhs_decoder.DecodeTagAndCovering(rhs_str);
          if (rhs_decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return StringVector::AddStringOrBlob(result, lhs_str);
          }

          // For definitely disjoint input, the intersection is the lefthand side
          if (!CoveringMayIntersect(lhs_decoder, rhs_decoder, &intersection)) {
            auto geog = make_uniq<s2geography::GeographyCollection>();
            return StringVector::AddStringOrBlob(result, lhs_str);
          }

          auto geog = DispatchShapeIndexFilter(
              lhs_decoder.Decode(lhs_str), rhs_decoder.Decode(rhs_str),
              [&options](const S2ShapeIndex& lhs_index, const S2ShapeIndex& rhs_index) {
                return s2geography::s2_boolean_operation(
                    lhs_index, rhs_index, S2BooleanOperation::OpType::DIFFERENCE,
                    options);
              });

          return StringVector::AddStringOrBlob(result, encoder.Encode(*geog));
        });
  }

  static void ExecuteUnion(Vector& lhs, Vector& rhs, Vector& result, idx_t count) {
    GeographyDecoder lhs_decoder;
    GeographyDecoder rhs_decoder;
    GeographyEncoder encoder;
    std::vector<S2CellId> intersection;

    s2geography::GlobalOptions options;
    InitGlobalOptions(&options);

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        lhs, rhs, result, count, [&](string_t lhs_str, string_t rhs_str) {
          lhs_decoder.DecodeTagAndCovering(lhs_str);

          // If the lefthand side is empty, the union is the righthand side
          if (lhs_decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return StringVector::AddStringOrBlob(result, rhs_str);
          }

          // If the righthand side is empty, the union is the lefthand side
          rhs_decoder.DecodeTagAndCovering(rhs_str);
          if (rhs_decoder.tag.flags & s2geography::EncodeTag::kFlagEmpty) {
            return StringVector::AddStringOrBlob(result, lhs_str);
          }

          // (No optimization for definitely disjoint binary union)

          auto geog = DispatchShapeIndexFilter(
              lhs_decoder.Decode(lhs_str), rhs_decoder.Decode(rhs_str),
              [&options](const S2ShapeIndex& lhs_index, const S2ShapeIndex& rhs_index) {
                return s2geography::s2_boolean_operation(
                    lhs_index, rhs_index, S2BooleanOperation::OpType::UNION, options);
              });

          return StringVector::AddStringOrBlob(result, encoder.Encode(*geog));
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
};

}  // namespace

void RegisterS2GeographyPredicates(DatabaseInstance& instance) {
  S2BinaryIndexOp::Register(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
