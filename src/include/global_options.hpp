#pragma once

#include "s2geography/build.h"

namespace duckdb {

namespace duckdb_s2 {

// S2 Provides a number of options. This is where default options get set,
// which should perhaps be configurable from a session.
inline void InitBooleanOperationOptions(S2BooleanOperation::Options* options) {
  options->set_polygon_model(S2BooleanOperation::PolygonModel::CLOSED);
  options->set_polyline_model(S2BooleanOperation::PolylineModel::CLOSED);
}

inline void InitGlobalOptions(s2geography::GlobalOptions* options) {
  InitBooleanOperationOptions(&options->boolean_operation);
}

}  // namespace duckdb_s2
}  // namespace duckdb
