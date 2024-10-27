
#include "s2_point_ops.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2geography/op/point.h"

#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

using s2geography::op::point::Point;
using s2geography::op::point::ToPoint;

inline void S2GeogPoint(DataChunk& args, ExpressionState& state, Vector& result) {
  D_ASSERT(args.data.size() == 2);
  idx_t count = args.size();

  auto& lng = args.data[0];
  auto& lat = args.data[1];
  lng.Flatten(count);
  lat.Flatten(count);
  auto lng_ptr = reinterpret_cast<double*>(lng.GetData());
  auto lat_ptr = reinterpret_cast<double*>(lat.GetData());

  auto& children = StructVector::GetEntries(result);
  auto& x_child = children[0];
  auto& y_child = children[1];
  auto& z_child = children[2];

  ToPoint to_point;
  for (idx_t i = 0; i < count; i++) {
    Point pt = to_point.ExecuteScalar({lng_ptr[i], lat_ptr[i]});
    x_child->SetValue(i, pt[0]);
    y_child->SetValue(i, pt[1]);
    z_child->SetValue(i, pt[2]);
  }

  if (count == 1) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
}

void RegisterS2PointOps(DatabaseInstance& instance) {
  auto s2_geog_point =
      ScalarFunction("s2_geog_point", {LogicalType::DOUBLE, LogicalType::DOUBLE},
                     Types::S2_POINT(), S2GeogPoint);
  ExtensionUtil::RegisterFunction(instance, s2_geog_point);
}

}  // namespace duckdb_s2
}  // namespace duckdb
