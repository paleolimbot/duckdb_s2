
#include "s2_point_ops.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2geography/op/cell.h"
#include "s2geography/op/point.h"

#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

using s2geography::op::point::Point;

inline void S2CellFromPoint(DataChunk& args, ExpressionState& state, Vector& result) {
  idx_t count = args.size();

  auto& children = StructVector::GetEntries(args.data[0]);
  auto& x = children[0];
  auto& y = children[1];
  auto& z = children[2];
  x->Flatten(count);
  y->Flatten(count);
  z->Flatten(count);
  double* x_ptr = reinterpret_cast<double*>(x->GetData());
  double* y_ptr = reinterpret_cast<double*>(y->GetData());
  double* z_ptr = reinterpret_cast<double*>(z->GetData());

  s2geography::op::cell::FromPoint op;
  for (idx_t i = 0; i < count; i++) {
    Point pt{x_ptr[i], y_ptr[i], z_ptr[i]};
    result.SetValue(i, static_cast<int64_t>(op.ExecuteScalar(pt)));
  }

  if (count == 1) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
}

void RegisterS2CellOps(DatabaseInstance& instance) {
  auto s2_geog_point = ScalarFunction("s2_cell_from_point", {Types::S2_POINT()},
                                      Types::S2_CELL(), S2CellFromPoint);
  ExtensionUtil::RegisterFunction(instance, s2_geog_point);
}

}  // namespace duckdb_s2
}  // namespace duckdb
