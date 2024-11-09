
#include "s2_types.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

namespace duckdb_s2 {
LogicalType Types::S2_POINT() {
  auto type = LogicalType::STRUCT({{"x", LogicalType::DOUBLE},
                                   {"y", LogicalType::DOUBLE},
                                   {"z", LogicalType::DOUBLE}});
  type.SetAlias("S2_POINT");
  return type;
}

LogicalType Types::S2_CELL() {
  LogicalType type = LogicalType::BIGINT;
  type.SetAlias("S2_CELL");
  return type;
}

LogicalType Types::GEOGRAPHY() {
  LogicalType type = LogicalType::BLOB;
  type.SetAlias("GEOGRAPHY");
  return type;
}

void RegisterTypes(DatabaseInstance& instance) {
  ExtensionUtil::RegisterType(instance, "S2_POINT", Types::S2_POINT());
  ExtensionUtil::RegisterType(instance, "S2_CELL", Types::S2_CELL());
  ExtensionUtil::RegisterType(instance, "GEOGRAPHY", Types::GEOGRAPHY());
}

}  // namespace duckdb_s2
}  // namespace duckdb
