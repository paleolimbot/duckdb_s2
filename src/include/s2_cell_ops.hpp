#pragma once

#include "duckdb/main/database.hpp"

namespace duckdb {

namespace duckdb_s2 {

void RegisterS2CellOps(DatabaseInstance& instance);

}
}  // namespace duckdb
