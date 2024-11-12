#pragma once

#include "duckdb/main/database.hpp"

namespace duckdb {

namespace duckdb_s2 {

void RegisterS2GeographyFunctionsIO(DatabaseInstance& instance);
void RegisterS2GeographyPredicates(DatabaseInstance& instance);

inline void RegisterS2GeographyOps(DatabaseInstance& instance) {
  RegisterS2GeographyFunctionsIO(instance);
  RegisterS2GeographyPredicates(instance);
}

}  // namespace duckdb_s2
}  // namespace duckdb
