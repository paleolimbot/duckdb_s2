#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/main/database.hpp"


namespace duckdb {

namespace duckdb_s2 {

struct Types {
    static LogicalType S2_CELL();
    static LogicalType GEOGRAPHY();
};

void RegisterTypes(DatabaseInstance &instance);

}
}
