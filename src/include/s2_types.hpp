#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/main/database.hpp"


namespace duckdb {

namespace duckdb_s2 {

struct Types {
	static LogicalType S2_POINT();
    static LogicalType S2_CELL();
    static LogicalType S2_GEOGRAPHY();
};

void RegisterTypes(DatabaseInstance &instance);

}
}
