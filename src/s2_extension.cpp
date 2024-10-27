#define DUCKDB_EXTENSION_MAIN

#include "s2_extension.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include "s2_dependencies.hpp"
#include "s2_types.hpp"

namespace duckdb {

inline void S2ScalarFun(DataChunk& args, ExpressionState& state, Vector& result) {
  result.SetVectorType(VectorType::CONSTANT_VECTOR);
  result.SetValue(0, "s2");
}

static void LoadInternal(DatabaseInstance& instance) {
  // Register a scalar function
  auto s2_scalar_function = ScalarFunction("s2", {}, LogicalType::VARCHAR, S2ScalarFun);
  ExtensionUtil::RegisterFunction(instance, s2_scalar_function);

  duckdb_s2::RegisterTypes(instance);
  duckdb_s2::RegisterS2Dependencies(instance);
}

void S2Extension::Load(DuckDB& db) { LoadInternal(*db.instance); }
std::string S2Extension::Name() { return "s2"; }

std::string S2Extension::Version() const {
#ifdef EXT_VERSION_S2
  return EXT_VERSION_S2;
#else
  return "";
#endif
}

}  // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void s2_init(duckdb::DatabaseInstance& db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::S2Extension>();
}

DUCKDB_EXTENSION_API const char* s2_version() { return duckdb::DuckDB::LibraryVersion(); }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
