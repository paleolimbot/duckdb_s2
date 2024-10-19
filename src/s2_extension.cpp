#define DUCKDB_EXTENSION_MAIN

#include "s2_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>
#include <s2geography.h>

#include "s2_versions.hpp"

namespace duckdb {

inline void S2ScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "S2 "+name.GetString()+" 🐥");;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto s2_scalar_function = ScalarFunction("s2", {LogicalType::VARCHAR}, LogicalType::VARCHAR, S2ScalarFun);
    ExtensionUtil::RegisterFunction(instance, s2_scalar_function);

    RegisterVersionFunctions(instance);
}

void S2Extension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string S2Extension::Name() {
	return "s2";
}

std::string S2Extension::Version() const {
#ifdef EXT_VERSION_S2
	return EXT_VERSION_S2;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void s2_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::S2Extension>();
}

DUCKDB_EXTENSION_API const char *s2_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
