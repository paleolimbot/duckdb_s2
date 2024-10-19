#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>
#include <s2geography.h>

namespace duckdb {

namespace {
// This sort of has no arguments and returns a scalar but it's unclear how to do that
inline void OpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, OPENSSL_VERSION_TEXT);
        });
}
}

inline void RegisterVersionFunctions(DatabaseInstance &instance) {
    // Register another scalar function
    auto s2_openssl_version_scalar_function = ScalarFunction("s2_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, OpenSSLVersionScalarFun);
    ExtensionUtil::RegisterFunction(instance, s2_openssl_version_scalar_function);
}

}


