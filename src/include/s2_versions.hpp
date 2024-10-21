#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"

// OpenSSL linked through vcpkg
#include <absl/base/config.h>
#include <openssl/opensslv.h>
#include <s2geography.h>

namespace duckdb {

namespace {
class S2VersionsFunctionData : public TableFunctionData {
 public:
  S2VersionsFunctionData() : finished(false) {}
  bool finished{false};
};

static inline duckdb::unique_ptr<FunctionData> S2VersionsBind(
    ClientContext& context, TableFunctionBindInput& input,
    vector<LogicalType>& return_types, vector<string>& names) {
  names.push_back("dependency");
  names.push_back("version");
  return_types.push_back(LogicalType::VARCHAR);
  return_types.push_back(LogicalType::VARCHAR);
  return make_uniq<S2VersionsFunctionData>();
}

static inline void S2VersionsFunction(ClientContext& context, TableFunctionInput& data_p,
                                      DataChunk& output) {
  auto& data = data_p.bind_data->CastNoConst<S2VersionsFunctionData>();
  if (data.finished) {
    return;
  }

  output.SetValue(0, 0, "openssl");
  output.SetValue(1, 0,
                  std::string() + std::to_string(OPENSSL_VERSION_MAJOR) + "." +
                      std::to_string(OPENSSL_VERSION_MINOR) + "." +
                      std::to_string(OPENSSL_VERSION_PATCH));
  output.SetValue(0, 1, "abseil-cpp");
  output.SetValue(1, 1,
                  std::string() + std::to_string(ABSL_LTS_RELEASE_VERSION) + "." +
                      std::to_string(ABSL_LTS_RELEASE_PATCH_LEVEL));
  output.SetValue(0, 2, "s2geometry");
  output.SetValue(1, 2,
                  std::string() + std::to_string(S2_VERSION_MAJOR) + "." +
                      std::to_string(S2_VERSION_MINOR) + "." +
                      std::to_string(S2_VERSION_PATCH));
  output.SetCardinality(3);
  data.finished = true;
}

}  // namespace

inline void RegisterVersionFunctions(DatabaseInstance& instance) {
  TableFunction versions_func("s2_dependencies", {}, S2VersionsFunction, S2VersionsBind);
  ExtensionUtil::RegisterFunction(instance, versions_func);
}

}  // namespace duckdb
