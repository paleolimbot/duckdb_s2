#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include <absl/base/config.h>
#include <openssl/opensslv.h>
#include <s2geography.h>

#include "s2_types.hpp"
#include "s2_data_static.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

class S2DataCitiesFunctionData : public TableFunctionData {
 public:
  S2DataCitiesFunctionData() : finished(false) {}
  bool finished{false};
};

static inline duckdb::unique_ptr<FunctionData> S2DataCitiesBind(
    ClientContext& context, TableFunctionBindInput& input,
    vector<LogicalType>& return_types, vector<string>& names) {
  names.push_back("name");
  names.push_back("population");
  names.push_back("geog");
  return_types.push_back(LogicalType::VARCHAR);
  return_types.push_back(LogicalType::INTEGER);
  return_types.push_back(Types::GEOGRAPHY());
  return make_uniq<S2DataCitiesFunctionData>();
}

void S2DataCitiesScan(ClientContext& context, TableFunctionInput& data_p,
                        DataChunk& output) {
  auto& data = data_p.bind_data->CastNoConst<S2DataCitiesFunctionData>();
  if (data.finished) {
    return;
  }

//   output.SetValue(0, 0, "openssl");
//   output.SetValue(1, 0,
//                   std::string() + std::to_string(OPENSSL_VERSION_MAJOR) + "." +
//                       std::to_string(OPENSSL_VERSION_MINOR) + "." +
//                       std::to_string(OPENSSL_VERSION_PATCH));
//   output.SetValue(0, 1, "abseil-cpp");
//   output.SetValue(1, 1,
//                   std::string() + std::to_string(ABSL_LTS_RELEASE_VERSION) + "." +
//                       std::to_string(ABSL_LTS_RELEASE_PATCH_LEVEL));
//   output.SetValue(0, 2, "s2geometry");
//   output.SetValue(1, 2,
//                   std::string() + std::to_string(S2_VERSION_MAJOR) + "." +
//                       std::to_string(S2_VERSION_MINOR) + "." +
//                       std::to_string(S2_VERSION_PATCH));
//   output.SetCardinality(3);
  data.finished = true;
}


class S2DataCountriesFunctionData : public TableFunctionData {
 public:
  S2DataCountriesFunctionData() : finished(false) {}
  bool finished{false};
};

static inline duckdb::unique_ptr<FunctionData> S2DataCountriesBind(
    ClientContext& context, TableFunctionBindInput& input,
    vector<LogicalType>& return_types, vector<string>& names) {
  names.push_back("name");
  names.push_back("continent");
  names.push_back("geog");
  return_types.push_back(LogicalType::VARCHAR);
  return_types.push_back(LogicalType::VARCHAR);
  return_types.push_back(Types::GEOGRAPHY());
  return make_uniq<S2DataCountriesFunctionData>();
}

void S2DataCountriesScan(ClientContext& context, TableFunctionInput& data_p,
                        DataChunk& output) {
  auto& data = data_p.bind_data->CastNoConst<S2DataCountriesFunctionData>();
  if (data.finished) {
    return;
  }

//   output.SetValue(0, 0, "openssl");
//   output.SetValue(1, 0,
//                   std::string() + std::to_string(OPENSSL_VERSION_MAJOR) + "." +
//                       std::to_string(OPENSSL_VERSION_MINOR) + "." +
//                       std::to_string(OPENSSL_VERSION_PATCH));
//   output.SetValue(0, 1, "abseil-cpp");
//   output.SetValue(1, 1,
//                   std::string() + std::to_string(ABSL_LTS_RELEASE_VERSION) + "." +
//                       std::to_string(ABSL_LTS_RELEASE_PATCH_LEVEL));
//   output.SetValue(0, 2, "s2geometry");
//   output.SetValue(1, 2,
//                   std::string() + std::to_string(S2_VERSION_MAJOR) + "." +
//                       std::to_string(S2_VERSION_MINOR) + "." +
//                       std::to_string(S2_VERSION_PATCH));
//   output.SetCardinality(3);
  data.finished = true;
}

}  // namespace

void RegisterS2Data(DatabaseInstance& instance) {
  TableFunction cities_func("s2_data_cities", {}, S2DataCitiesScan,
                              S2DataCitiesBind);
  ExtensionUtil::RegisterFunction(instance, cities_func);

  TableFunction countries_func("s2_data_countries", {}, S2DataCountriesScan,
                              S2DataCountriesBind);
  ExtensionUtil::RegisterFunction(instance, countries_func);
}

}  // namespace duckdb_s2
}  // namespace duckdb
