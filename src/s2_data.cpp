#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include <absl/base/config.h>
#include <openssl/opensslv.h>
#include <s2geography.h>

#include "s2_data_static.hpp"
#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

class S2DataFunctionData : public TableFunctionData {
 public:
  S2DataFunctionData() {}
  idx_t offset{0};
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
  return make_uniq<S2DataFunctionData>();
}

void S2DataCitiesScan(ClientContext& context, TableFunctionInput& data_p,
                      DataChunk& output) {
  auto& data = data_p.bind_data->CastNoConst<S2DataFunctionData>();
  idx_t n_cities = static_cast<idx_t>(kCities.size());

  if (data.offset >= n_cities) {
    return;
  }

  idx_t start = data.offset;
  idx_t end = start + STANDARD_VECTOR_SIZE;
  if (end > n_cities) {
    end = n_cities;
  }

  s2geography::WKTReader reader;
  GeographyEncoder encoder;
  Vector& names = output.data[0];
  Vector& populations = output.data[1];
  Vector& geogs = output.data[2];

  // There seems to be some issue with constructing a Value from
  // invalid unicode (i.e., a blob), and it's unclear if SetValue()
  // will automatically call AddString(). So, we do this manually.
  auto geogs_data = reinterpret_cast<string_t*>(geogs.GetData());

  for (idx_t i = start; i < end; i++) {
    const City& city = kCities[i];
    names.SetValue(i - start, StringVector::AddString(names, city.name));
    populations.SetValue(i - start, city.population);

    auto geog = reader.read_feature(city.geog_wkt);
    string_t encoded = StringVector::AddStringOrBlob(geogs, encoder.Encode(*geog));
    geogs_data[i] = encoded;
  }

  data.offset = end;
  output.SetCardinality(end - start);
}

static inline duckdb::unique_ptr<FunctionData> S2DataCountriesBind(
    ClientContext& context, TableFunctionBindInput& input,
    vector<LogicalType>& return_types, vector<string>& names) {
  names.push_back("name");
  names.push_back("continent");
  names.push_back("geog");
  return_types.push_back(LogicalType::VARCHAR);
  return_types.push_back(LogicalType::VARCHAR);
  return_types.push_back(Types::GEOGRAPHY());
  return make_uniq<S2DataFunctionData>();
}

void S2DataCountriesScan(ClientContext& context, TableFunctionInput& data_p,
                         DataChunk& output) {
  auto& data = data_p.bind_data->CastNoConst<S2DataFunctionData>();
  idx_t n_cities = static_cast<idx_t>(kCountries.size());

  if (data.offset >= n_cities) {
    return;
  }

  idx_t start = data.offset;
  idx_t end = start + STANDARD_VECTOR_SIZE;
  if (end > n_cities) {
    end = n_cities;
  }

  s2geography::WKTReader reader;
  GeographyEncoder encoder;
  Vector& names = output.data[0];
  Vector& continents = output.data[1];
  Vector& geogs = output.data[2];

  // There seems to be some issue with constructing a Value from
  // invalid unicode (i.e., a blob), and it's unclear if SetValue()
  // will automatically call AddString(). So, we do this manually.
  auto geogs_data = reinterpret_cast<string_t*>(geogs.GetData());

  for (idx_t i = start; i < end; i++) {
    const Country& country = kCountries[i];
    names.SetValue(i - start, StringVector::AddString(names, country.name));
    continents.SetValue(i - start, StringVector::AddString(names, country.continent));

    auto geog = reader.read_feature(country.geog_wkt);
    string_t encoded = StringVector::AddStringOrBlob(geogs, encoder.Encode(*geog));
    geogs_data[i] = encoded;
  }

  data.offset = end;
  output.SetCardinality(end - start);
}

}  // namespace

void RegisterS2Data(DatabaseInstance& instance) {
  TableFunction cities_func("s2_data_cities", {}, S2DataCitiesScan, S2DataCitiesBind);
  ExtensionUtil::RegisterFunction(instance, cities_func);

  TableFunction countries_func("s2_data_countries", {}, S2DataCountriesScan,
                               S2DataCountriesBind);
  ExtensionUtil::RegisterFunction(instance, countries_func);
}

}  // namespace duckdb_s2
}  // namespace duckdb
