
## Test Data

Generated with:

```r
library(s2)

cities <- s2_data_tbl_cities
cities$geometry <- s2_as_text(cities$geometry, precision = 9)
cities |>
  readr::write_tsv("~/Desktop/rscratch/duckdb_s2/data/cities.tsv")

countries <- s2_data_tbl_countries
countries$geometry <- countries$geometry |>
  s2_rebuild(options = s2_options(snap = s2_snap_precision(1e6), duplicate_edges = FALSE, validate = TRUE))
countries$geometry <- s2_as_text(countries$geometry, precision = 9)

countries |>
  readr::write_tsv("~/Desktop/rscratch/duckdb_s2/data/countries.tsv")
```
