# DuckDB Geography

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, geography, allows you leverage [Google's s2geometry library](https://github.com/google/s2geometry) via the [s2geography wrapper library](https://github.com/paleolimbot/s2geography) that also powers S2 integration as an [R package](https://r-spatial.github.io/s2) and a [Python library](https://github.com/benbovy/spherely). It is preliminary and not currently published as a community extension.

In general, the functions are the same as those implemented in the [spatial extension](https://duckdb.org/docs/extensions/spatial/functions.html) except they are prefixed with `s2_` instead of `st_`.

```
LOAD geography;

D CREATE TABLE countries as SELECT name, s2_prepare(geog) as geog FROM s2_data_countries();
D SELECT
    countries.name as country, cities.name as city, cities.geog as geog
  FROM countries
    INNER JOIN s2_data_cities() AS cities
    ON s2_intersects(countries.geog, cities.geog);

┌──────────────────────────┬──────────────────┬────────────────────────────────────────────────┐
│         country          │       city       │                      geog                      │
│         varchar          │     varchar      │                   geography                    │
├──────────────────────────┼──────────────────┼────────────────────────────────────────────────┤
│ Afghanistan              │ Kabul            │ POINT (69.18131420000002 34.5186361)           │
│ Angola                   │ Luanda           │ POINT (13.2324812 -8.836340260000002)          │
│ Albania                  │ Tirana           │ POINT (19.818883 41.3275407)                   │
│ United Arab Emirates     │ Abu Dhabi        │ POINT (54.3665934 24.466683599999996)          │
│ United Arab Emirates     │ Dubai            │ POINT (55.2780285 25.231942000000004)          │
│ Argentina                │ Buenos Aires     │ POINT (-58.39947719999999 -34.6005557)         │
│ Armenia                  │ Yerevan          │ POINT (44.5116055 40.1830966)                  │
│ Australia                │ Canberra         │ POINT (149.129026 -35.2830285)                 │
│ Australia                │ Melbourne        │ POINT (144.97307 -37.8180855)                  │
│ Australia                │ Sydney           │ POINT (151.183234 -33.9180651)                 │
│ Austria                  │ Vaduz            │ POINT (9.51666947 47.1337238)                  │
│ Austria                  │ Vienna           │ POINT (16.364693100000004 48.2019611)          │
│ Azerbaijan               │ Baku             │ POINT (49.8602713 40.3972179)                  │
│ Burundi                  │ Bujumbura        │ POINT (29.360006100000003 -3.3760872200000005) │
│ Belgium                  │ Brussels         │ POINT (4.33137075 50.8352629)                  │
│ Benin                    │ Porto-Novo       │ POINT (2.6166255300000003 6.483310970000001)   │
│ Benin                    │ Cotonou          │ POINT (2.51804474 6.40195442)                  │
│ Burkina Faso             │ Ouagadougou      │ POINT (-1.52666961 12.3722618)                 │
│ Bangladesh               │ Dhaka            │ POINT (90.4066336 23.7250056)                  │
│ Bulgaria                 │ Sofia            │ POINT (23.314708199999995 42.6852953)          │
│    ·                     │  ·               │                   ·                            │
│    ·                     │  ·               │                   ·                            │
│    ·                     │  ·               │                   ·                            │
│ Ukraine                  │ Kiev             │ POINT (30.514682099999998 50.4353132)          │
│ United States of America │ San Francisco    │ POINT (-122.417169 37.7691956)                 │
│ United States of America │ Denver           │ POINT (-104.98596200000001 39.7411339)         │
│ United States of America │ Houston          │ POINT (-95.3419251 29.821920199999994)         │
│ United States of America │ Miami            │ POINT (-80.2260519 25.7895566)                 │
│ United States of America │ Atlanta          │ POINT (-84.4018952 33.8319597)                 │
│ United States of America │ Chicago          │ POINT (-87.7520008 41.8319365)                 │
│ United States of America │ Los Angeles      │ POINT (-118.181926 33.991924100000006)         │
│ United States of America │ Washington, D.C. │ POINT (-77.0113644 38.9014952)                 │
│ United States of America │ New York         │ POINT (-73.9819628 40.75192489999999)          │
│ Uzbekistan               │ Tashkent         │ POINT (69.292987 41.3136477)                   │
│ Venezuela                │ Caracas          │ POINT (-66.9189831 10.502944399999999)         │
│ Vietnam                  │ Hanoi            │ POINT (105.848068 21.035273099999998)          │
│ Yemen                    │ Sanaa            │ POINT (44.20464750000001 15.356679200000002)   │
│ South Africa             │ Bloemfontein     │ POINT (26.2299129 -29.119993899999994)         │
│ South Africa             │ Pretoria         │ POINT (28.2274832 -25.7049747)                 │
│ South Africa             │ Johannesburg     │ POINT (28.028063900000003 -26.168098900000004) │
│ South Africa             │ Cape Town        │ POINT (18.433042299999997 -33.9180651)         │
│ Zambia                   │ Lusaka           │ POINT (28.281381699999997 -15.4146984)         │
│ Zimbabwe                 │ Harare           │ POINT (31.0427636 -17.8158438)                 │
├──────────────────────────┴──────────────────┴────────────────────────────────────────────────┤
│ 210 rows (40 shown)                                                                3 columns │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Installation

The s2 extension is not currently a community extension (although could be in the future!). To use it, you'll have to grab a binary from the CI job on the main branch and load it after allowing
unsigned extensions in your DuckDB session.

```python

```

## Types

The geography extension defines the following types:

- `GEOGRAPHY`: A (multi)point, (multi)linestring, (multi)polygon, or an arbitrary
  collection of those where coordinates are represented as geodedic longitude, latitude on
  the WGS84 ellipsoid and edges are represented as geodesics approximated on the
  sphere. This is exactly the same as the definition of coordinates and edges in
  [BigQuery Geography](https://cloud.google.com/bigquery/docs/geospatial-data#coordinate_systems_and_edges).

  The underlying representation of the `GEOGRAPHY` type is a `BLOB`. The exact
  packing of bytes in this blob is not currently guaranteed but is intended to
  be documented when stable such that other libraries can decode the value
  independently.

- `S2_CELL`: A cell in [S2's cell indexing system](http://s2geometry.io/devguide/s2cell_hierarchy).
  Briefly, this is a way to encode every ~2cm square on earth with an unsigned 64-bit
  integer. The indexing system is heiarchical with
  [31 levels](http://s2geometry.io/resources/s2cell_statistics).

- `S2_CELL_CENTER`: The center of an `S2_CELL`. This shares a physical representation
  of the `S2_CELL` but has a different logical meaning (a point rather than a polygon).
  This is a compact mechanism to encode a point (8 bytes) and can be more efficiently
  compared for intersection and containment against an `S2_CELL` or `S2_CELL_UNION`.
  For maximum efficiency, always store points as cell centers (they can be loaded
  directly from WKB using `s2_cellfromwkb()` created from longitude and latitude
  with `s2_cellfromlonlat()`, or casted from an existing `GEOGRAPHY`).

- `S2_CELL_UNION`: A normalized list of `S2_CELL`s. This can be used to
   approximate a polygon and is used internally as a rapid mechanism for
   approximating the bounds of a `GEOGRAPHY` in a way that is more efficient
   to compare for possible intersection. This covering can be generated
   with `s2_covering()`.

## Functions

Currently implemented functions are listed below. Documentation is a work in progress!
Note that all types listed above are implicitly castable to `GEOGRAPHY` such that
you can use them with any function that accepts a `GEOGRAPHY`. In general, functions
are intended to have the same behaviour as the equivalent `ST_xx()` function
(if it exists).

Documentation is a work in progress! If you need a function that is missing, open
an issue (most functions have already been ported to the underlying C++ library
and just aren't wired up to DuckDB yet).

### Test data

You can use the table functions `s2_data_cities()` and `s2_data_countries()` to
load tables useful for testing. You can also use `s2_data_city('<city name>')` and
`s2_data_country('<country name>')` to get a scalar `GEOGRAPHY`.

### Geography accessors

Note that areas, lengths, and distances are currently spherical approximations
(i.e., do not take into account the WGS84 ellipsoid).

- `s2_isempty(GEOGRAPHY)`
- `s2_area(GEOGRAPHY)`
- `s2_perimeter(GEOGRAPHY)`
- `s2_length(GEOGRAPHY)`

### Input/output

- `s2_geogfromwkb(BLOB)`
- `s2_aswkb(GEOGRAPHY)`
- `CAST(VARCHAR AS GEOGRAPHY)` (e.g., `'POINT (-64 45)::GEOGRAPHY`).
- `CAST(GEOGRAPHY AS VARCHAR)` (e.g., `'POINT (-64 45)::GEOGRAPHY::VARCHAR`).
- `s2_format(GEOGRAPHY, INTEGER)` (WKT export with trimmed precision)
- `s2_prepare(GEOGRAPHY)`: Builds an internal index representationthat can
  be efficiently reused for multiple comparisons.

### Geography predicates

Note that predicate operations need to build an edge index for each
geography for every comparison. If you suspect you will need to perform
many comparisons against a single geography (e.g., point in polygon in
a join), you can use `CREATE TABLE ... AS SELECT s2_prepare(geog) FROM ...`
to avoid a costly recomputation of the index for each element.

- `s2_mayintersect(GEOGRAPHY, GEOGRAPHY)`
- `s2_intersects(GEOGRAPHY, GEOGRAPHY)`
- `s2_contains(GEOGRAPHY, GEOGRAPHY)`
- `s2_equals(GEOGRAPHY, GEOGRAPHY)`

### Overlay operations

- `s2_intersects(GEOGRAPHY, GEOGRAPHY)`
- `s2_difference(GEOGRAPHY, GEOGRAPHY)`
- `s2_union(GEOGRAPHY, GEOGRAPHY)`

### Bounds

- `s2_covering(GEOGRAPHY)`

### Cell operators

- `s2_cellfromwkb(BLOB)`
- `s2_cellfromlonlat()`
- `s2_cell_from_token(VARCHAR)`
- `s2_cell_token(S2_CELL)`
- `s2_cell_level(S2_CELL)`
- `s2_cell_parent(S2_CELL, INTEGER)`
- `s2_cell_child(S2_CELL, INTEGER)`
- `s2_cell_edge_neighbor(S2_CELL, INTEGER)`
- `s2_cell_contains(S2_CELL, S2_CELL)`
- `s2_cell_intersects(S2_CELL, S2_CELL)`

### Version information

- `s2_dependencies()`
