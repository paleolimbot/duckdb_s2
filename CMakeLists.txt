cmake_minimum_required(VERSION 3.5)

include(FetchContent)

# Set extension name here
set(TARGET_NAME s2)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(SWIG_FOUND FALSE)
set(BUILD_SHARED_LIBS OFF)
# FetchContent_Declare(
#     s2geography
#     GIT_REPOSITORY https://github.com/paleolimbot/s2geography.git
#     GIT_TAG e141957e848a499f8b83ca0347bb914aeea61ce2
#     GIT_SHALLOW TRUE)
FetchContent_Declare(s2geography DIRECTORY ../s2-geography)

FetchContent_MakeAvailable(s2geography)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/s2_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} s2geography s2 OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} s2geography s2 OpenSSL::SSL OpenSSL::Crypto)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
