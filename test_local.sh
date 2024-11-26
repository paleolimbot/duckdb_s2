#!/usr/bin/env bash

# Cheap version of make test that works with cmake
SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SOURCE_DIR_NAME="$(basename "${SOURCE_DIR}")"

"$SOURCE_DIR/build/test/unittest" "*/${SOURCE_DIR_NAME}/*"
