#!/bin/sh -l

set -e

echo "Running code coverage"
cargo tarpaulin --workspace --out Lcov --output-dir ./coverage/ 