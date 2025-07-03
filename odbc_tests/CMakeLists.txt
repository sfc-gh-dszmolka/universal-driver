cmake_minimum_required(VERSION 3.20)
project(odbc_tests)

enable_testing()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

Include(FetchContent)

FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG        v3.8.1 # or a later release
)

FetchContent_MakeAvailable(Catch2)

FetchContent_Declare(
  picojson
  GIT_REPOSITORY https://github.com/kazuho/picojson.git
  GIT_TAG v1.3.0
)

FetchContent_MakeAvailable(picojson)

# Find odbc library
find_package(ODBC REQUIRED)
message(STATUS "ODBC_INCLUDE_DIRS: ${ODBC_INCLUDE_DIRS}")
message(STATUS "ODBC_LIBRARIES: ${ODBC_LIBRARIES}")

# Add tests
add_executable(tests test.cpp)
target_link_libraries(tests PRIVATE Catch2::Catch2WithMain ODBC::ODBC)
target_include_directories(tests PRIVATE ${picojson_SOURCE_DIR})
add_test(NAME tests COMMAND tests)
