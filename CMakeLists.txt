cmake_minimum_required(VERSION 3.20)
project(distq LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
FetchContent_Declare(googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_library(distq_lib STATIC
    src/segment.cpp
    src/partition.cpp
    src/broker.cpp
    src/consumer.cpp)
target_include_directories(distq_lib PUBLIC src)
target_compile_options(distq_lib PRIVATE
    $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall -Wextra>
    $<$<CXX_COMPILER_ID:MSVC>:/W4>)

add_executable(distq_tests tests/test_distq.cpp)
target_link_libraries(distq_tests PRIVATE distq_lib GTest::gtest_main)

enable_testing()
add_test(NAME distq_tests COMMAND distq_tests)
