cmake_minimum_required(VERSION 4.0)

set(CMAKE_EXPERIMENTAL_CXX_IMPORT_STD
  "d0edc3af-4c50-42ea-a356-e2862fe7a444"
)

project(graphs VERSION 0.0.1 LANGUAGES CXX)

include(CTest)

set(CMAKE_CXX_MODULE_STD 1)

set(CMAKE_CXX_STANDARD 23)

add_subdirectory(libgraphs)
add_subdirectory(libMNA)

if (BUILD_TESTING)
  add_subdirectory(test)
endif()
