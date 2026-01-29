add_executable(test)

target_sources(test
  PRIVATE
  main.cpp
  PRIVATE
  FILE_SET cxx_modules TYPE CXX_MODULES FILES
  test-graph.mpp
)

target_link_libraries(test
  PRIVATE
  libgraphs
)
