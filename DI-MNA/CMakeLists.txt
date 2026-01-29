add_executable(di-mna)

target_sources(di-mna
  PRIVATE
  main.cpp
  PRIVATE
  # FILE_SET cxx_modules TYPE CXX_MODULES FILES
  # test-graph.mpp
)

target_link_libraries(di-mna
  PRIVATE
  libgraphs
)
