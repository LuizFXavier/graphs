set(CMAKE_CXX_STANDARD 23)

add_library(libdimna)

target_sources(libdimna
  # PUBLIC
  # FILE_SET cxx_modules TYPE CXX_MODULES FILES
  # model/Graph.mpp
)

# target_compile_options(libgraphs PRIVATE -Werror)

set_target_properties(libdimna PROPERTIES PREFIX "")
