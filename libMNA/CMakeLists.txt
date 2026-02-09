
set(CMAKE_CXX_STANDARD 23)

add_library(libMNA)

target_sources(libMNA
  PUBLIC
  FILE_SET cxx_modules TYPE CXX_MODULES FILES
  model/iot_network.mpp
  io/cli_parser.mpp
  io/ParquetReader.mpp
  io/directory_scanner.mpp
)

set_target_properties(libMNA PROPERTIES PREFIX "")
