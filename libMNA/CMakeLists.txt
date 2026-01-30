
set(CMAKE_CXX_STANDARD 23)

add_library(libMNA)

target_sources(libMNA
  PUBLIC
  FILE_SET cxx_modules TYPE CXX_MODULES FILES
  model/IoTNetwork.mpp
  io/commandLineParser.mpp
  io/parquetReader.mpp
)

set_target_properties(libMNA PROPERTIES PREFIX "")
