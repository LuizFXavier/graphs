
add_executable(cliTest)

target_sources(cliTest
  PRIVATE
  unit/cli_parser.test.cpp)

target_link_libraries(cliTest
  PRIVATE
  libMNA
)

add_test(NAME cli COMMAND cliTest)
