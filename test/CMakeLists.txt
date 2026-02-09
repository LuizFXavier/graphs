
add_executable(cliTest)

target_sources(cliTest
  PRIVATE
  unit/cli_parser.test.cpp)

target_link_libraries(cliTest
  PRIVATE
  libMNA
)
add_test(NAME cli COMMAND cliTest)

add_executable(dsTest)

target_sources(dsTest
  PRIVATE
  unit/directory_scanner.test.cpp)

target_link_libraries(dsTest
  PRIVATE
  libMNA
)

add_test(NAME directory_scanner COMMAND dsTest)
