# libFuzzer targets for the parsers of untrusted input (see README.md).
# Configured only with WITH_FUZZERS=ON and Clang; the whole build is expected
# to use '-fsanitize=fuzzer-no-link,address,undefined' (CMAKE_C_FLAGS /
# CMAKE_CXX_FLAGS) so that the libraries under test are instrumented too,
# and each target links the libFuzzer runtime itself.

set(fuzzer_link_options -fsanitize=fuzzer)

# the binlog event stream 'fetch' / 'pull' receive from a source
add_executable(event_stream_fuzzer event_stream_fuzzer.cpp)
target_include_directories(event_stream_fuzzer PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(event_stream_fuzzer
  PRIVATE
    binlog_server_compiler_flags
    binsrv::lib_events
)
target_link_options(event_stream_fuzzer PRIVATE ${fuzzer_link_options})
set_target_properties(event_stream_fuzzer PROPERTIES
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
)

# client packets to the replication source listener of 'pull';
# connection_context is part of the executable's minimysql sources, so it is
# compiled in directly, as in tests/minimysql_property_test
add_executable(listener_packet_fuzzer
  listener_packet_fuzzer.cpp
  "${PROJECT_SOURCE_DIR}/src/minimysql/connection_context.cpp"
  "${PROJECT_SOURCE_DIR}/src/minimysql/caching_sha2_password_authenticator.cpp"
)
target_include_directories(listener_packet_fuzzer
  PRIVATE
    "${PROJECT_SOURCE_DIR}/src"
    "${PROJECT_SOURCE_DIR}/extra/mysql_protocol"
)
target_link_libraries(listener_packet_fuzzer
  PRIVATE
    binlog_server_compiler_flags
    binsrv::lib_opensslpp
    binsrv::lib_util
    Boost::headers
    MySQL::client
)
target_link_options(listener_packet_fuzzer PRIVATE ${fuzzer_link_options})
set_target_properties(listener_packet_fuzzer PROPERTIES
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
)

# stored binlog data read back by the replication source of 'pull'; the
# sender is part of the executable's operations sources, so it is compiled in
# directly, as in tests/sender_context_property_test
add_executable(stored_binlog_fuzzer
  stored_binlog_fuzzer.cpp
  "${PROJECT_SOURCE_DIR}/src/operations/sender_context.cpp"
  "${PROJECT_SOURCE_DIR}/src/operations/sender_single_file_context.cpp"
  "${PROJECT_SOURCE_DIR}/src/operations/event_generation_helpers.cpp"
)
target_include_directories(stored_binlog_fuzzer PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(stored_binlog_fuzzer
  PRIVATE
    binlog_server_compiler_flags
    binsrv::lib_binsrv
)
target_link_options(stored_binlog_fuzzer PRIVATE ${fuzzer_link_options})
set_target_properties(stored_binlog_fuzzer PROPERTIES
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
)

# the small decoders of user and network input (GTID sets, binlog names,
# server versions, timestamps, size and time units, heartbeat bodies)
add_executable(decoders_fuzzer decoders_fuzzer.cpp)
target_include_directories(decoders_fuzzer PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(decoders_fuzzer
  PRIVATE
    binlog_server_compiler_flags
    binsrv::lib_binsrv
)
target_link_options(decoders_fuzzer PRIVATE ${fuzzer_link_options})
set_target_properties(decoders_fuzzer PROPERTIES
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
)
