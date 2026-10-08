# Copyright (c) 2023-2024 Percona and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

find_package(Boost REQUIRED COMPONENTS unit_test_framework)

add_executable(byte_span_encoding_test byte_span_encoding_test.cpp)
target_include_directories(byte_span_encoding_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(byte_span_encoding_test
  PRIVATE
    binlog_server_compiler_flags
    binsrv::lib_util
    Boost::unit_test_framework
)
set_target_properties(byte_span_encoding_test PROPERTIES
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
)

add_executable(uuid_test uuid_test.cpp)
target_include_directories(uuid_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(uuid_test
  PRIVATE
    binlog_server_compiler_flags
    binsrv::lib_gtids
    Boost::unit_test_framework
)
set_target_properties(uuid_test PROPERTIES
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
)

add_executable(tag_test tag_test.cpp)
target_include_directories(tag_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(tag_test
  PRIVATE
    binlog_server_compiler_flags
    binsrv::lib_gtids
    Boost::unit_test_framework
)
set_target_properties(tag_test PROPERTIES
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
)

add_executable(gtid_test gtid_test.cpp)
target_include_directories(gtid_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(gtid_test
  PRIVATE
    binlog_server_compiler_flags
    binsrv::lib_gtids
    Boost::unit_test_framework
)
set_target_properties(gtid_test PROPERTIES
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
)

add_executable(gtid_set_test gtid_set_test.cpp)
target_include_directories(gtid_set_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(gtid_set_test
  PRIVATE
    binlog_server_compiler_flags
    binsrv::lib_gtids
    Boost::unit_test_framework
)
set_target_properties(gtid_set_test PROPERTIES
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
)

add_executable(event_test event_test.cpp)
target_include_directories(event_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(event_test
  PRIVATE
    binlog_server_compiler_flags
    binsrv::lib_gtids
    binsrv::lib_events
    Boost::unit_test_framework
    ZLIB::ZLIB
)
set_target_properties(event_test PROPERTIES
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
)

add_executable(cipher_context_test cipher_context_test.cpp)
target_include_directories(cipher_context_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(cipher_context_test
  PRIVATE
    binlog_server_compiler_flags
    binsrv::lib_opensslpp
    Boost::unit_test_framework
)
set_target_properties(cipher_context_test PROPERTIES
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
)

add_executable(crypto_rnd_test crypto_rnd_test.cpp)
target_include_directories(crypto_rnd_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(crypto_rnd_test
  PRIVATE
    binlog_server_compiler_flags
    binsrv::lib_opensslpp
    Boost::unit_test_framework
)
set_target_properties(crypto_rnd_test PROPERTIES
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
)

add_executable(digest_context_test digest_context_test.cpp)
target_include_directories(digest_context_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(digest_context_test
  PRIVATE
    binlog_server_compiler_flags
    binsrv::lib_opensslpp
    Boost::unit_test_framework
)
set_target_properties(digest_context_test PROPERTIES
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
)


set(test_run_options --no_color_output)
add_test(NAME byte_span_encoding_test COMMAND byte_span_encoding_test ${test_run_options})
add_test(NAME uuid_test COMMAND uuid_test ${test_run_options})
add_test(NAME tag_test COMMAND tag_test ${test_run_options})
add_test(NAME gtid_test COMMAND gtid_test ${test_run_options})
add_test(NAME gtid_set_test COMMAND gtid_set_test ${test_run_options})
add_test(NAME event_test COMMAND event_test ${test_run_options})
add_test(NAME cipher_context_test COMMAND cipher_context_test ${test_run_options})
add_test(NAME crypto_rnd_test COMMAND crypto_rnd_test ${test_run_options})
add_test(NAME digest_context_test COMMAND digest_context_test ${test_run_options})

if(WITH_PROPERTY_TESTS)
  include(FetchContent)
  # 'default_generator' (reflect-cpp based derivation) is not needed and
  # disabling it avoids fetching one more dependency
  set(HEGEL_REFLECTION OFF CACHE BOOL "" FORCE)
  if(WITH_ASAN)
    set(HEGEL_SANITIZE "address" CACHE STRING "" FORCE)
  endif()
  FetchContent_Declare(
    hegel
    GIT_REPOSITORY https://github.com/hegeldev/hegel-cpp.git
    GIT_TAG v0.13.1
  )
  FetchContent_MakeAvailable(hegel)

  # hegel's public API passes standard library types (std::string,
  # std::function, etc.) across the library boundary, so it must be built
  # against the same standard library as the tests
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    target_compile_options(hegel PRIVATE ${libcxx_options})
  endif()
  # marking hegel headers as system ones so that our strict warning flags
  # (including -Werror) are not applied to them
  get_target_property(hegel_include_directories hegel INTERFACE_INCLUDE_DIRECTORIES)
  set_target_properties(hegel PROPERTIES
    INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${hegel_include_directories}"
  )

  add_executable(gtid_set_property_test gtid_set_property_test.cpp)
  target_include_directories(gtid_set_property_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
  target_link_libraries(gtid_set_property_test
    PRIVATE
      binlog_server_compiler_flags
      binsrv::lib_gtids
      Boost::unit_test_framework
      hegel
  )
  set_target_properties(gtid_set_property_test PROPERTIES
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
  )

  add_executable(event_property_test event_property_test.cpp)
  target_include_directories(event_property_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
  target_link_libraries(event_property_test
    PRIVATE
      binlog_server_compiler_flags
      binsrv::lib_gtids
      binsrv::lib_events
      Boost::unit_test_framework
      ZLIB::ZLIB
      hegel
  )
  set_target_properties(event_property_test PROPERTIES
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
  )

  # sender_context (the COM_BINLOG_DUMP reader) is part of the executable's
  # operations sources, so it is compiled into the test directly
  add_executable(storage_property_test
    storage_property_test.cpp
    "${PROJECT_SOURCE_DIR}/src/operations/sender_context.cpp"
    # sender_context now builds the artificial preamble through the event
    # generators, so it needs them linked in too
    "${PROJECT_SOURCE_DIR}/src/operations/event_generation_helpers.cpp"
  )
  target_include_directories(storage_property_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
  target_link_libraries(storage_property_test
    PRIVATE
      binlog_server_compiler_flags
      binsrv::lib_binsrv
      Boost::unit_test_framework
      hegel
  )
  set_target_properties(storage_property_test PROPERTIES
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
  )

  add_executable(indexed_event_block_property_test indexed_event_block_property_test.cpp)
  target_include_directories(indexed_event_block_property_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
  target_link_libraries(indexed_event_block_property_test
    PRIVATE
      binlog_server_compiler_flags
      binsrv::lib_binsrv
      Boost::unit_test_framework
      hegel
  )
  set_target_properties(indexed_event_block_property_test PROPERTIES
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
  )

  add_executable(cipher_context_property_test cipher_context_property_test.cpp)
  target_include_directories(cipher_context_property_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
  target_link_libraries(cipher_context_property_test
    PRIVATE
      binlog_server_compiler_flags
      binsrv::lib_opensslpp
      Boost::unit_test_framework
      hegel
  )
  set_target_properties(cipher_context_property_test PROPERTIES
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
  )

  # connection_context (the MySQL protocol side of the replication source
  # listener) is part of the executable's minimysql sources, so it is
  # compiled into the test directly, together with the protocol codec headers
  add_executable(minimysql_property_test
    minimysql_property_test.cpp
    "${PROJECT_SOURCE_DIR}/src/minimysql/connection_context.cpp"
    "${PROJECT_SOURCE_DIR}/src/minimysql/caching_sha2_password_authenticator.cpp"
  )
  target_include_directories(minimysql_property_test
    PRIVATE
      "${PROJECT_SOURCE_DIR}/src"
      "${PROJECT_SOURCE_DIR}/extra/mysql_protocol"
  )
  target_link_libraries(minimysql_property_test
    PRIVATE
      binlog_server_compiler_flags
      binsrv::lib_opensslpp
      binsrv::lib_util
      Boost::headers
      MySQL::client
      Boost::unit_test_framework
      hegel
  )
  set_target_properties(minimysql_property_test PROPERTIES
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
  )

  add_executable(rewriter_property_test rewriter_property_test.cpp)
  target_include_directories(rewriter_property_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
  target_link_libraries(rewriter_property_test
    PRIVATE
      binlog_server_compiler_flags
      binsrv::lib_gtids
      binsrv::lib_events
      Boost::unit_test_framework
      ZLIB::ZLIB
      hegel
  )
  set_target_properties(rewriter_property_test PROPERTIES
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
  )

  # the event generators (operations/event_generation_helpers.cpp) are part of
  # the executable's sources, so they are compiled into the test directly
  add_executable(event_generation_property_test
    event_generation_property_test.cpp
    "${PROJECT_SOURCE_DIR}/src/operations/event_generation_helpers.cpp"
  )
  target_include_directories(event_generation_property_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
  target_link_libraries(event_generation_property_test
    PRIVATE
      binlog_server_compiler_flags
      binsrv::lib_gtids
      binsrv::lib_events
      Boost::unit_test_framework
      ZLIB::ZLIB
      hegel
  )
  set_target_properties(event_generation_property_test PROPERTIES
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
  )

  add_executable(reader_context_property_test reader_context_property_test.cpp)
  target_include_directories(reader_context_property_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
  target_link_libraries(reader_context_property_test
    PRIVATE
      binlog_server_compiler_flags
      binsrv::lib_gtids
      binsrv::lib_events
      Boost::unit_test_framework
      ZLIB::ZLIB
      hegel
  )
  set_target_properties(reader_context_property_test PROPERTIES
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
  )

  # the record-selection logic of the search operations
  # (operations/search_helpers.cpp) is part of the executable's sources, so it
  # is compiled into the test directly
  add_executable(search_helpers_property_test
    search_helpers_property_test.cpp
    "${PROJECT_SOURCE_DIR}/src/operations/search_helpers.cpp"
  )
  target_include_directories(search_helpers_property_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
  target_link_libraries(search_helpers_property_test
    PRIVATE
      binlog_server_compiler_flags
      binsrv::lib_binsrv
      Boost::unit_test_framework
      hegel
  )
  set_target_properties(search_helpers_property_test PROPERTIES
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
  )

  # sender_context and the event generators it uses are part of the
  # executable's operations sources, so they are compiled into the test
  add_executable(sender_context_property_test
    sender_context_property_test.cpp
    "${PROJECT_SOURCE_DIR}/src/operations/sender_context.cpp"
    "${PROJECT_SOURCE_DIR}/src/operations/event_generation_helpers.cpp"
  )
  target_include_directories(sender_context_property_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
  target_link_libraries(sender_context_property_test
    PRIVATE
      binlog_server_compiler_flags
      binsrv::lib_binsrv
      Boost::unit_test_framework
      hegel
  )
  set_target_properties(sender_context_property_test PROPERTIES
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
  )

  # select_purge_victim_count lives in lib_binsrv (storage_core), so the test
  # only needs to link it
  add_executable(purge_selection_property_test purge_selection_property_test.cpp)
  target_include_directories(purge_selection_property_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
  target_link_libraries(purge_selection_property_test
    PRIVATE
      binlog_server_compiler_flags
      binsrv::lib_binsrv
      Boost::unit_test_framework
      hegel
  )
  set_target_properties(purge_selection_property_test PROPERTIES
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
  )

  add_executable(config_value_property_test config_value_property_test.cpp)
  target_include_directories(config_value_property_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
  target_link_libraries(config_value_property_test
    PRIVATE
      binlog_server_compiler_flags
      binsrv::lib_binsrv
      Boost::unit_test_framework
      hegel
  )
  set_target_properties(config_value_property_test PROPERTIES
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
  )

  add_test(NAME gtid_set_property_test COMMAND gtid_set_property_test ${test_run_options})
  add_test(NAME event_property_test COMMAND event_property_test ${test_run_options})
  add_test(NAME storage_property_test COMMAND storage_property_test ${test_run_options})
  add_test(NAME indexed_event_block_property_test COMMAND indexed_event_block_property_test ${test_run_options})
  add_test(NAME cipher_context_property_test COMMAND cipher_context_property_test ${test_run_options})
  add_test(NAME minimysql_property_test COMMAND minimysql_property_test ${test_run_options})
  add_test(NAME rewriter_property_test COMMAND rewriter_property_test ${test_run_options})
  add_test(NAME event_generation_property_test COMMAND event_generation_property_test ${test_run_options})
  add_test(NAME reader_context_property_test COMMAND reader_context_property_test ${test_run_options})
  add_test(NAME search_helpers_property_test COMMAND search_helpers_property_test ${test_run_options})
  add_test(NAME sender_context_property_test COMMAND sender_context_property_test ${test_run_options})
  add_test(NAME purge_selection_property_test COMMAND purge_selection_property_test ${test_run_options})
  add_test(NAME config_value_property_test COMMAND config_value_property_test ${test_run_options})
endif()
