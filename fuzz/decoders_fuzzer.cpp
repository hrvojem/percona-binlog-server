// Copyright (c) 2026 Percona and/or its affiliates.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// libFuzzer target: the small decoders of text and bytes that come from
// users or the network. The first byte picks one, the rest is its input:
//
//   0  a GTID set in text form (search_by_gtid_set, storage metadata)
//   1  a GTID set in binary form (PREVIOUS_GTIDS_LOG bodies)
//   2  a binlog name (COM_BINLOG_DUMP, binlog.index)
//   3  a server version (FORMAT_DESCRIPTION, the source's handshake)
//   4  a timestamp (search_by_timestamp)
//   5  a size with a unit (storage.checkpoint_size)
//   6  a time with a unit (storage.checkpoint_interval)
//   7  a HEARTBEAT_LOG_EVENT_V2 body (from a source)
//
// A rejection is a std::exception (try_parse: false). What is accepted is
// printed, and for the text forms that the code itself writes back (GTID set,
// binlog name, server version) printing and parsing again must give the same
// value; a mismatch aborts, so libFuzzer reports it. The binary GTID set is
// only printed: a decoded set that cannot be printed and parsed back is
// ticket 3 (known).

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <sstream>
#include <string>
#include <string_view>

#include "binsrv/size_unit.hpp"
#include "binsrv/time_unit.hpp"

#include "binsrv/events/code_type.hpp"
#include "binsrv/events/composite_binlog_name.hpp"
#include "binsrv/events/heartbeat_log_v2_body_impl.hpp"
#include "binsrv/gtids/gtid_set.hpp"

#include "util/byte_span.hpp"
#include "util/ctime_timestamp.hpp"
#include "util/semantic_version.hpp"

namespace {

[[noreturn]] void round_trip_failed(std::string_view what,
                                    std::string_view input,
                                    std::string_view printed) {
  std::fprintf(stderr, "%.*s does not round-trip: input '%.*s', printed '%.*s'\n",
               static_cast<int>(std::size(what)), std::data(what),
               static_cast<int>(std::size(input)), std::data(input),
               static_cast<int>(std::size(printed)), std::data(printed));
  std::abort();
}

void decode(std::uint8_t which, util::const_byte_span bytes) {
  const std::string_view text{
      reinterpret_cast<const char *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
          std::data(bytes)),
      std::size(bytes)};
  switch (which % 8U) {
  case 0: {
    const binsrv::gtids::gtid_set parsed{text};
    const auto printed{parsed.str()};
    if (binsrv::gtids::gtid_set{printed} != parsed) {
      round_trip_failed("a GTID set", text, printed);
    }
  } break;
  case 1: {
    const binsrv::gtids::gtid_set decoded{bytes};
    [[maybe_unused]] const auto printed{decoded.str()};
  } break;
  case 2: {
    const auto parsed{binsrv::events::composite_binlog_name::parse(text)};
    const auto printed{parsed.str()};
    if (binsrv::events::composite_binlog_name::parse(printed) != parsed) {
      round_trip_failed("a binlog name", text, printed);
    }
  } break;
  case 3: {
    const util::semantic_version parsed{text};
    const auto printed{parsed.get_string()};
    if (util::semantic_version{printed}.get_encoded() != parsed.get_encoded()) {
      round_trip_failed("a server version", text, printed);
    }
  } break;
  case 4: {
    util::ctime_timestamp timestamp{};
    if (util::ctime_timestamp::try_parse(text, timestamp)) {
      std::ostringstream printed;
      printed << timestamp;
    }
  } break;
  case 5: {
    const binsrv::size_unit parsed{text};
    [[maybe_unused]] const auto value{parsed.get_value()};
    [[maybe_unused]] const auto description{parsed.get_description()};
  } break;
  case 6: {
    const binsrv::time_unit parsed{text};
    [[maybe_unused]] const auto value{parsed.get_value()};
    [[maybe_unused]] const auto description{parsed.get_description()};
  } break;
  default: {
    const binsrv::events::generic_body_impl<
        binsrv::events::code_type::heartbeat_log_v2>
        body{bytes};
    std::ostringstream printed;
    printed << body;
  } break;
  }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data,
                                      std::size_t size) {
  if (size == 0U) {
    return 0;
  }
  try {
    decode(data[0],
           util::const_byte_span{
               reinterpret_cast<const std::byte *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                   std::next(data)),
               size - 1U});
  } catch (const std::exception &) {
    // a clean rejection
  }
  return 0;
}
