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

// libFuzzer target: a binlog event stream as 'fetch' / 'pull' receive it
// from a source. The first byte picks the connection (replication mode,
// checksum verification, server version); the rest is split into events by
// the event_size field of each common header, as the network delivers whole
// events. Each event goes through what collector_context does with it: an
// event_view against the reader_context, printed as at debug log level, then
// reader_context::process_event_view(), then the materialized event, printed.
// A rejection is a std::exception, on which the collector stops cleanly; a
// crash, a std::terminate or a sanitizer report is a bug.
//
// Known bug filter (known_bug_filters.hpp): a FORMAT_DESCRIPTION whose server
// version is not major.minor.patch ends the process (ticket 1 / PBS-68); left
// in, it ended nearly every run, so such an event ends the stream like the
// clean rejection the fix should make it (-DFUZZ_KEEP_PBS_68 keeps the
// crash).

#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <sstream>

#include "known_bug_filters.hpp"

#include "binsrv/replication_mode_type.hpp"

#include "binsrv/events/common_header_view.hpp"
#include "binsrv/events/event.hpp"
#include "binsrv/events/event_view.hpp"
#include "binsrv/events/reader_context.hpp"

#include "util/byte_span.hpp"
#include "util/semantic_version.hpp"

namespace {

// the server versions of the MySQL series the Binlog Server supports
constexpr std::array<util::semantic_version, 3> server_versions{
    util::semantic_version{8U, 0U, 45U}, util::semantic_version{8U, 4U, 8U},
    util::semantic_version{9U, 7U, 2U}};

[[nodiscard]] std::size_t next_event_size(util::const_byte_span rest) {
  using header = binsrv::events::common_header_view_base;
  if (std::size(rest) < header::size_in_bytes) {
    return std::size(rest);
  }
  std::uint32_t size{0U};
  for (std::size_t index{0U}; index < sizeof size; ++index) {
    size |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(
                rest[header::event_size_offset + index]))
            << (8U * index);
  }
  return size >= header::size_in_bytes && size <= std::size(rest)
             ? size
             : std::size(rest);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data,
                                      std::size_t size) {
  if (size == 0U) {
    return 0;
  }
  const auto selector{data[0]};
  const bool gtid_mode{(selector & 1U) != 0U};
  const bool verify_checksum{(selector & 2U) != 0U};
  const auto &server_version{
      server_versions[(selector >> 2U) % std::size(server_versions)]};

  util::const_byte_span rest{
      reinterpret_cast<const std::byte *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
          std::next(data)),
      size - 1U};
  try {
    binsrv::events::reader_context context{
        server_version.get_encoded(), verify_checksum,
        gtid_mode ? binsrv::replication_mode_type::gtid
                  : binsrv::replication_mode_type::position,
        "", 0U};
    while (!rest.empty()) {
      const auto event_size{next_event_size(rest)};
      const auto portion{rest.subspan(0U, event_size)};
      rest = rest.subspan(event_size);
#ifndef FUZZ_KEEP_PBS_68
      if (fuzz_filters::has_unparsable_server_version(portion)) {
        break;
      }
#endif

      const binsrv::events::event_view event_v{context, portion};
      std::ostringstream printed;
      printed << event_v;
      [[maybe_unused]] const bool info_only{
          context.process_event_view(event_v)};
      const binsrv::events::event event{event_v};
      printed << event;
    }
  } catch (const std::exception &) {
    // a clean rejection
  }
  return 0;
}
