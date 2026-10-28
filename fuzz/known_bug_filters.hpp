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

#ifndef FUZZ_KNOWN_BUG_FILTERS_HPP
#define FUZZ_KNOWN_BUG_FILTERS_HPP

// Known bug filters of the libFuzzer targets: a known crash that almost any
// input reaches would end nearly every run and hide everything else, so the
// targets treat the inputs that reach it as the clean rejection the fix
// should make them. Each filter names its ticket and can be switched off with
// -DFUZZ_KEEP_<ticket>; remove it once the bug is fixed.

#include <cstddef>
#include <cstdint>
#include <exception>
#include <string_view>

#include "binsrv/events/code_type.hpp"
#include "binsrv/events/common_header_view.hpp"

#include "util/byte_span.hpp"
#include "util/semantic_version.hpp"

namespace fuzz_filters {

// PBS-68 (ticket 1): a FORMAT_DESCRIPTION whose server version is not
// major.minor.patch ends the process (std::terminate from a noexcept
// accessor). Whether 'event' is one: the 50-byte field after the 2-byte
// binlog version, cut at its first NUL, as util::semantic_version reads it.
[[nodiscard]] inline bool
has_unparsable_server_version(util::const_byte_span event) {
  using header = binsrv::events::common_header_view_base;
  static constexpr std::size_t binlog_version_size{2U};
  static constexpr std::size_t server_version_size{50U};
  if (std::size(event) < header::size_in_bytes + binlog_version_size +
                             server_version_size ||
      std::to_integer<std::uint8_t>(event[header::type_code_offset]) !=
          static_cast<std::uint8_t>(
              binsrv::events::code_type::format_description)) {
    return false;
  }
  std::string_view version{
      reinterpret_cast<const char *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
          std::data(event) + header::size_in_bytes + binlog_version_size),
      server_version_size};
  version = version.substr(0U, version.find('\0'));
  try {
    [[maybe_unused]] const util::semantic_version parsed{version};
  } catch (const std::exception &) {
    return true;
  }
  return false;
}

// PBS-112 (ticket 39): an event header with an invalid type code where the
// pull sender expects one (a dump position inside an event, or corrupted
// stored data) ends the process (std::terminate from a noexcept
// indexed_event_block accessor). Whether 'header_bytes' (at least a common
// header) is such a header.
[[nodiscard]] inline bool
has_invalid_type_code(util::const_byte_span header_bytes) {
  using header = binsrv::events::common_header_view_base;
  try {
    [[maybe_unused]] const binsrv::events::common_header_view view{
        header_bytes.subspan(0U, header::size_in_bytes)};
  } catch (const std::exception &) {
    return true;
  }
  return false;
}

} // namespace fuzz_filters

#endif // FUZZ_KNOWN_BUG_FILTERS_HPP
