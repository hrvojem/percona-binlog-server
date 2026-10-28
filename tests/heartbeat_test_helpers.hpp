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

#ifndef TESTS_HEARTBEAT_TEST_HELPERS_HPP
#define TESTS_HEARTBEAT_TEST_HELPERS_HPP

// helpers shared by the property tests that check the HEARTBEAT_LOG_EVENT_V2
// events a sender_context generates while a dump is idle (PBS-30)

#include <cstddef>
#include <cstdint>
#include <exception>
#include <string>
#include <vector>

#include <boost/crc.hpp>

#include "binsrv/events/code_type.hpp"
#include "binsrv/events/common_header_flag_type.hpp"
#include "binsrv/events/common_header_view.hpp"
#include "binsrv/events/composite_binlog_name.hpp"
#include "binsrv/events/generic_body.hpp"
#include "binsrv/events/generic_post_header.hpp"
#include "binsrv/events/heartbeat_log_v2_body_impl.hpp"
#include "binsrv/events/heartbeat_log_v2_post_header_impl.hpp"
#include "binsrv/events/rotate_post_header_impl.hpp"

#include "util/byte_span.hpp"

namespace heartbeat_testing {

namespace events = binsrv::events;

inline constexpr std::size_t footer_size{4U};

// the binlog name and position a replica has reached in the stream it
// received, which a MySQL source reports in its heartbeats: set by every
// artificial ROTATE, moved past every stored event; the FORMAT_DESCRIPTION
// sent when resuming is not in the binlog and does not move it
struct replica_coordinates {
  // empty until the first artificial ROTATE
  std::string binlog;
  std::uint64_t position{0ULL};
};

// follows one received event. The first artificial ROTATE has a CRC32 footer
// when the session asked for checksums, later ones when the previous
// binlog's FORMAT_DESCRIPTION has them (the sender carries that setting
// across binlogs, as MySQL's Binlog_sender does)
inline void follow(replica_coordinates &coordinates,
                   const std::vector<std::byte> &event, bool session_checksum,
                   bool format_description_checksum) {
  using header = events::common_header_view_base;
  using rotate_post_header = events::generic_post_header<events::code_type::rotate>;
  if (std::size(event) < header::size_in_bytes) {
    return;
  }
  const util::const_byte_span span{event};
  const events::common_header_view view{
      span.subspan(0U, header::size_in_bytes)};
  // only the events the sender generates are outside the binlog: artificial
  // ROTATEs, and the FORMAT_DESCRIPTION sent when resuming, which has a zero
  // next_event_position (as MySQL sends it) but no artificial flag; a stored
  // event's flags are whatever its source wrote
  const auto type_code{view.get_type_code_raw()};
  const bool generated{
      (type_code == static_cast<std::uint8_t>(events::code_type::rotate) &&
       view.get_flags().has_element(
           events::common_header_flag_type::artificial)) ||
      (type_code ==
           static_cast<std::uint8_t>(events::code_type::format_description) &&
       view.get_next_event_position_raw() == 0U)};
  if (!generated) {
    coordinates.position += std::size(event);
    return;
  }
  if (type_code != static_cast<std::uint8_t>(events::code_type::rotate)) {
    return;
  }
  const bool with_footer{coordinates.binlog.empty()
                             ? session_checksum
                             : format_description_checksum};
  const auto name_offset{header::size_in_bytes +
                         rotate_post_header::size_in_bytes};
  const auto name_end{std::size(event) - (with_footer ? footer_size : 0U)};
  if (name_end < name_offset) {
    return;
  }
  const rotate_post_header post_header{
      span.subspan(header::size_in_bytes, rotate_post_header::size_in_bytes)};
  coordinates.binlog.clear();
  for (std::size_t index{name_offset}; index < name_end; ++index) {
    coordinates.binlog.push_back(
        static_cast<char>(std::to_integer<unsigned char>(event[index])));
  }
  coordinates.position = post_header.get_position_raw();
}

// what is wrong with 'heartbeat' as the heartbeat for a replica at
// 'coordinates', or an empty string: a HEARTBEAT_LOG_EVENT_V2 with a zero
// timestamp, no flags, the binlog's server_id, the position (truncated to 32
// bits) as next_event_position, a valid CRC32 footer exactly when
// 'with_checksum', and a body naming the binlog and the full position
[[nodiscard]] inline std::string
heartbeat_error(const std::vector<std::byte> &heartbeat,
                const replica_coordinates &coordinates,
                std::uint32_t server_id, bool with_checksum) {
  using header = events::common_header_view_base;
  using heartbeat_post_header =
      events::generic_post_header<events::code_type::heartbeat_log_v2>;
  using heartbeat_body =
      events::generic_body_impl<events::code_type::heartbeat_log_v2>;
  const auto expected{coordinates.binlog + ":" +
                      std::to_string(coordinates.position)};
  const auto footer{with_checksum ? footer_size : 0U};
  const auto body_offset{header::size_in_bytes +
                         heartbeat_post_header::size_in_bytes};
  if (std::size(heartbeat) < body_offset + footer) {
    return "the heartbeat for " + expected + " has only " +
           std::to_string(std::size(heartbeat)) + " byte(s)";
  }
  const util::const_byte_span span{heartbeat};
  const events::common_header_view view{
      span.subspan(0U, header::size_in_bytes)};
  if (view.get_type_code_raw() !=
      static_cast<std::uint8_t>(events::code_type::heartbeat_log_v2)) {
    return "the heartbeat for " + expected + " has type code " +
           std::to_string(view.get_type_code_raw());
  }
  if (view.get_timestamp_raw() != 0U || view.get_flags_raw() != 0U) {
    return "the heartbeat for " + expected +
           " has a non-zero timestamp or flags";
  }
  if (view.get_server_id_raw() != server_id) {
    return "the heartbeat for " + expected + " has server_id " +
           std::to_string(view.get_server_id_raw()) + ", expected " +
           std::to_string(server_id);
  }
  if (view.get_event_size_raw() != std::size(heartbeat)) {
    return "the heartbeat for " + expected + " has event size " +
           std::to_string(view.get_event_size_raw()) + " but " +
           std::to_string(std::size(heartbeat)) + " byte(s)";
  }
  if (view.get_next_event_position_raw() !=
      static_cast<std::uint32_t>(coordinates.position)) {
    return "the heartbeat for " + expected + " has next_event_position " +
           std::to_string(view.get_next_event_position_raw());
  }
  const auto body_end{std::size(heartbeat) - footer};
  if (with_checksum) {
    boost::crc_32_type crc;
    crc.process_bytes(std::data(heartbeat), body_end);
    std::uint32_t stored{0U};
    for (std::size_t index{0U}; index < footer_size; ++index) {
      stored |= static_cast<std::uint32_t>(
                    std::to_integer<unsigned char>(heartbeat[body_end + index]))
                << (8U * index);
    }
    if (stored != crc.checksum()) {
      return "the heartbeat for " + expected + " has a wrong CRC32 footer";
    }
  }
  try {
    const auto body_span{span.subspan(body_offset, body_end - body_offset)};
    const heartbeat_body body{body_span};
    if (body.calculate_encoded_size() != std::size(body_span)) {
      return "the heartbeat body for " + expected + " has trailing bytes";
    }
    if (body.get_readable_binlog() != coordinates.binlog ||
        body.get_position() != coordinates.position) {
      return "the heartbeat reports " + std::string{body.get_readable_binlog()} +
             ":" + std::to_string(body.get_position()) + " where the replica is at " +
             expected;
    }
  } catch (const std::exception &e) {
    return "the heartbeat body for " + expected + " does not parse: " + e.what();
  }
  return {};
}

} // namespace heartbeat_testing

#endif // TESTS_HEARTBEAT_TEST_HELPERS_HPP
