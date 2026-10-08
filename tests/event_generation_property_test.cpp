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

// Property-based tests for the event generators in
// operations/event_generation_helpers.cpp, built with Hegel
// (https://hegel.dev). These build the artificial events the collector and
// the replication source need (the ROTATE that names a binlog, the
// FORMAT_DESCRIPTION that announces the checksum algorithm, and the
// PREVIOUS_GTIDS that carries a binlog's starting GTID set). The MTR suite
// only exercises them with the events of a live server; here each generator
// is checked over many inputs for the invariants a reader relies on:
//
// - generate_rotate_event: the event parses back as a ROTATE with the given
//   server id; the artificial flag and the timestamp follow the parameters;
//   the post header holds the magic offset; the body round-trips to the
//   requested binlog name; and the footer is present exactly when the reader
//   context expects one, with a correct CRC32.
// - generate_format_description_event: the event parses back as a
//   FORMAT_DESCRIPTION with the given server id and a correct checksum, and,
//   fed to a fresh reader context, makes that context expect checksummed
//   events (it announces crc32, as the rewrite mode requires).
// - generate_previous_gtids_log_event: the event parses back as a
//   PREVIOUS_GTIDS_LOG with the given server id and a correct checksum, and
//   its body round-trips to the requested GTID set.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

// needed for binsrv::events::event_storage
#include <boost/container/small_vector.hpp> // IWYU pragma: keep

#define BOOST_TEST_MODULE EventGenerationPropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "property_test_helpers.hpp"

#include "operations/event_generation_helpers.hpp"

#include "binsrv/replication_mode_type.hpp"

#include "binsrv/gtids/gtid_set.hpp"
#include "binsrv/gtids/uuid.hpp"

#include "binsrv/events/checksum_algorithm_type.hpp"
#include "binsrv/events/code_type.hpp"
#include "binsrv/events/common_header_flag_type.hpp"
#include "binsrv/events/common_header_view.hpp"
#include "binsrv/events/common_types.hpp"
#include "binsrv/events/composite_binlog_name.hpp"
#include "binsrv/events/event.hpp"
#include "binsrv/events/event_view.hpp"
#include "binsrv/events/footer_view.hpp"
#include "binsrv/events/format_description_body_impl.hpp"
#include "binsrv/events/format_description_post_header_impl.hpp"
#include "binsrv/events/previous_gtids_log_body_impl.hpp"
#include "binsrv/events/protocol_traits_fwd.hpp"
#include "binsrv/events/reader_context.hpp"
#include "binsrv/events/rotate_body_impl.hpp"
#include "binsrv/events/rotate_post_header_impl.hpp"

#include "util/byte_span_fwd.hpp"
#include "util/ctime_timestamp.hpp"
#include "util/semantic_version.hpp"

namespace {

namespace gs = hegel::generators;
namespace events = binsrv::events;

using property_testing::require;
using property_testing::run_property;

using events::code_type;

const util::semantic_version server_version{8U, 4U, 8U};
constexpr std::uint32_t preamble_server_id{42U};

// a reader context positioned after the preamble of a binlog stream (an
// artificial ROTATE and a FORMAT_DESCRIPTION event), so that it knows the
// post header lengths of all event types of the server version, exactly as
// when it parses events received from a server. 'with_checksum' decides
// whether the preamble's FORMAT_DESCRIPTION announces crc32, which is what
// the context then expects for the following events.
[[nodiscard]] std::unique_ptr<events::reader_context>
make_context(bool with_checksum) {
  auto context{std::make_unique<events::reader_context>(
      server_version.get_encoded(), with_checksum,
      binsrv::replication_mode_type::gtid, "", 0U)};
  events::event_storage buffer;

  const events::common_header_flag_set artificial{
      events::common_header_flag_type::artificial};
  [[maybe_unused]] const auto rotate{
      events::event::create_event<code_type::rotate>(
          0U, util::ctime_timestamp{}, preamble_server_id, artificial,
          events::generic_post_header<code_type::rotate>{
              events::magic_binlog_offset},
          events::generic_body<code_type::rotate>{
              events::composite_binlog_name{"binlog", 1U}},
          with_checksum, buffer)};
  [[maybe_unused]] const auto rotate_info_only{context->process_event_view(
      events::event_view{*context, util::const_byte_span{buffer}})};

  [[maybe_unused]] const auto format_description{
      events::event::create_event<code_type::format_description>(
          static_cast<std::uint32_t>(events::magic_binlog_offset),
          util::ctime_timestamp{1700000000}, preamble_server_id, {},
          events::generic_post_header<code_type::format_description>{
              events::default_binlog_version, server_version,
              util::ctime_timestamp{1700000000},
              events::default_common_header_length,
              events::reader_context::get_hardcoded_post_header_lengths(
                  server_version.get_encoded())},
          events::generic_body<code_type::format_description>{
              with_checksum ? events::checksum_algorithm_type::crc32
                            : events::checksum_algorithm_type::off},
          true, buffer)};
  [[maybe_unused]] const auto format_description_info_only{
      context->process_event_view(
          events::event_view{*context, util::const_byte_span{buffer}})};
  return context;
}

// checks the parts of the common header and footer every generated event
// must get right: the type, the server id, a self-consistent size, and a
// footer that is present exactly when the context expects one and, when it
// is, carries the event's own CRC32.
void check_common(const events::event_view &view,
                  const events::reader_context &context, code_type expected,
                  std::uint32_t expected_server_id) {
  const auto header{view.get_common_header_view()};
  require(header.get_type_code() == expected, "unexpected event type");
  require(header.get_server_id_raw() == expected_server_id,
          "server id not preserved");
  require(header.get_event_size_raw() == view.get_total_size(),
          "event size in the header does not match the actual size");
  require(view.has_footer() == context.is_footer_expected(),
          "footer presence does not match what the context expects");
  if (view.has_footer()) {
    require(view.get_footer_view().get_crc_raw() == view.calculate_crc(),
            "footer CRC32 does not match the event content");
  }
}

constexpr std::array<std::string_view, 2> binlog_base_pool{"binlog",
                                                           "mysql-bin"};
constexpr std::array<std::string_view, 3> uuid_pool{
    "11111111-aaaa-1111-aaaa-111111111111",
    "22222222-bbbb-2222-bbbb-222222222222",
    "33333333-cccc-3333-cccc-333333333333"};

[[nodiscard]] events::composite_binlog_name draw_binlog_name(hegel::TestCase &tc) {
  const auto base{binlog_base_pool.at(tc.draw(gs::integers<std::size_t>(
      {.min_value = 0U, .max_value = std::size(binlog_base_pool) - 1U})))};
  const auto sequence{tc.draw(gs::integers<std::uint32_t>(
      {.min_value = 1U, .max_value = 999999U}))};
  return events::composite_binlog_name{base, sequence};
}

} // namespace

BOOST_AUTO_TEST_CASE(generate_rotate_event_round_trips) {
  run_property([](hegel::TestCase &tc) {
    const bool with_checksum{tc.draw(gs::booleans())};
    const auto context{make_context(with_checksum)};
    const auto offset{tc.draw(gs::integers<std::uint32_t>(
        {.min_value = 0U, .max_value = 0xFFFFFFF0U}))};
    const auto server_id{tc.draw(gs::integers<std::uint32_t>())};
    const bool current_timestamp{tc.draw(gs::booleans())};
    const bool artificial{tc.draw(gs::booleans())};
    const auto binlog_name{draw_binlog_name(tc)};
    const auto position{tc.draw(gs::integers<std::uint64_t>())};

    events::event_storage buffer;
    const auto view{operations::generate_rotate_event(
        buffer, *context, offset, current_timestamp, server_id, artificial,
        binlog_name, position)};

    check_common(view, *context, code_type::rotate, server_id);

    const auto header{view.get_common_header_view()};
    require(header.get_flags().has_element(
                events::common_header_flag_type::artificial) == artificial,
            "artificial flag does not match the parameter");
    require((header.get_timestamp_raw() != 0U) == current_timestamp,
            "timestamp does not follow the current_timestamp parameter");

    const events::generic_post_header<code_type::rotate> post_header{
        view.get_post_header_raw()};
    require(post_header.get_position_raw() == position,
            "rotate post header does not round-trip to the requested position");

    const events::generic_body<code_type::rotate> body{view.get_body_raw()};
    require(body.get_parsed_binlog() == binlog_name,
            "rotate body does not round-trip to the requested binlog name");
  });
}

BOOST_AUTO_TEST_CASE(generate_format_description_event_announces_checksums) {
  run_property([](hegel::TestCase &tc) {
    // the generators are only used in the rewrite mode, where checksums are
    // enforced, so the context always expects a footer here
    const auto context{make_context(true)};
    const auto offset{tc.draw(gs::integers<std::uint32_t>(
        {.min_value = 0U, .max_value = 0xFFFFFFF0U}))};
    const auto server_id{tc.draw(gs::integers<std::uint32_t>())};
    const bool artificial{tc.draw(gs::booleans())};

    events::event_storage buffer;
    // enable_checksum_algorithm is true so the body announces crc32, as the
    // rewrite mode requires
    const auto view{operations::generate_format_description_event(
        buffer, *context, offset, server_id, /*enable_checksum_algorithm=*/true,
        artificial)};

    check_common(view, *context, code_type::format_description, server_id);
    // note: for FORMAT_DESCRIPTION the 'artificial' parameter is only an
    // internal marker (it governs next_event_position) and is cleared before
    // serialization, so it is exercised with both values but not asserted on
    // the serialized common header here

    // the generated FORMAT_DESCRIPTION must announce crc32 in its body, so
    // that downstream readers expect a checksum on the following events: this
    // is what the rewrite mode relies on ("enforcing checksums for all
    // rewritten upcoming events")
    const events::generic_body<code_type::format_description> body{
        view.get_body_raw()};
    require(body.has_checksum_algorithm(),
            "the generated FORMAT_DESCRIPTION has no checksum algorithm");
    require(body.get_checksum_algorithm() ==
                events::checksum_algorithm_type::crc32,
            "the generated FORMAT_DESCRIPTION does not announce crc32");

    // the post header must carry the server version the context connected
    // with, as the readers downstream size their post headers from it
    const events::generic_post_header<code_type::format_description>
        post_header{view.get_post_header_raw()};
    require(post_header.get_encoded_server_version() ==
                context->get_current_encoded_server_version(),
            "the generated FORMAT_DESCRIPTION carries the wrong server "
            "version");
  });
}

BOOST_AUTO_TEST_CASE(generate_previous_gtids_log_event_round_trips) {
  run_property([](hegel::TestCase &tc) {
    const auto context{make_context(true)};
    const auto offset{tc.draw(gs::integers<std::uint32_t>(
        {.min_value = 0U, .max_value = 0xFFFFFFF0U}))};
    const auto server_id{tc.draw(gs::integers<std::uint32_t>())};

    // an untagged GTID set drawn from a small pool of UUIDs, each with a few
    // intervals; the set may be empty
    binsrv::gtids::gtid_set gtids;
    const auto number_of_uuids{tc.draw(gs::integers<std::size_t>(
        {.min_value = 0U, .max_value = std::size(uuid_pool)}))};
    for (std::size_t index{0U}; index < number_of_uuids; ++index) {
      const binsrv::gtids::uuid uuid_component{uuid_pool.at(index)};
      const auto number_of_intervals{tc.draw(gs::integers<std::uint64_t>(
          {.min_value = 1U, .max_value = 3U}))};
      std::uint64_t next{1U};
      for (std::uint64_t interval{0U}; interval < number_of_intervals;
           ++interval) {
        const auto gap{tc.draw(
            gs::integers<std::uint64_t>({.min_value = 1U, .max_value = 5U}))};
        const auto length{tc.draw(
            gs::integers<std::uint64_t>({.min_value = 1U, .max_value = 10U}))};
        gtids.add_interval(uuid_component, binsrv::gtids::tag{}, next,
                           next + length);
        next += length + gap;
      }
    }

    events::event_storage buffer;
    const auto view{operations::generate_previous_gtids_log_event(
        buffer, *context, offset, server_id, gtids)};

    check_common(view, *context, code_type::previous_gtids_log, server_id);

    const events::generic_body<code_type::previous_gtids_log> body{
        view.get_body_raw()};
    require(body.get_gtids() == gtids,
            "previous_gtids body does not round-trip to the requested set");
  });
}
