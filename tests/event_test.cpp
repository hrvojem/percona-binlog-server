// Copyright (c) 2023-2024 Percona and/or its affiliates.
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

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#define BOOST_TEST_MODULE EventTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <boost/test/tools/old/interface.hpp>

#include "binsrv/replication_mode_type.hpp"

#include "binsrv/gtids/common_types.hpp"
#include "binsrv/gtids/gtid_set.hpp"

#include "binsrv/events/checksum_algorithm_type.hpp"
#include "binsrv/events/code_type.hpp"
#include "binsrv/events/common_header.hpp"
#include "binsrv/events/common_header_flag_type.hpp"
#include "binsrv/events/common_types.hpp"
#include "binsrv/events/composite_binlog_name.hpp"
#include "binsrv/events/event.hpp"
#include "binsrv/events/event_view.hpp"
#include "binsrv/events/format_description_body_impl.hpp"
#include "binsrv/events/gtid_tagged_log_body_impl.hpp"
#include "binsrv/events/gtid_log_flag_type.hpp"
#include "binsrv/events/gtid_log_post_header.hpp"
#include "binsrv/events/protocol_traits.hpp"
#include "binsrv/events/protocol_traits_fwd.hpp"
#include "binsrv/events/reader_context.hpp"
#include "binsrv/events/rewriter.hpp"
#include "binsrv/events/rotate_post_header_impl.hpp"

#include "util/byte_span.hpp"
#include "util/byte_span_extractors.hpp"
#include "util/byte_span_fwd.hpp"
#include "util/byte_span_inserters.hpp"
#include "util/ctime_timestamp.hpp"
#include "util/timestamp_helpers.hpp"
#include "util/semantic_version.hpp"

static constexpr std::uint32_t default_server_id{42U};

BOOST_AUTO_TEST_CASE(EventRoundTrip) {
  const util::semantic_version server_version{"8.4.8"};
  std::uint32_t offset{0U};
  binsrv::events::reader_context context{server_version.get_encoded(), true,
                                         binsrv::replication_mode_type::gtid,
                                         "", offset};

  binsrv::events::event_storage event_buffer;

  // artificial ROTATE event
  offset = 0U;
  const binsrv::events::common_header_flag_set flags{
      binsrv::events::common_header_flag_type::artificial};

  const binsrv::events::generic_post_header<binsrv::events::code_type::rotate>
      rotate_post_header{binsrv::events::magic_binlog_offset};
  const binsrv::events::composite_binlog_name binlog_name{"binlog", 1U};
  const binsrv::events::generic_body<binsrv::events::code_type::rotate>
      rotate_body{binlog_name};

  const auto generated_rotate_event{
      binsrv::events::event::create_event<binsrv::events::code_type::rotate>(
          offset,
          // artificial ROTATE event must include zero timestamp
          util::ctime_timestamp{}, default_server_id, flags, rotate_post_header,
          rotate_body, true, event_buffer)};

  const binsrv::events::event_view generated_rotate_event_v{
      context, util::const_byte_span{event_buffer}};
  const binsrv::events::event parsed_rotate_event{generated_rotate_event_v};
  bool info_only{false};
  info_only = context.process_event_view(generated_rotate_event_v);
  BOOST_CHECK(info_only);

  BOOST_CHECK_EQUAL(generated_rotate_event, parsed_rotate_event);

  // FORMAT_DESCRIPTION event
  offset = binsrv::events::magic_binlog_offset;
  const binsrv::events::generic_post_header<
      binsrv::events::code_type::format_description>
      format_description_post_header{
          binsrv::events::default_binlog_version, server_version,
          util::ctime_timestamp::now(),
          binsrv::events::default_common_header_length,
          binsrv::events::reader_context::get_hardcoded_post_header_lengths(
              server_version.get_encoded())};
  const binsrv::events::generic_body<
      binsrv::events::code_type::format_description>
      format_description_body{binsrv::events::checksum_algorithm_type::crc32};

  const auto generated_format_description_event{
      binsrv::events::event::create_event<
          binsrv::events::code_type::format_description>(
          offset, util::ctime_timestamp::now(), default_server_id,
          binsrv::events::common_header_flag_set{},
          format_description_post_header, format_description_body, true,
          event_buffer)};

  const binsrv::events::event_view generated_format_description_event_v{
      context, util::const_byte_span{event_buffer}};
  const binsrv::events::event parsed_format_description_event{
      generated_format_description_event_v};
  info_only = context.process_event_view(generated_format_description_event_v);
  BOOST_CHECK(!info_only);

  BOOST_CHECK_EQUAL(generated_format_description_event,
                    parsed_format_description_event);

  // PREVIOUS_GTIDS_LOG event
  offset = generated_format_description_event.get_common_header()
               .get_next_event_position_raw();
  const binsrv::events::generic_post_header<
      binsrv::events::code_type::previous_gtids_log>
      previous_gtids_log_post_header{};
  const binsrv::events::generic_body<
      binsrv::events::code_type::previous_gtids_log>
      previous_gtids_log_body{binsrv::gtids::gtid_set{
          "11111111-aaaa-1111-aaaa-111111111111:1:3-5"}};
  const auto generated_previous_gtids_log_event{
      binsrv::events::event::create_event<
          binsrv::events::code_type::previous_gtids_log>(
          offset, util::ctime_timestamp::now(), default_server_id,
          binsrv::events::common_header_flag_set{},
          previous_gtids_log_post_header, previous_gtids_log_body, true,
          event_buffer)};

  const binsrv::events::event_view generated_previous_gtids_log_event_v{
      context, util::const_byte_span{event_buffer}};
  const binsrv::events::event parsed_previous_gtids_log_event{
      generated_previous_gtids_log_event_v};
  info_only = context.process_event_view(generated_previous_gtids_log_event_v);
  BOOST_CHECK(!info_only);

  BOOST_CHECK_EQUAL(generated_previous_gtids_log_event,
                    parsed_previous_gtids_log_event);

  const auto earlier_ts{std::chrono::high_resolution_clock::now()};
  const auto later_ts{earlier_ts + std::chrono::hours(1U)};

  // GTID_LOG event
  offset = generated_previous_gtids_log_event.get_common_header()
               .get_next_event_position_raw();
  const binsrv::events::generic_post_header<binsrv::events::code_type::gtid_log>
      gtid_log_post_header{
          binsrv::events::gtid_log_flag_set{
              binsrv::events::gtid_log_flag_type::may_have_sbr},       // flags
          binsrv::gtids::uuid{"11111111-aaaa-1111-aaaa-111111111111"}, // uuid
          binsrv::gtids::gno_t{6ULL},                                  // gno
          binsrv::events::gtid_log_post_header::
              known_logical_ts_code,      // logical_ts_code
          binsrv::events::seq_no_t{0ULL}, // last_committed
          binsrv::events::seq_no_t{1ULL}  // sequence_number
      };
  const binsrv::events::generic_body<binsrv::events::code_type::gtid_log>
      gtid_log_body{
          later_ts,                         // immediate_commit_timestamp
          earlier_ts,                       // original_commit_timestamp
          42ULL,                            // transaction_length
          util::semantic_version{"8.4.8"},  // immediate_server_version
          util::semantic_version{"8.0.46"}, // original_server_version
          123ULL                            // commit_group_ticket
      };
  const auto generated_gtid_log_event{
      binsrv::events::event::create_event<binsrv::events::code_type::gtid_log>(
          offset, util::ctime_timestamp::now(), default_server_id,
          binsrv::events::common_header_flag_set{}, gtid_log_post_header,
          gtid_log_body, true, event_buffer)};

  const binsrv::events::event_view generated_gtid_log_event_v{
      context, util::const_byte_span{event_buffer}};
  const binsrv::events::event parsed_gtid_log_event{generated_gtid_log_event_v};

  BOOST_CHECK_EQUAL(generated_gtid_log_event, parsed_gtid_log_event);

  // ANONYMOUS_GTID_LOG event
  offset = generated_previous_gtids_log_event.get_common_header()
               .get_next_event_position_raw();
  const binsrv::events::generic_post_header<
      binsrv::events::code_type::anonymous_gtid_log>
      anonymous_gtid_log_post_header{
          binsrv::events::gtid_log_flag_set{
              binsrv::events::gtid_log_flag_type::may_have_sbr}, // flags
          binsrv::gtids::uuid{},                                 // uuid
          binsrv::gtids::gno_t{0ULL},                            // gno
          binsrv::events::gtid_log_post_header::
              known_logical_ts_code,      // logical_ts_code
          binsrv::events::seq_no_t{0ULL}, // last_committed
          binsrv::events::seq_no_t{1ULL}  // sequence_number
      };
  const binsrv::events::generic_body<
      binsrv::events::code_type::anonymous_gtid_log>
      anonymous_gtid_log_body{
          later_ts,                         // immediate_commit_timestamp
          earlier_ts,                       // original_commit_timestamp
          42ULL,                            // transaction_length
          util::semantic_version{"8.4.8"},  // immediate_server_version
          util::semantic_version{"8.0.46"}, // original_server_version
          123ULL                            // commit_group_ticket
      };
  const auto generated_anonymous_gtid_log_event{
      binsrv::events::event::create_event<
          binsrv::events::code_type::anonymous_gtid_log>(
          offset, util::ctime_timestamp::now(), default_server_id,
          binsrv::events::common_header_flag_set{},
          anonymous_gtid_log_post_header, anonymous_gtid_log_body, true,
          event_buffer)};

  const binsrv::events::event_view generated_anonymous_gtid_log_event_v{
      context, util::const_byte_span{event_buffer}};
  const binsrv::events::event parsed_anonymous_gtid_log_event{
      generated_anonymous_gtid_log_event_v};

  BOOST_CHECK_EQUAL(generated_anonymous_gtid_log_event,
                    parsed_anonymous_gtid_log_event);

  // GTID_TAGGED_LOG event
  offset = generated_previous_gtids_log_event.get_common_header()
               .get_next_event_position_raw();
  const binsrv::events::generic_post_header<
      binsrv::events::code_type::gtid_tagged_log>
      gtid_tagged_log_post_header{};
  const binsrv::events::generic_body<binsrv::events::code_type::gtid_tagged_log>
      gtid_tagged_log_body{
          binsrv::events::gtid_log_flag_set{
              binsrv::events::gtid_log_flag_type::may_have_sbr},       // flags
          binsrv::gtids::uuid{"11111111-aaaa-1111-aaaa-111111111111"}, // uuid
          binsrv::gtids::gno_t{6ULL},                                  // gno
          binsrv::gtids::tag{"mytag"},                                 // tag
          binsrv::events::seq_no_t{0ULL},   // last_committed
          binsrv::events::seq_no_t{1ULL},   // sequence_number
          later_ts,                         // immediate_commit_timestamp
          earlier_ts,                       // original_commit_timestamp
          42ULL,                            // transaction_length
          util::semantic_version{"8.4.8"},  // immediate_server_version
          util::semantic_version{"8.0.46"}, // original_server_version
          123ULL                            // commit_group_ticket
      };
  const auto generated_gtid_tagged_log_event{
      binsrv::events::event::create_event<
          binsrv::events::code_type::gtid_tagged_log>(
          offset, util::ctime_timestamp::now(), default_server_id,
          binsrv::events::common_header_flag_set{}, gtid_tagged_log_post_header,
          gtid_tagged_log_body, true, event_buffer)};

  const binsrv::events::event_view generated_gtid_tagged_log_event_v{
      context, util::const_byte_span{event_buffer}};
  const binsrv::events::event parsed_gtid_tagged_log_event{
      generated_gtid_tagged_log_event_v};

  BOOST_CHECK_EQUAL(generated_gtid_tagged_log_event,
                    parsed_gtid_tagged_log_event);
}

BOOST_AUTO_TEST_CASE(EventMaterialization) {
  const util::semantic_version server_version{"8.4.8"};
  std::uint32_t offset{0U};
  const binsrv::events::reader_context context_with_checksum{
      server_version.get_encoded(), true, binsrv::replication_mode_type::gtid,
      "", offset};
  const binsrv::events::reader_context context_wo_checksum{
      server_version.get_encoded(), false, binsrv::replication_mode_type::gtid,
      "", offset};

  binsrv::events::event_storage event_with_footer_buffer;

  // artificial ROTATE event
  offset = 0U;
  const binsrv::events::common_header_flag_set flags{
      binsrv::events::common_header_flag_type::artificial};

  const binsrv::events::generic_post_header<binsrv::events::code_type::rotate>
      post_header{binsrv::events::magic_binlog_offset};
  const binsrv::events::composite_binlog_name binlog_name{"binlog", 1U};
  const binsrv::events::generic_body<binsrv::events::code_type::rotate> body{
      binlog_name};

  const auto generated_event{
      binsrv::events::event::create_event<binsrv::events::code_type::rotate>(
          offset,
          // artificial ROTATE event must include zero timestamp
          util::ctime_timestamp{}, default_server_id, flags, post_header, body,
          true, event_with_footer_buffer)};

  const binsrv::events::event_view generated_event_v{
      context_with_checksum, util::const_byte_span{event_with_footer_buffer}};

  BOOST_CHECK(generated_event_v.has_footer());
  BOOST_CHECK(generated_event_v.get_footer_view().get_crc_raw() ==
              generated_event_v.calculate_crc());
  BOOST_CHECK(generated_event_v.get_common_header_view().get_event_size_raw() ==
              generated_event_v.get_total_size());
  BOOST_CHECK(generated_event.calculate_encoded_size() ==
              generated_event_v.get_total_size());

  binsrv::events::event_storage materialization_buffer;

  // checking materialization of an event that has checksum
  const binsrv::events::event_view checksum_as_is_generated_v{
      binsrv::events::rewriter::materialize(
          generated_event_v, materialization_buffer,
          binsrv::events::materialization_type::leave_checksum_as_is)};
  BOOST_CHECK(checksum_as_is_generated_v.has_footer());
  BOOST_CHECK(checksum_as_is_generated_v.get_footer_view().get_crc_raw() ==
              checksum_as_is_generated_v.calculate_crc());
  BOOST_CHECK(checksum_as_is_generated_v.get_common_header_view()
                  .get_event_size_raw() ==
              checksum_as_is_generated_v.get_total_size());
  const binsrv::events::event_view checked_checksum_as_is_generated_v{
      context_with_checksum, util::const_byte_span{materialization_buffer}};

  const binsrv::events::event_view force_add_checksum_generated_v{
      binsrv::events::rewriter::materialize(
          generated_event_v, materialization_buffer,
          binsrv::events::materialization_type::force_add_checksum)};
  BOOST_CHECK(force_add_checksum_generated_v.has_footer());
  BOOST_CHECK(force_add_checksum_generated_v.get_footer_view().get_crc_raw() ==
              force_add_checksum_generated_v.calculate_crc());
  BOOST_CHECK(force_add_checksum_generated_v.get_common_header_view()
                  .get_event_size_raw() ==
              force_add_checksum_generated_v.get_total_size());
  const binsrv::events::event_view checked_force_add_checksum_generated_v{
      context_with_checksum, util::const_byte_span{materialization_buffer}};

  const binsrv::events::event_view force_remove_checksum_generated_v{
      binsrv::events::rewriter::materialize(
          generated_event_v, materialization_buffer,
          binsrv::events::materialization_type::force_remove_checksum)};
  BOOST_CHECK(!force_remove_checksum_generated_v.has_footer());
  BOOST_CHECK(force_remove_checksum_generated_v.get_common_header_view()
                  .get_event_size_raw() ==
              force_remove_checksum_generated_v.get_total_size());
  const binsrv::events::event_view checked_force_remove_checksum_generated_v{
      context_wo_checksum, util::const_byte_span{materialization_buffer}};

  // checking materialization of an event that does not have checksum
  const binsrv::events::event_storage event_wo_footer_buffer{
      materialization_buffer};
  const binsrv::events::event_view copied_event_v{
      context_wo_checksum, util::const_byte_span{event_wo_footer_buffer}};

  const binsrv::events::event_view checksum_as_is_copied_v{
      binsrv::events::rewriter::materialize(
          copied_event_v, materialization_buffer,
          binsrv::events::materialization_type::leave_checksum_as_is)};
  BOOST_CHECK(!checksum_as_is_copied_v.has_footer());
  BOOST_CHECK(
      checksum_as_is_copied_v.get_common_header_view().get_event_size_raw() ==
      checksum_as_is_copied_v.get_total_size());
  const binsrv::events::event_view checked_checksum_as_is_copied_v{
      context_wo_checksum, util::const_byte_span{materialization_buffer}};

  const binsrv::events::event_view force_add_checksum_copied_v{
      binsrv::events::rewriter::materialize(
          copied_event_v, materialization_buffer,
          binsrv::events::materialization_type::force_add_checksum)};
  BOOST_CHECK(force_add_checksum_copied_v.has_footer());
  BOOST_CHECK(force_add_checksum_copied_v.get_footer_view().get_crc_raw() ==
              force_add_checksum_copied_v.calculate_crc());
  BOOST_CHECK(force_add_checksum_copied_v.get_common_header_view()
                  .get_event_size_raw() ==
              force_add_checksum_copied_v.get_total_size());
  const binsrv::events::event_view checked_force_add_checksum_copied_v{
      context_with_checksum, util::const_byte_span{materialization_buffer}};

  const binsrv::events::event_view force_remove_checksum_copied_v{
      binsrv::events::rewriter::materialize(
          copied_event_v, materialization_buffer,
          binsrv::events::materialization_type::force_remove_checksum)};
  BOOST_CHECK(!force_remove_checksum_copied_v.has_footer());
  BOOST_CHECK(force_remove_checksum_copied_v.get_common_header_view()
                  .get_event_size_raw() ==
              force_remove_checksum_copied_v.get_total_size());
  const binsrv::events::event_view checked_force_remove_checksum_copied_v{
      context_wo_checksum, util::const_byte_span{materialization_buffer}};
}

// ---------------------------------------------------------------------------
// exact boundaries and text forms (gaps found by mutation testing, see
// extra/qa/mull)
// ---------------------------------------------------------------------------

// through the library's printing functions: the name helpers are inline in
// headers, so calling them here would test this file's own copy of them
BOOST_AUTO_TEST_CASE(EventEnumAndFlagNames) {
  using binsrv::events::checksum_algorithm_type;
  using fde_body = binsrv::events::generic_body<
      binsrv::events::code_type::format_description>;
  BOOST_CHECK_EQUAL(
      fde_body{checksum_algorithm_type::off}.get_readable_checksum_algorithm(),
      "off");
  BOOST_CHECK_EQUAL(fde_body{checksum_algorithm_type::crc32}
                        .get_readable_checksum_algorithm(),
                    "crc32");

  using binsrv::events::common_header_view_base;
  // binlog_in_use 0x001, artificial 0x020, mts_isolate 0x200; 0x002 is not
  // a flag and has no name
  BOOST_CHECK_EQUAL(
      common_header_view_base::get_readable_flags_from_raw(0x221U),
      "binlog_in_use | artificial | mts_isolate");
  BOOST_CHECK_EQUAL(common_header_view_base::get_readable_flags_from_raw(0x020U),
                    "artificial");
  BOOST_CHECK(common_header_view_base::get_readable_flags_from_raw(0x002U)
                  .empty());
  BOOST_CHECK(common_header_view_base::get_readable_flags_from_raw(0U).empty());
}

BOOST_AUTO_TEST_CASE(EventPostHeaderLengthTables) {
  using binsrv::events::code_type;
  using binsrv::events::reader_context;
  const util::semantic_version v80{8U, 0U, 45U};
  const util::semantic_version v84{8U, 4U, 8U};
  // GTID_TAGGED_LOG (code 42) is past the end of the 8.0 table (42 codes,
  // 1-41): its post header length is unknown there
  BOOST_CHECK_EQUAL(binsrv::events::get_post_header_length_for_code(
                        v80.get_encoded(),
                        reader_context::get_hardcoded_post_header_lengths(
                            v80.get_encoded()),
                        code_type::gtid_tagged_log),
                    binsrv::events::unspecified_post_header_length);
  BOOST_CHECK_EQUAL(binsrv::events::get_post_header_length_for_code(
                        v84.get_encoded(),
                        reader_context::get_hardcoded_post_header_lengths(
                            v84.get_encoded()),
                        code_type::gtid_tagged_log),
                    0U);

  // one line per code from 1 on: 41 for 8.0, 42 for 8.4
  const auto printed{[](const util::semantic_version &version) {
    std::ostringstream output;
    binsrv::events::print_post_header_lengths(
        output, version.get_encoded(),
        reader_context::get_hardcoded_post_header_lengths(
            version.get_encoded()));
    return output.str();
  }};
  const auto v80_text{printed(v80)};
  const auto v84_text{printed(v84)};
  BOOST_CHECK_EQUAL(std::ranges::count(v80_text, '\n'), 40);
  BOOST_CHECK_EQUAL(std::ranges::count(v84_text, '\n'), 41);
  BOOST_CHECK(v80_text.starts_with("start_v3"));
  BOOST_CHECK(v80_text.find("gtid_tagged_log") == std::string::npos);
  BOOST_CHECK(v84_text.find("gtid_tagged_log") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(EventBinlogNameSequenceNumberLimit) {
  using binsrv::events::composite_binlog_name;
  BOOST_CHECK_NO_THROW(
      static_cast<void>(composite_binlog_name("binlog", 999'999U)));
  BOOST_CHECK_THROW(
      static_cast<void>(composite_binlog_name("binlog", 1'000'000U)),
      std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(EventFormatDescriptionChecksumAlgorithmByte) {
  using fde_body = binsrv::events::generic_body<
      binsrv::events::code_type::format_description>;
  const std::array<std::byte, 1U> crc32{std::byte{1U}};
  const std::array<std::byte, 1U> first_invalid{std::byte{2U}};
  BOOST_CHECK_NO_THROW(
      static_cast<void>(fde_body{util::const_byte_span{crc32}}));
  BOOST_CHECK_THROW(
      static_cast<void>(fde_body{util::const_byte_span{first_invalid}}),
      std::logic_error);
}

// encode_to() fits exactly into a buffer of calculate_encoded_size() bytes
// and refuses one a byte smaller
BOOST_AUTO_TEST_CASE(EventEncodeIntoExactSizeBuffers) {
  using binsrv::events::code_type;
  const binsrv::events::generic_body<code_type::format_description> body{
      binsrv::events::checksum_algorithm_type::crc32};
  std::vector<std::byte> body_buffer(body.calculate_encoded_size());
  util::byte_span body_span{body_buffer};
  BOOST_CHECK_NO_THROW(body.encode_to(body_span));
  BOOST_CHECK(body_span.empty());
  util::byte_span body_short{std::data(body_buffer), 0U};
  BOOST_CHECK_THROW(body.encode_to(body_short), std::invalid_argument);

  const binsrv::events::generic_post_header<code_type::rotate> post_header{
      binsrv::events::magic_binlog_offset};
  std::vector<std::byte> post_header_buffer(
      post_header.calculate_encoded_size());
  util::byte_span post_header_span{post_header_buffer};
  BOOST_CHECK_NO_THROW(post_header.encode_to(post_header_span));
  BOOST_CHECK(post_header_span.empty());
  util::byte_span post_header_short{std::data(post_header_buffer),
                                    std::size(post_header_buffer) - 1U};
  BOOST_CHECK_THROW(post_header.encode_to(post_header_short),
                    std::invalid_argument);
}

// next_event_position is 0 exactly for artificial ROTATE and
// FORMAT_DESCRIPTION events
BOOST_AUTO_TEST_CASE(EventCommonHeaderNextPositionOfArtificialEvents) {
  using binsrv::events::code_type;
  using binsrv::events::common_header;
  using binsrv::events::common_header_flag_type;
  const binsrv::events::common_header_flag_set artificial{
      common_header_flag_type::artificial};
  const util::ctime_timestamp timestamp{std::time_t{1}};
  const auto next{[&](code_type code,
                      binsrv::events::common_header_flag_set flags) {
    return common_header::create_with_offset(100U, 30U, timestamp, code, 1U,
                                             flags)
        .get_next_event_position_raw();
  }};
  BOOST_CHECK_EQUAL(next(code_type::rotate, artificial), 0U);
  BOOST_CHECK_EQUAL(next(code_type::format_description, artificial), 0U);
  BOOST_CHECK_EQUAL(next(code_type::rotate, {}), 130U);
  BOOST_CHECK_EQUAL(next(code_type::query, artificial), 130U);
}

BOOST_AUTO_TEST_CASE(EventTimestampEpoch) {
  BOOST_CHECK(util::ctime_timestamp{}.is_epoch());
  BOOST_CHECK(!util::ctime_timestamp{std::time_t{1}}.is_epoch());
}

namespace {

// a context that has seen an artificial ROTATE and a FORMAT_DESCRIPTION of
// an 8.4.8 server with checksums, as when parsing events from a server
[[nodiscard]] std::unique_ptr<binsrv::events::reader_context>
make_checksum_context() {
  using binsrv::events::code_type;
  const util::semantic_version version{8U, 4U, 8U};
  constexpr std::uint32_t server_id{42U};
  auto context{std::make_unique<binsrv::events::reader_context>(
      version.get_encoded(), true, binsrv::replication_mode_type::position, "",
      0U)};
  binsrv::events::event_storage buffer;
  [[maybe_unused]] const auto rotate{
      binsrv::events::event::create_event<code_type::rotate>(
          0U, util::ctime_timestamp{}, server_id,
          binsrv::events::common_header_flag_set{
              binsrv::events::common_header_flag_type::artificial},
          binsrv::events::generic_post_header<code_type::rotate>{
              binsrv::events::magic_binlog_offset},
          binsrv::events::generic_body<code_type::rotate>{
              binsrv::events::composite_binlog_name{"binlog", 1U}},
          true, buffer)};
  [[maybe_unused]] const auto rotate_info_only{context->process_event_view(
      binsrv::events::event_view{*context, util::const_byte_span{buffer}})};
  [[maybe_unused]] const auto format_description{
      binsrv::events::event::create_event<code_type::format_description>(
          static_cast<std::uint32_t>(binsrv::events::magic_binlog_offset),
          util::ctime_timestamp{1700000000}, server_id, {},
          binsrv::events::generic_post_header<code_type::format_description>{
              binsrv::events::default_binlog_version, version,
              util::ctime_timestamp{1700000000},
              binsrv::events::default_common_header_length,
              binsrv::events::reader_context::get_hardcoded_post_header_lengths(
                  version.get_encoded())},
          binsrv::events::generic_body<code_type::format_description>{
              binsrv::events::checksum_algorithm_type::crc32},
          true, buffer)};
  [[maybe_unused]] const auto format_description_info_only{
      context->process_event_view(
          binsrv::events::event_view{*context, util::const_byte_span{buffer}})};
  return context;
}

} // anonymous namespace

// an event shorter than its common header + post header + footer is refused
// as such: a 27-byte ROTATE when 19 + 8 + 4 = 31 are needed
BOOST_AUTO_TEST_CASE(EventViewRejectsAnEventShorterThanItsFixedParts) {
  const auto context{make_checksum_context()};
  constexpr std::size_t size{27U};
  std::vector<std::byte> bytes(size);
  util::byte_span header{bytes};
  util::insert_fixed_int_to_byte_span(header, std::uint32_t{1U});
  util::insert_fixed_int_to_byte_span(
      header, static_cast<std::uint8_t>(binsrv::events::code_type::rotate));
  util::insert_fixed_int_to_byte_span(header, std::uint32_t{42U});
  util::insert_fixed_int_to_byte_span(header,
                                      static_cast<std::uint32_t>(size));
  BOOST_CHECK_EXCEPTION(
      static_cast<void>(binsrv::events::event_view{
          *context, util::const_byte_span{bytes}}),
      std::logic_error, [](const std::logic_error &error) {
        return std::string_view{error.what()}.find(
                   "not enough data for event common header + post header + "
                   "footer") != std::string_view::npos;
      });
}

namespace {

using tagged_body =
    binsrv::events::generic_body<binsrv::events::code_type::gtid_tagged_log>;

[[nodiscard]] std::vector<std::byte> encode(const tagged_body &body) {
  std::vector<std::byte> result(body.calculate_encoded_size());
  util::byte_span destination{result};
  body.encode_to(destination);
  BOOST_REQUIRE(destination.empty());
  return result;
}

[[nodiscard]] std::vector<std::byte> varlen(std::uint64_t value) {
  std::vector<std::byte> result(util::calculate_varlen_int_size(value));
  util::byte_span destination{result};
  BOOST_REQUIRE(util::insert_varlen_int_to_byte_span_checked(destination,
                                                             value));
  return result;
}

} // anonymous namespace

// the encoded size of a GTID_TAGGED_LOG body counts its own varlen-encoded
// size field, whose width grows from 1 to 2 bytes at 128: bodies of every
// size around that boundary (tags of 1-32 characters, small and large field
// values, with and without the optional fields) encode into exactly
// calculate_encoded_size() bytes and decode back to the same body
BOOST_AUTO_TEST_CASE(EventGtidTaggedBodySizesAroundVarlenBoundaries) {
  const binsrv::gtids::uuid uuid{"3e11fa47-71ca-11e1-9e33-c80aa9429562"};
  const util::semantic_version version{8U, 4U, 8U};
  std::size_t smallest{std::numeric_limits<std::size_t>::max()};
  std::size_t largest{0U};
  for (const std::uint64_t magnitude :
       {std::uint64_t{1U}, std::uint64_t{1U} << 20U, std::uint64_t{1U} << 40U,
        std::uint64_t{1U} << 60U}) {
    for (const bool optional_fields : {false, true}) {
      for (std::size_t tag_length{1U}; tag_length <= 32U; ++tag_length) {
        const tagged_body body{
            binsrv::events::gtid_log_flag_set{},
            uuid,
            magnitude,
            binsrv::gtids::tag{std::string(tag_length, 'a')},
            magnitude,
            magnitude + 1U,
            util::microseconds_to_high_resolution_time_point(magnitude),
            optional_fields ? std::optional{util::
                                                microseconds_to_high_resolution_time_point(
                                                    magnitude)}
                            : std::nullopt,
            magnitude,
            version,
            optional_fields ? std::optional{version} : std::nullopt,
            optional_fields ? std::optional{magnitude} : std::nullopt};
        const auto encoded{encode(body)};
        smallest = std::min(smallest, std::size(encoded));
        largest = std::max(largest, std::size(encoded));
        BOOST_TEST_INFO("tag length " << tag_length << ", magnitude "
                                      << magnitude << ", optional fields "
                                      << optional_fields);
        BOOST_CHECK(tagged_body{util::const_byte_span{encoded}} == body);
      }
    }
  }
  // the sweep must cross the 1-to-2-byte boundary of the size field
  BOOST_CHECK_LT(smallest, 127U);
  BOOST_CHECK_GT(largest, 129U);
}

// field ids must strictly increase: a body whose last field (the
// commit_group_ticket, id 11) is repeated is refused
BOOST_AUTO_TEST_CASE(EventGtidTaggedBodyRejectsARepeatedFieldId) {
  const util::semantic_version version{8U, 4U, 8U};
  const tagged_body body{
      binsrv::events::gtid_log_flag_set{},
      binsrv::gtids::uuid{"3e11fa47-71ca-11e1-9e33-c80aa9429562"},
      binsrv::gtids::gno_t{5U},
      binsrv::gtids::tag{"alpha"},
      binsrv::events::seq_no_t{1U},
      binsrv::events::seq_no_t{2U},
      util::microseconds_to_high_resolution_time_point(1700000000000000ULL),
      std::nullopt,
      std::uint64_t{100U},
      version,
      std::nullopt,
      std::optional<std::uint64_t>{7U}};
  const auto encoded{encode(body)};
  BOOST_REQUIRE(tagged_body{util::const_byte_span{encoded}} == body);

  // <serialization version> <size> <last non-ignorable field id> <fields>
  util::const_byte_span remainder{encoded};
  std::uint8_t serialization_version{};
  std::uint64_t size{};
  BOOST_REQUIRE(util::extract_varlen_int_from_byte_span_checked(
      remainder, serialization_version));
  BOOST_REQUIRE(util::extract_varlen_int_from_byte_span_checked(remainder,
                                                                size));
  std::vector<std::byte> rest{std::cbegin(remainder), std::cend(remainder)};
  constexpr std::uint64_t commit_group_ticket_id{11U};
  for (const auto value : {commit_group_ticket_id, std::uint64_t{7U}}) {
    const auto bytes{varlen(value)};
    rest.insert(std::end(rest), std::cbegin(bytes), std::cend(bytes));
  }
  const auto version_bytes{varlen(serialization_version)};
  std::size_t new_size{std::size(version_bytes) + 1U + std::size(rest)};
  while (std::size(version_bytes) + std::size(varlen(new_size)) +
             std::size(rest) !=
         new_size) {
    new_size = std::size(version_bytes) + std::size(varlen(new_size)) +
               std::size(rest);
  }
  std::vector<std::byte> damaged{version_bytes};
  const auto size_bytes{varlen(new_size)};
  damaged.insert(std::end(damaged), std::cbegin(size_bytes),
                 std::cend(size_bytes));
  damaged.insert(std::end(damaged), std::cbegin(rest), std::cend(rest));
  BOOST_CHECK_EXCEPTION(
      static_cast<void>(tagged_body{util::const_byte_span{damaged}}),
      std::invalid_argument, [](const std::invalid_argument &error) {
        return std::string_view{error.what()}.find(
                   "broken field_id sequence") != std::string_view::npos;
      });
}
