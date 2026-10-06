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

// Property-based tests for binsrv::events::rewriter, which renumbers and
// relocates events in the 'replication.rewrite' mode, built with Hegel
// (https://hegel.dev).
//
// - materialization: an event copied with any checksum mode parses back with
//   the right size, position and footer, and is otherwise unchanged;
// - rewriting a stream of transactions from one or more source binlogs (each
//   with its own logical clock: sequence_number restarting at 1) into local
//   binlog files (numbering restarting at 1 in each file, as the collector
//   does when it rotates at 'rewrite.file_size'):
//   * sequence numbers are 1, 2, 3, ... in every local file and
//     0 <= last_committed < sequence_number;
//   * dependencies are kept: every transaction that a transaction waited for
//     on the source (an earlier source binlog, or a transaction of its own
//     source binlog with sequence_number <= its last_committed) and that is
//     in the same local file must still be covered by the new
//     last_committed;
//   * next_event_position and transaction_length match where the events
//     land, and GTIDs and all other event contents are unchanged.
//
// Source events always carry checksums, as the collector requires in the
// rewrite mode.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

// needed for binsrv::events::event_storage
#include <boost/container/small_vector.hpp> // IWYU pragma: keep

#define BOOST_TEST_MODULE RewriterPropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "property_test_helpers.hpp"

#include "binsrv/replication_mode_type.hpp"

#include "binsrv/gtids/common_types.hpp"
#include "binsrv/gtids/gtid.hpp"
#include "binsrv/gtids/tag.hpp"
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
#include "binsrv/events/gtid_log_body.hpp"
#include "binsrv/events/gtid_log_flag_type.hpp"
#include "binsrv/events/gtid_log_post_header.hpp"
#include "binsrv/events/protocol_traits_fwd.hpp"
#include "binsrv/events/reader_context.hpp"
#include "binsrv/events/rewriter.hpp"

#include "util/byte_span_fwd.hpp"
#include "util/byte_span_inserters.hpp"
#include "util/crc_helpers.hpp"
#include "util/ctime_timestamp.hpp"
#include "util/semantic_version.hpp"
#include "util/timestamp_helpers.hpp"

namespace {

namespace gs = hegel::generators;
namespace events = binsrv::events;

using property_testing::require;
using property_testing::run_property;

using events::code_type;
using events::seq_no_t;

using header = events::common_header_view_base;

const util::semantic_version server_version{8U, 4U, 8U};
constexpr std::uint32_t server_id{42U};
constexpr std::size_t magic_size{events::magic_binlog_offset};
constexpr std::size_t footer_size{events::default_footer_length};

constexpr std::array<std::string_view, 2> uuid_pool{
    "11111111-aaaa-1111-aaaa-111111111111",
    "22222222-bbbb-2222-bbbb-222222222222"};
constexpr std::array<std::string_view, 2> tag_pool{"alpha", "beta"};

// a reader context positioned after the preamble of a binlog stream (an
// artificial ROTATE and a FORMAT_DESCRIPTION event), so that it knows the
// post header lengths of all event types of the server version (including
// QUERY), as when parsing events received from a server
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
          0U, util::ctime_timestamp{}, server_id, artificial,
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
          util::ctime_timestamp{1700000000}, server_id, {},
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

[[nodiscard]] std::string describe_event(std::size_t index) {
  return "event " + std::to_string(index + 1U) + ": ";
}

// ---------------------------------------------------------------------------
// source transactions
// ---------------------------------------------------------------------------

struct source_transaction {
  bool tagged;
  std::size_t uuid_index;
  std::size_t tag_index;
  binsrv::gtids::gno_t gno;
  // the source binlog the transaction comes from, and its logical clock
  // there (sequence_number restarts at 1 in every source binlog)
  std::size_t source_binlog;
  seq_no_t sequence_number;
  seq_no_t last_committed;
  // sizes of the events following the GTID event
  std::vector<std::size_t> other_event_sizes;
};

std::ostream &operator<<(std::ostream &output,
                         const source_transaction &value) {
  output << (value.tagged ? "tagged " : "") << "u" << value.uuid_index;
  if (value.tagged) {
    output << ':' << tag_pool.at(value.tag_index);
  }
  output << ':' << value.gno << " (source binlog " << value.source_binlog
         << ", seq " << value.sequence_number << ", last_committed "
         << value.last_committed << ", " << std::size(value.other_event_sizes)
         << " more event(s))";
  return output;
}

// a QUERY event (post header of 13 bytes) is the smallest event after the
// GTID event: common header + post header + footer
constexpr std::size_t min_other_event_size{header::size_in_bytes + 13U +
                                           footer_size};
constexpr std::size_t max_other_event_size{300U};

[[nodiscard]] gs::Generator<std::vector<source_transaction>>
source_transactions() {
  return gs::compose([](const hegel::TestCase &tc) {
    static constexpr std::size_t max_source_binlogs{3U};
    static constexpr std::size_t max_transactions_per_binlog{8U};
    static constexpr std::size_t max_other_events{3U};
    std::vector<source_transaction> result;
    binsrv::gtids::gno_t next_gno{1U};
    const auto number_of_source_binlogs{tc.draw(gs::integers<std::size_t>(
        {.min_value = 1U, .max_value = max_source_binlogs}))};
    for (std::size_t binlog{0U}; binlog < number_of_source_binlogs; ++binlog) {
      const auto number_of_transactions{tc.draw(gs::integers<std::size_t>(
          {.min_value = 0U, .max_value = max_transactions_per_binlog}))};
      for (std::size_t index{0U}; index < number_of_transactions; ++index) {
        const seq_no_t sequence_number{index + 1U};
        source_transaction transaction{
            .tagged = tc.draw(gs::booleans()),
            .uuid_index = tc.draw(gs::integers<std::size_t>(
                {.min_value = 0U, .max_value = std::size(uuid_pool) - 1U})),
            .tag_index = tc.draw(gs::integers<std::size_t>(
                {.min_value = 0U, .max_value = std::size(tag_pool) - 1U})),
            .gno = next_gno++,
            .source_binlog = binlog,
            .sequence_number = sequence_number,
            // a transaction depends on an earlier one of its binlog, or on
            // none of them (0)
            .last_committed = tc.draw(gs::integers<seq_no_t>(
                {.min_value = 0U, .max_value = sequence_number - 1U})),
            .other_event_sizes = tc.draw(gs::vectors(
                gs::integers<std::size_t>({.min_value = min_other_event_size,
                                           .max_value = max_other_event_size}),
                {.min_size = 1U, .max_size = max_other_events}))};
        result.push_back(std::move(transaction));
      }
    }
    return result;
  });
}

// ---------------------------------------------------------------------------
// encoding source events
// ---------------------------------------------------------------------------

void write_crc(events::event_storage &buffer) {
  const auto payload_size{std::size(buffer) - footer_size};
  const events::footer_updatable_view footer_uv{util::byte_span{
      std::next(std::data(buffer), static_cast<std::ptrdiff_t>(payload_size)),
      footer_size}};
  footer_uv.set_crc_raw(util::calculate_crc32(
      util::const_byte_span{std::data(buffer), payload_size}));
}

// a QUERY event with distinct content, a valid common header and a checksum
[[nodiscard]] events::event_storage make_other_event(std::size_t size,
                                                     std::uint64_t seed) {
  events::event_storage result(size);
  for (std::size_t index{0U}; index < size; ++index) {
    result[index] =
        static_cast<std::byte>((seed * 131U + index * 7U) & 0xFFU);
  }
  const auto write_field{[&result](std::size_t offset, auto value) {
    util::byte_span field{
        std::next(std::data(result), static_cast<std::ptrdiff_t>(offset)),
        sizeof value};
    util::insert_fixed_int_to_byte_span(field, value);
  }};
  write_field(header::timestamp_offset, std::uint32_t{1700000000U});
  write_field(header::type_code_offset,
              static_cast<std::uint8_t>(code_type::query));
  write_field(header::server_id_offset, server_id);
  write_field(header::event_size_offset, static_cast<std::uint32_t>(size));
  write_field(header::next_event_position_offset, std::uint32_t{0U});
  write_field(header::flags_offset, std::uint16_t{0U});
  write_crc(result);
  return result;
}

// the GTID event of a transaction, whose transaction_length (which counts
// the GTID event itself, whose size depends on that length) is consistent
// with the sizes of the other events
[[nodiscard]] events::event_storage
make_gtid_event(const source_transaction &transaction) {
  std::size_t other_events_size{0U};
  for (const auto size : transaction.other_event_sizes) {
    other_events_size += size;
  }
  const binsrv::gtids::uuid uuid{uuid_pool.at(transaction.uuid_index)};
  const auto timestamp{util::microseconds_to_high_resolution_time_point(
      1700000000000000ULL)};
  const util::ctime_timestamp header_timestamp{1700000000};

  std::uint64_t transaction_length{other_events_size};
  for (std::size_t iteration{0U}; iteration < 10U; ++iteration) {
    events::event_storage buffer;
    std::size_t event_size{};
    if (transaction.tagged) {
      const events::generic_body<code_type::gtid_tagged_log> body{
          events::gtid_log_flag_set{},
          uuid,
          transaction.gno,
          binsrv::gtids::tag{tag_pool.at(transaction.tag_index)},
          transaction.last_committed,
          transaction.sequence_number,
          timestamp,
          std::nullopt,
          transaction_length,
          server_version,
          std::nullopt,
          std::nullopt};
      event_size = events::event::create_event<code_type::gtid_tagged_log>(
                       0U, header_timestamp, server_id, {}, {}, body, true,
                       buffer)
                       .calculate_encoded_size();
    } else {
      const events::generic_post_header<code_type::gtid_log> post_header{
          events::gtid_log_flag_set{},
          uuid,
          transaction.gno,
          events::gtid_log_post_header::known_logical_ts_code,
          transaction.last_committed,
          transaction.sequence_number};
      const events::generic_body<code_type::gtid_log> body{
          events::gtid_log_body{timestamp, std::nullopt, transaction_length,
                                server_version, std::nullopt, std::nullopt}};
      event_size =
          events::event::create_event<code_type::gtid_log>(
              0U, header_timestamp, server_id, {}, post_header, body, true,
              buffer)
              .calculate_encoded_size();
    }
    if (transaction_length == event_size + other_events_size) {
      return buffer;
    }
    transaction_length = event_size + other_events_size;
  }
  throw std::logic_error{"transaction_length did not converge"};
}

// ---------------------------------------------------------------------------
// checks shared by the properties
// ---------------------------------------------------------------------------

// common header fields that rewriting must not change
void require_same_common_header(const events::event &original,
                                const events::event &rewritten,
                                const std::string &label) {
  const auto &original_header{original.get_common_header()};
  const auto &rewritten_header{rewritten.get_common_header()};
  require(original_header.get_timestamp_raw() ==
                  rewritten_header.get_timestamp_raw() &&
              original_header.get_type_code() ==
                  rewritten_header.get_type_code() &&
              original_header.get_server_id_raw() ==
                  rewritten_header.get_server_id_raw() &&
              original_header.get_flags_raw() ==
                  rewritten_header.get_flags_raw(),
          label + "a common header field other than event_size / "
                  "next_event_position changed");
}

// position and size fields of a relocated event
void require_relocated(const events::event_view &rewritten_v,
                       std::uint64_t offset, const std::string &label) {
  const auto header_v{rewritten_v.get_common_header_view()};
  require(header_v.get_event_size_raw() == rewritten_v.get_total_size(),
          label + "event_size " + std::to_string(header_v.get_event_size_raw()) +
              " instead of " + std::to_string(rewritten_v.get_total_size()));
  require(header_v.get_next_event_position_raw() ==
              offset + rewritten_v.get_total_size(),
          label + "next_event_position " +
              std::to_string(header_v.get_next_event_position_raw()) +
              " instead of " +
              std::to_string(offset + rewritten_v.get_total_size()));
}

// the parts of a QUERY event between the common header and the footer must
// be copied unchanged
void require_same_payload(util::const_byte_span original,
                          util::const_byte_span rewritten,
                          std::size_t original_footer_size,
                          std::size_t rewritten_footer_size,
                          const std::string &label) {
  const auto original_payload{original.subspan(
      header::size_in_bytes,
      std::size(original) - header::size_in_bytes - original_footer_size)};
  const auto rewritten_payload{rewritten.subspan(
      header::size_in_bytes,
      std::size(rewritten) - header::size_in_bytes - rewritten_footer_size)};
  require(std::ranges::equal(original_payload, rewritten_payload),
          label + "the event content changed");
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// materialization
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(RewriterMaterializationKeepsContent) {
  run_property([](hegel::TestCase &tc) {
    const auto size{tc.draw("size", gs::integers<std::size_t>(
                                        {.min_value = min_other_event_size,
                                         .max_value = max_other_event_size}))};
    const auto source_has_checksum{tc.draw("source_has_checksum",
                                           gs::booleans())};
    const auto mode{tc.draw(
        "mode", gs::sampled_from({events::materialization_type::force_add_checksum,
                                  events::materialization_type::force_remove_checksum,
                                  events::materialization_type::leave_checksum_as_is}))};
    const auto relocate_offset{tc.draw(
        "relocate_offset",
        gs::one_of({gs::just(std::optional<std::uint32_t>{}),
                    gs::integers<std::uint32_t>({.min_value = 4U,
                                                 .max_value = 1U << 30U})
                        .map([](std::uint32_t value) {
                          return std::optional<std::uint32_t>{value};
                        })}))};

    // a source event with or without a footer
    auto source{make_other_event(size, 1U)};
    if (!source_has_checksum) {
      source.resize(size - footer_size);
      util::byte_span size_field{
          std::next(std::data(source),
                    static_cast<std::ptrdiff_t>(header::event_size_offset)),
          sizeof(std::uint32_t)};
      util::insert_fixed_int_to_byte_span(
          size_field, static_cast<std::uint32_t>(std::size(source)));
    }
    const auto source_context{make_context(source_has_checksum)};
    const events::event_view source_v{*source_context,
                                      util::const_byte_span{source}};

    events::event_storage buffer;
    const events::event_view result_v{
        relocate_offset.has_value()
            ? events::rewriter::materialize_and_relocate(source_v, buffer, mode,
                                                         *relocate_offset)
            : events::rewriter::materialize(source_v, buffer, mode)};

    const bool result_has_checksum{
        mode == events::materialization_type::force_add_checksum ||
        (mode == events::materialization_type::leave_checksum_as_is &&
         source_has_checksum)};
    // parsing the result back with the matching footer expectation verifies
    // its size field and (when present) its checksum
    const auto result_context{make_context(result_has_checksum)};
    const events::event_view parsed_v{*result_context,
                                      util::const_byte_span{buffer}};
    require(parsed_v.has_footer() == result_has_checksum,
            "the result footer does not match the checksum mode");
    require(parsed_v.get_total_size() ==
                std::size(source) - (source_has_checksum ? footer_size : 0U) +
                    (result_has_checksum ? footer_size : 0U),
            "the result has an unexpected size");
    require_same_payload(util::const_byte_span{source},
                         util::const_byte_span{buffer},
                         source_has_checksum ? footer_size : 0U,
                         result_has_checksum ? footer_size : 0U, "");
    require_same_common_header(events::event{source_v},
                               events::event{parsed_v}, "");
    if (relocate_offset.has_value()) {
      require_relocated(parsed_v, *relocate_offset, "");
    } else {
      require(parsed_v.get_common_header_view().get_next_event_position_raw() ==
                  source_v.get_common_header_view()
                      .get_next_event_position_raw(),
              "materialize() changed next_event_position");
    }
  });
}

// ---------------------------------------------------------------------------
// rewriting a stream of transactions
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(RewriterRenumbersTransactionsSafely) {
  run_property([](hegel::TestCase &tc) {
    const auto transactions{tc.draw("transactions", source_transactions())};
    // the collector rotates to a new local binlog at a transaction boundary
    // once 'rewrite.file_size' is reached; numbering restarts there
    const auto file_size{tc.draw(
        "file_size", gs::integers<std::size_t>({.min_value = 100U,
                                                .max_value = 3000U}))};
    const auto context_ptr{make_context(true)};
    const auto &context{*context_ptr};

    struct rewritten_transaction {
      std::size_t local_file;
      seq_no_t sequence_number;
      seq_no_t last_committed;
    };
    std::vector<rewritten_transaction> rewritten;

    std::size_t local_file{0U};
    std::uint64_t offset{magic_size};
    seq_no_t last_local_sequence_number{0U};
    std::uint64_t event_seed{0U};
    for (std::size_t index{0U}; index < std::size(transactions); ++index) {
      const auto &transaction{transactions[index]};
      const auto label{"transaction " + std::to_string(index + 1U) + ": "};
      if (offset >= file_size) {
        ++local_file;
        offset = magic_size;
        last_local_sequence_number = 0U;
      }
      const auto transaction_start{offset};

      // the GTID event
      const auto gtid_source{make_gtid_event(transaction)};
      const events::event_view gtid_source_v{
          context, util::const_byte_span{gtid_source}};
      events::event_storage gtid_buffer;
      [[maybe_unused]] const events::event_view gtid_rewritten_v{
          events::rewriter::rewrite(last_local_sequence_number, gtid_source_v,
                                    gtid_buffer, offset)};
      const events::event_view gtid_parsed_v{
          context, util::const_byte_span{gtid_buffer}};
      require_relocated(gtid_parsed_v, offset, label + "GTID event: ");
      const events::event gtid_original{gtid_source_v};
      const events::event gtid_rewritten{gtid_parsed_v};
      require_same_common_header(gtid_original, gtid_rewritten,
                                 label + "GTID event: ");

      seq_no_t new_sequence_number{};
      seq_no_t new_last_committed{};
      std::uint64_t transaction_length{};
      if (transaction.tagged) {
        auto expected{gtid_original.get_body<code_type::gtid_tagged_log>()};
        const auto &actual{
            gtid_rewritten.get_body<code_type::gtid_tagged_log>()};
        new_sequence_number = actual.get_sequence_number();
        new_last_committed = actual.get_last_committed();
        transaction_length = actual.get_transaction_length_raw();
        expected.set_sequence_number(new_sequence_number);
        expected.set_last_committed(new_last_committed);
        expected.set_transaction_length_raw(transaction_length);
        require(expected == actual,
                label + "a GTID_TAGGED_LOG field other than sequence_number "
                        "/ last_committed / transaction_length changed");
      } else {
        auto expected{
            gtid_original.get_post_header<code_type::gtid_log>()};
        const auto &actual{
            gtid_rewritten.get_post_header<code_type::gtid_log>()};
        new_sequence_number = actual.get_sequence_number();
        new_last_committed = actual.get_last_committed();
        expected.set_sequence_number(new_sequence_number);
        expected.set_last_committed(new_last_committed);
        require(expected == actual,
                label + "a GTID_LOG post header field other than "
                        "sequence_number / last_committed changed");
        require(gtid_original.get_body<code_type::gtid_log>() ==
                    gtid_rewritten.get_body<code_type::gtid_log>(),
                label + "the GTID_LOG body changed");
        transaction_length = gtid_rewritten.get_body<code_type::gtid_log>()
                                 .get_transaction_length_raw();
      }
      offset += gtid_parsed_v.get_total_size();
      last_local_sequence_number = new_sequence_number;

      // the other events of the transaction
      for (std::size_t event_index{0U};
           event_index < std::size(transaction.other_event_sizes);
           ++event_index) {
        const auto event_label{label + describe_event(event_index + 1U)};
        const auto source{make_other_event(
            transaction.other_event_sizes[event_index], event_seed++)};
        const events::event_view source_v{context,
                                          util::const_byte_span{source}};
        events::event_storage buffer;
        [[maybe_unused]] const events::event_view rewritten_v{
            events::rewriter::rewrite(last_local_sequence_number, source_v,
                                      buffer, offset)};
        const events::event_view parsed_v{context,
                                          util::const_byte_span{buffer}};
        require_relocated(parsed_v, offset, event_label);
        require_same_common_header(events::event{source_v},
                                   events::event{parsed_v}, event_label);
        require_same_payload(util::const_byte_span{source},
                             util::const_byte_span{buffer}, footer_size,
                             footer_size, event_label);
        offset += parsed_v.get_total_size();
      }

      require(transaction_length == offset - transaction_start,
              label + "transaction_length " +
                  std::to_string(transaction_length) +
                  " does not match the transaction size " +
                  std::to_string(offset - transaction_start));

      // numbering
      const seq_no_t expected_sequence_number{
          rewritten.empty() || rewritten.back().local_file != local_file
              ? 1U
              : rewritten.back().sequence_number + 1U};
      require(new_sequence_number == expected_sequence_number,
              label + "sequence_number " + std::to_string(new_sequence_number) +
                  " instead of " + std::to_string(expected_sequence_number));
      require(new_last_committed < new_sequence_number,
              label + "last_committed " + std::to_string(new_last_committed) +
                  " is not below sequence_number " +
                  std::to_string(new_sequence_number));

      // dependencies: every earlier transaction of this local file that
      // this one waited for on the source must be covered by last_committed
      for (std::size_t earlier{0U}; earlier < index; ++earlier) {
        const auto &earlier_source{transactions[earlier]};
        const auto &earlier_local{rewritten[earlier]};
        if (earlier_local.local_file != local_file) {
          continue;
        }
        const bool waited_for{
            earlier_source.source_binlog < transaction.source_binlog ||
            earlier_source.sequence_number <= transaction.last_committed};
        require(!waited_for ||
                    earlier_local.sequence_number <= new_last_committed,
                label + "last_committed " + std::to_string(new_last_committed) +
                    " no longer covers transaction " +
                    std::to_string(earlier + 1U) + " (local sequence_number " +
                    std::to_string(earlier_local.sequence_number) +
                    "), which it depended on in the source");
      }
      rewritten.push_back({.local_file = local_file,
                           .sequence_number = new_sequence_number,
                           .last_committed = new_last_committed});
    }
  });
}
