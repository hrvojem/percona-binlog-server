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

// Property-based tests for binsrv::events::reader_context, the state machine
// that every event received from a server goes through, built with Hegel
// (https://hegel.dev).
//
// It validates the stream grammar, repeated for every binlog:
//   ROTATE(artificial) FORMAT_DESCRIPTION PREVIOUS_GTIDS_LOG?
//   ((ANONYMOUS_GTID_LOG | GTID_LOG | GTID_TAGGED_LOG) <ANY>*)*
//   (ROTATE | STOP)?
// checks next_event_position of every event and the declared length of
// every transaction, and reports transaction boundaries, which is where the
// storage flushes data and where streaming resumes after a restart.
//
// - valid streams (GTID or position-based mode, with or without checksums,
//   MySQL 8.0 or 8.4, one or more binlogs) are accepted, transaction
//   boundaries are reported exactly after the last event of every
//   transaction and after every ROTATE / STOP, and the reported GTID and
//   sequence number are the ones of the transaction;
// - streams with dropped, duplicated or swapped events (with positions and
//   checksums repaired, so that only the structure is wrong) are rejected
//   with the exceptions the state machine uses, and every transaction
//   reported complete before that starts with exactly one GTID event and has
//   exactly its declared length;
// - resuming from a stored binlog name and position (as after a restart)
//   accepts the continuation the server sends, with its preamble reported as
//   info-only.

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
#include <utility>
#include <vector>

// needed for binsrv::events::event_storage
#include <boost/container/small_vector.hpp> // IWYU pragma: keep

#define BOOST_TEST_MODULE ReaderContextPropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "property_test_helpers.hpp"

#include "binsrv/replication_mode_type.hpp"

#include "binsrv/gtids/common_types.hpp"
#include "binsrv/gtids/gtid.hpp"
#include "binsrv/gtids/gtid_set.hpp"
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

#include "util/byte_span_extractors.hpp"
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

constexpr std::uint32_t server_id{42U};
constexpr std::size_t footer_size{events::default_footer_length};
constexpr std::uint64_t magic_offset{events::magic_binlog_offset};
constexpr std::string_view binlog_base_name{"binlog"};

constexpr std::array<std::string_view, 2> uuid_pool{
    "11111111-aaaa-1111-aaaa-111111111111",
    "22222222-bbbb-2222-bbbb-222222222222"};
constexpr std::array<std::string_view, 2> tag_pool{"alpha", "beta"};

// a QUERY event (post header of 13 bytes) is the smallest event after the
// GTID event: common header + post header (+ footer)
constexpr std::size_t min_other_event_content{header::size_in_bytes + 13U};
constexpr std::size_t max_other_event_content{200U};

// ---------------------------------------------------------------------------
// stream description
// ---------------------------------------------------------------------------

enum class gtid_kind : std::uint8_t { anonymous, untagged, tagged };

struct transaction_spec {
  gtid_kind kind;
  std::size_t uuid_index;
  std::size_t tag_index;
  binsrv::gtids::gno_t gno;
  seq_no_t sequence_number;
  // sizes of the events after the GTID event, without footers
  std::vector<std::size_t> other_event_contents;
};

enum class binlog_end : std::uint8_t { none, rotate, stop };

struct binlog_spec {
  std::vector<transaction_spec> transactions;
  // how the binlog ends; 'none' only for the last one
  binlog_end end;
};

struct stream_spec {
  bool gtid_mode;
  bool with_checksum;
  bool v84;
  std::vector<binlog_spec> binlogs;
};

std::ostream &operator<<(std::ostream &output, const stream_spec &spec) {
  output << (spec.gtid_mode ? "gtid" : "position") << " mode, "
         << (spec.with_checksum ? "with" : "without") << " checksums, "
         << (spec.v84 ? "8.4" : "8.0") << ", binlogs:";
  for (const auto &binlog : spec.binlogs) {
    output << " [";
    for (const auto &transaction : binlog.transactions) {
      output << (transaction.kind == gtid_kind::anonymous ? 'A'
                 : transaction.kind == gtid_kind::tagged  ? 'T'
                                                          : 'G')
             << std::size(transaction.other_event_contents) << ' ';
    }
    output << (binlog.end == binlog_end::rotate ? "ROTATE"
               : binlog.end == binlog_end::stop ? "STOP"
                                                : "-")
           << ']';
  }
  return output;
}

[[nodiscard]] gs::Generator<stream_spec> stream_specs() {
  return gs::compose([](const hegel::TestCase &tc) {
    static constexpr std::size_t max_binlogs{3U};
    static constexpr std::size_t max_transactions{5U};
    static constexpr std::size_t max_other_events{3U};
    stream_spec spec{.gtid_mode = tc.draw(gs::booleans()),
                     .with_checksum = tc.draw(gs::booleans()),
                     .v84 = tc.draw(gs::booleans()),
                     .binlogs = {}};
    // in position-based mode the server may have GTIDs on or off
    const bool anonymous{!spec.gtid_mode && tc.draw(gs::booleans())};
    binsrv::gtids::gno_t next_gno{1U};
    const auto number_of_binlogs{tc.draw(gs::integers<std::size_t>(
        {.min_value = 1U, .max_value = max_binlogs}))};
    for (std::size_t binlog{0U}; binlog < number_of_binlogs; ++binlog) {
      binlog_spec binlog_value{.transactions = {}, .end = binlog_end::none};
      const auto number_of_transactions{tc.draw(gs::integers<std::size_t>(
          {.min_value = 0U, .max_value = max_transactions}))};
      for (std::size_t index{0U}; index < number_of_transactions; ++index) {
        gtid_kind kind{gtid_kind::anonymous};
        if (!anonymous) {
          kind = (spec.v84 && tc.draw(gs::booleans())) ? gtid_kind::tagged
                                                       : gtid_kind::untagged;
        }
        binlog_value.transactions.push_back(
            {.kind = kind,
             .uuid_index = tc.draw(gs::integers<std::size_t>(
                 {.min_value = 0U, .max_value = std::size(uuid_pool) - 1U})),
             .tag_index = tc.draw(gs::integers<std::size_t>(
                 {.min_value = 0U, .max_value = std::size(tag_pool) - 1U})),
             .gno = next_gno++,
             .sequence_number = index + 1U,
             .other_event_contents = tc.draw(gs::vectors(
                 gs::integers<std::size_t>(
                     {.min_value = min_other_event_content,
                      .max_value = max_other_event_content}),
                 {.min_size = 0U, .max_size = max_other_events}))});
      }
      const bool last{binlog + 1U == number_of_binlogs};
      binlog_value.end =
          last ? tc.draw(gs::sampled_from(
                     {binlog_end::none, binlog_end::rotate, binlog_end::stop}))
               : tc.draw(gs::sampled_from({binlog_end::rotate, binlog_end::stop}));
      spec.binlogs.push_back(std::move(binlog_value));
    }
    return spec;
  });
}

// ---------------------------------------------------------------------------
// encoded stream
// ---------------------------------------------------------------------------

enum class event_role : std::uint8_t {
  artificial_rotate,
  format_description,
  previous_gtids,
  gtid,
  other,
  rotate,
  stop
};

struct stream_event {
  events::event_storage bytes;
  event_role role;
  // for transaction events: the index of the transaction in the stream
  std::optional<std::size_t> transaction;
  bool last_of_transaction;
};

struct expected_transaction {
  binsrv::gtids::gtid gtid;
  seq_no_t sequence_number;
};

struct encoded_stream {
  std::uint32_t encoded_server_version;
  bool with_checksum;
  binsrv::replication_mode_type mode;
  std::vector<stream_event> events;
  std::vector<expected_transaction> transactions;
};

[[nodiscard]] util::semantic_version version_of(bool v84) {
  return v84 ? util::semantic_version{8U, 4U, 8U}
             : util::semantic_version{8U, 0U, 45U};
}

void write_crc(events::event_storage &buffer) {
  const auto payload_size{std::size(buffer) - footer_size};
  const events::footer_updatable_view footer_uv{util::byte_span{
      std::next(std::data(buffer), static_cast<std::ptrdiff_t>(payload_size)),
      footer_size}};
  footer_uv.set_crc_raw(util::calculate_crc32(
      util::const_byte_span{std::data(buffer), payload_size}));
}

template <typename T>
void write_header_field(events::event_storage &buffer, std::size_t offset,
                        T value) {
  util::byte_span field{
      std::next(std::data(buffer), static_cast<std::ptrdiff_t>(offset)),
      sizeof value};
  util::insert_fixed_int_to_byte_span(field, value);
}

[[nodiscard]] std::uint32_t read_event_size(const events::event_storage &buffer) {
  util::const_byte_span field{util::const_byte_span{buffer}.subspan(
      header::event_size_offset, sizeof(std::uint32_t))};
  std::uint32_t result{};
  util::extract_fixed_int_from_byte_span(field, result);
  return result;
}

[[nodiscard]] events::event_storage
make_other_event(std::size_t content_size, bool with_checksum,
                 std::uint64_t offset, std::uint64_t seed) {
  const auto size{content_size + (with_checksum ? footer_size : 0U)};
  events::event_storage result(size);
  for (std::size_t index{0U}; index < size; ++index) {
    result[index] = static_cast<std::byte>((seed * 131U + index * 7U) & 0xFFU);
  }
  write_header_field(result, header::timestamp_offset,
                     std::uint32_t{1700000000U});
  write_header_field(result, header::type_code_offset,
                     static_cast<std::uint8_t>(code_type::query));
  write_header_field(result, header::server_id_offset, server_id);
  write_header_field(result, header::event_size_offset,
                     static_cast<std::uint32_t>(size));
  write_header_field(result, header::next_event_position_offset,
                     static_cast<std::uint32_t>(offset + size));
  write_header_field(result, header::flags_offset, std::uint16_t{0U});
  if (with_checksum) {
    write_crc(result);
  }
  return result;
}

// the GTID event of a transaction, with a transaction_length (which counts
// the GTID event itself) consistent with the sizes of the other events
[[nodiscard]] events::event_storage
make_gtid_event(const transaction_spec &transaction,
                const util::semantic_version &version, bool with_checksum,
                std::uint64_t offset, std::size_t other_events_size) {
  const auto timestamp{
      util::microseconds_to_high_resolution_time_point(1700000000000000ULL)};
  const util::ctime_timestamp header_timestamp{1700000000};
  const seq_no_t last_committed{transaction.sequence_number - 1U};
  std::uint64_t transaction_length{other_events_size};
  for (std::size_t iteration{0U}; iteration < 10U; ++iteration) {
    events::event_storage buffer;
    std::size_t event_size{};
    if (transaction.kind == gtid_kind::tagged) {
      const events::generic_body<code_type::gtid_tagged_log> body{
          events::gtid_log_flag_set{},
          binsrv::gtids::uuid{uuid_pool.at(transaction.uuid_index)},
          transaction.gno,
          binsrv::gtids::tag{tag_pool.at(transaction.tag_index)},
          last_committed,
          transaction.sequence_number,
          timestamp,
          std::nullopt,
          transaction_length,
          version,
          std::nullopt,
          std::nullopt};
      event_size = events::event::create_event<code_type::gtid_tagged_log>(
                       static_cast<std::uint32_t>(offset), header_timestamp,
                       server_id, {}, {}, body, with_checksum, buffer)
                       .calculate_encoded_size();
    } else {
      const bool anonymous{transaction.kind == gtid_kind::anonymous};
      const events::gtid_log_post_header post_header{
          events::gtid_log_flag_set{},
          anonymous ? binsrv::gtids::uuid{}
                    : binsrv::gtids::uuid{uuid_pool.at(transaction.uuid_index)},
          anonymous ? binsrv::gtids::gno_t{0U} : transaction.gno,
          events::gtid_log_post_header::known_logical_ts_code,
          last_committed,
          transaction.sequence_number};
      const events::gtid_log_body body{timestamp, std::nullopt,
                                       transaction_length, version,
                                       std::nullopt, std::nullopt};
      if (anonymous) {
        event_size =
            events::event::create_event<code_type::anonymous_gtid_log>(
                static_cast<std::uint32_t>(offset), header_timestamp,
                server_id, {}, post_header, body, with_checksum, buffer)
                .calculate_encoded_size();
      } else {
        event_size = events::event::create_event<code_type::gtid_log>(
                         static_cast<std::uint32_t>(offset), header_timestamp,
                         server_id, {}, post_header, body, with_checksum,
                         buffer)
                         .calculate_encoded_size();
      }
    }
    if (transaction_length == event_size + other_events_size) {
      return buffer;
    }
    transaction_length = event_size + other_events_size;
  }
  throw std::logic_error{"transaction_length did not converge"};
}

[[nodiscard]] binsrv::gtids::gtid
expected_gtid(const transaction_spec &transaction) {
  switch (transaction.kind) {
  case gtid_kind::anonymous:
    return binsrv::gtids::gtid{};
  case gtid_kind::untagged:
    return binsrv::gtids::gtid{
        binsrv::gtids::uuid{uuid_pool.at(transaction.uuid_index)},
        transaction.gno};
  case gtid_kind::tagged:
    return binsrv::gtids::gtid{
        binsrv::gtids::uuid{uuid_pool.at(transaction.uuid_index)},
        binsrv::gtids::tag{tag_pool.at(transaction.tag_index)},
        transaction.gno};
  }
  return {};
}

// encodes the stream a server sends from the start of the first binlog;
// 'resume_after' (binlog 0 only) skips the first transactions and produces
// the continuation a server sends when streaming resumes after them
[[nodiscard]] encoded_stream
encode_stream(const stream_spec &spec,
              std::optional<std::size_t> resume_after = std::nullopt) {
  const auto version{version_of(spec.v84)};
  encoded_stream result{.encoded_server_version = version.get_encoded(),
                        .with_checksum = spec.with_checksum,
                        .mode = spec.gtid_mode
                                    ? binsrv::replication_mode_type::gtid
                                    : binsrv::replication_mode_type::position,
                        .events = {},
                        .transactions = {}};
  binsrv::gtids::gtid_set executed{};
  std::uint64_t seed{0U};

  for (std::size_t binlog{0U}; binlog < std::size(spec.binlogs); ++binlog) {
    const auto &binlog_value{spec.binlogs[binlog]};
    const events::composite_binlog_name name{binlog_base_name,
                                             static_cast<std::uint32_t>(binlog + 1U)};
    const bool resuming{binlog == 0U && resume_after.has_value()};
    events::event_storage buffer;

    // the positions of the transactions in this binlog, to know where a
    // resumed stream continues
    std::uint64_t offset{magic_offset};
    std::vector<std::uint64_t> transaction_offsets;

    // the preamble as written in the binlog file
    events::event_storage fde_bytes;
    [[maybe_unused]] const auto fde{
        events::event::create_event<code_type::format_description>(
            static_cast<std::uint32_t>(magic_offset),
            util::ctime_timestamp{1700000000}, server_id, {},
            events::generic_post_header<code_type::format_description>{
                events::default_binlog_version, version,
                util::ctime_timestamp{1700000000},
                events::default_common_header_length,
                events::reader_context::get_hardcoded_post_header_lengths(
                    version.get_encoded())},
            events::generic_body<code_type::format_description>{
                spec.with_checksum ? events::checksum_algorithm_type::crc32
                                   : events::checksum_algorithm_type::off},
            true, fde_bytes)};
    offset += std::size(fde_bytes);
    events::event_storage previous_gtids_bytes;
    [[maybe_unused]] const auto previous_gtids{
        events::event::create_event<code_type::previous_gtids_log>(
            static_cast<std::uint32_t>(offset),
            util::ctime_timestamp{1700000000}, server_id, {}, {},
            events::generic_body<code_type::previous_gtids_log>{executed},
            spec.with_checksum, previous_gtids_bytes)};
    offset += std::size(previous_gtids_bytes);

    // the transactions, encoded at their positions in the binlog file
    struct encoded_transaction {
      std::vector<events::event_storage> events;
      expected_transaction expected;
    };
    std::vector<encoded_transaction> encoded;
    for (const auto &transaction : binlog_value.transactions) {
      transaction_offsets.push_back(offset);
      std::size_t other_events_size{0U};
      for (const auto content : transaction.other_event_contents) {
        other_events_size += content + (spec.with_checksum ? footer_size : 0U);
      }
      encoded_transaction value{.events = {},
                                .expected = {.gtid = expected_gtid(transaction),
                                             .sequence_number =
                                                 transaction.sequence_number}};
      value.events.push_back(make_gtid_event(transaction, version,
                                             spec.with_checksum, offset,
                                             other_events_size));
      offset += std::size(value.events.back());
      for (const auto content : transaction.other_event_contents) {
        value.events.push_back(
            make_other_event(content, spec.with_checksum, offset, seed++));
        offset += std::size(value.events.back());
      }
      if (transaction.kind != gtid_kind::anonymous) {
        executed += value.expected.gtid;
      }
      encoded.push_back(std::move(value));
    }

    // what the server sends
    const std::size_t first_transaction{resuming ? *resume_after : 0U};
    const std::uint64_t start_position{
        resuming ? (first_transaction < std::size(transaction_offsets)
                        ? transaction_offsets[first_transaction]
                        : offset)
                 : magic_offset};
    // artificial ROTATE: in position-based mode it carries the requested
    // position, in GTID-based mode always 4
    [[maybe_unused]] const auto artificial_rotate{
        events::event::create_event<code_type::rotate>(
            0U, util::ctime_timestamp{}, server_id,
            events::common_header_flag_set{
                events::common_header_flag_type::artificial},
            events::generic_post_header<code_type::rotate>{
                spec.gtid_mode ? magic_offset : start_position},
            events::generic_body<code_type::rotate>{name}, spec.with_checksum,
            buffer)};
    result.events.push_back({.bytes = buffer,
                             .role = event_role::artificial_rotate,
                             .transaction = std::nullopt,
                             .last_of_transaction = false});
    if (resuming && !spec.gtid_mode) {
      // position-based resume: a pseudo FORMAT_DESCRIPTION (next event
      // position 0) and no PREVIOUS_GTIDS_LOG
      events::event_storage pseudo_fde{fde_bytes};
      write_header_field(pseudo_fde, header::next_event_position_offset,
                         std::uint32_t{0U});
      write_crc(pseudo_fde);
      result.events.push_back({.bytes = pseudo_fde,
                               .role = event_role::format_description,
                               .transaction = std::nullopt,
                               .last_of_transaction = false});
    } else {
      result.events.push_back({.bytes = fde_bytes,
                               .role = event_role::format_description,
                               .transaction = std::nullopt,
                               .last_of_transaction = false});
      result.events.push_back({.bytes = previous_gtids_bytes,
                               .role = event_role::previous_gtids,
                               .transaction = std::nullopt,
                               .last_of_transaction = false});
    }
    for (std::size_t index{first_transaction}; index < std::size(encoded);
         ++index) {
      const auto transaction_index{std::size(result.transactions)};
      result.transactions.push_back(encoded[index].expected);
      const auto &transaction_events{encoded[index].events};
      for (std::size_t event_index{0U};
           event_index < std::size(transaction_events); ++event_index) {
        result.events.push_back(
            {.bytes = transaction_events[event_index],
             .role = event_index == 0U ? event_role::gtid : event_role::other,
             .transaction = transaction_index,
             .last_of_transaction =
                 event_index + 1U == std::size(transaction_events)});
      }
    }
    if (binlog_value.end == binlog_end::rotate) {
      [[maybe_unused]] const auto rotate{
          events::event::create_event<code_type::rotate>(
              static_cast<std::uint32_t>(offset),
              util::ctime_timestamp{1700000000}, server_id, {},
              events::generic_post_header<code_type::rotate>{magic_offset},
              events::generic_body<code_type::rotate>{name.next()},
              spec.with_checksum, buffer)};
      result.events.push_back({.bytes = buffer,
                               .role = event_role::rotate,
                               .transaction = std::nullopt,
                               .last_of_transaction = false});
    } else if (binlog_value.end == binlog_end::stop) {
      [[maybe_unused]] const auto stop{
          events::event::create_event<code_type::stop>(
              static_cast<std::uint32_t>(offset),
              util::ctime_timestamp{1700000000}, server_id, {}, {}, {},
              spec.with_checksum, buffer)};
      result.events.push_back({.bytes = buffer,
                               .role = event_role::stop,
                               .transaction = std::nullopt,
                               .last_of_transaction = false});
    }
  }
  return result;
}

[[nodiscard]] std::unique_ptr<events::reader_context>
make_context(const encoded_stream &stream, std::string_view binlog_name = "",
             std::uint32_t position = 0U) {
  return std::make_unique<events::reader_context>(
      stream.encoded_server_version, stream.with_checksum, stream.mode,
      binlog_name, position);
}

[[nodiscard]] bool is_transaction_start(const events::event_storage &bytes) {
  const auto code{static_cast<code_type>(
      std::to_integer<std::uint8_t>(bytes[header::type_code_offset]))};
  return code == code_type::gtid_log || code == code_type::anonymous_gtid_log ||
         code == code_type::gtid_tagged_log;
}

// processes a valid stream and checks the reported boundaries, GTIDs and
// sequence numbers; 'info_only_events' is the number of leading events
// expected to be reported as info-only
void require_valid_stream_accepted(const encoded_stream &stream,
                                   events::reader_context &context,
                                   std::size_t info_only_events) {
  for (std::size_t index{0U}; index < std::size(stream.events); ++index) {
    const auto &event{stream.events[index]};
    const auto label{"event " + std::to_string(index + 1U) + ": "};
    bool info_only{};
    try {
      const events::event_view event_v{context,
                                       util::const_byte_span{event.bytes}};
      info_only = context.process_event_view(event_v);
    } catch (const std::exception &e) {
      throw std::runtime_error{label + "a valid event was rejected: " +
                               e.what()};
    }
    const bool expected_info_only{
        event.role == event_role::artificial_rotate || index < info_only_events};
    require(info_only == expected_info_only,
            label + (info_only ? "reported as info-only"
                               : "not reported as info-only"));

    const bool expected_boundary{event.last_of_transaction ||
                                 event.role == event_role::rotate ||
                                 event.role == event_role::stop};
    require(context.is_at_transaction_boundary() == expected_boundary,
            label + (expected_boundary
                         ? "no transaction boundary reported after it"
                         : "a transaction boundary reported after it"));
    if (event.last_of_transaction) {
      const auto &expected{stream.transactions[*event.transaction]};
      require(context.get_transaction_gtid() == expected.gtid,
              label + "transaction GTID '" +
                  context.get_transaction_gtid().str() + "' instead of '" +
                  expected.gtid.str() + "'");
      require(context.get_transaction_sequence_number() ==
                  expected.sequence_number,
              label + "transaction sequence number " +
                  std::to_string(context.get_transaction_sequence_number()) +
                  " instead of " + std::to_string(expected.sequence_number));
    }
  }
}

// ---------------------------------------------------------------------------
// stream mutations
// ---------------------------------------------------------------------------

enum class mutation_kind : std::uint8_t { drop, duplicate, swap_with_next };

struct mutation {
  mutation_kind kind;
  std::size_t position;
};

std::ostream &operator<<(std::ostream &output, const mutation &value) {
  static constexpr std::array names{std::string_view{"drop"},
                                    std::string_view{"duplicate"},
                                    std::string_view{"swap_with_next"}};
  return output << names.at(static_cast<std::size_t>(value.kind)) << '('
                << value.position << ')';
}

// rewrites next_event_position (and the checksum) of every event after the
// mutations, so that positions are consistent and only the structure of the
// stream is wrong
void repair_positions(std::vector<stream_event> &stream_events) {
  std::uint64_t position{magic_offset};
  for (auto &event : stream_events) {
    auto &bytes{event.bytes};
    if (event.role == event_role::artificial_rotate) {
      position = magic_offset;
      continue;
    }
    const auto size{read_event_size(bytes)};
    const bool has_footer{
        event.role == event_role::format_description ||
        std::size(bytes) == size}; // all events here keep their footer state
    const std::uint32_t next_position{
        static_cast<std::uint32_t>(position + size)};
    write_header_field(bytes, header::next_event_position_offset,
                       next_position);
    position = next_position;
    if (has_footer && event.role == event_role::format_description) {
      write_crc(bytes);
    }
    if (event.role == event_role::rotate || event.role == event_role::stop) {
      position = magic_offset;
    }
  }
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// properties
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(ReaderContextAcceptsValidStreams) {
  run_property([](hegel::TestCase &tc) {
    const auto spec{tc.draw("stream", stream_specs())};
    const auto stream{encode_stream(spec)};
    auto context{make_context(stream)};
    require_valid_stream_accepted(stream, *context, 0U);
  });
}

BOOST_AUTO_TEST_CASE(ReaderContextResumesFromStoredPosition) {
  run_property([](hegel::TestCase &tc) {
    const auto spec{tc.draw("stream", stream_specs())};
    // resume only after at least one transaction of the first binlog was
    // stored (otherwise the storage would be at the start of the binlog)
    tc.assume(!spec.binlogs.front().transactions.empty());
    const auto resume_after{tc.draw(
        "resume_after",
        gs::integers<std::size_t>(
            {.min_value = 1U,
             .max_value = std::size(spec.binlogs.front().transactions)}))};
    const auto full{encode_stream(spec)};
    const auto resumed{encode_stream(spec, resume_after)};

    // the stored position: right after the 'resume_after' transactions,
    // found from the next_event_position of their last event
    std::uint32_t stored_position{0U};
    for (const auto &event : full.events) {
      if (event.last_of_transaction && *event.transaction + 1U == resume_after) {
        util::const_byte_span field{util::const_byte_span{event.bytes}.subspan(
            header::next_event_position_offset, sizeof(std::uint32_t))};
        util::extract_fixed_int_from_byte_span(field, stored_position);
        break;
      }
    }
    require(stored_position > magic_offset, "test setup: no stored position");

    const events::composite_binlog_name first_binlog{binlog_base_name, 1U};
    auto context{make_context(resumed, first_binlog.str(), stored_position)};
    // the preamble of the resumed binlog (FORMAT_DESCRIPTION, plus
    // PREVIOUS_GTIDS_LOG in GTID mode) is info-only: it is already stored
    const std::size_t info_only_events{spec.gtid_mode ? 3U : 2U};
    require_valid_stream_accepted(resumed, *context, info_only_events);
  });
}

BOOST_AUTO_TEST_CASE(ReaderContextRejectsBrokenStreamsCleanly) {
  run_property([](hegel::TestCase &tc) {
    const auto spec{tc.draw("stream", stream_specs())};
    auto stream{encode_stream(spec)};
    const auto mutations{tc.draw(
        "mutations",
        gs::vectors(gs::compose([&stream](const hegel::TestCase &inner_tc) {
                      return mutation{
                          .kind = inner_tc.draw(gs::sampled_from(
                              {mutation_kind::drop, mutation_kind::duplicate,
                               mutation_kind::swap_with_next})),
                          .position = inner_tc.draw(gs::integers<std::size_t>(
                              {.min_value = 0U,
                               .max_value = std::size(stream.events) - 1U}))};
                    }),
                    {.min_size = 1U, .max_size = 3U}))};
    auto &stream_events{stream.events};
    for (const auto &[kind, requested_position] : mutations) {
      if (stream_events.empty()) {
        break;
      }
      const auto position{std::min(requested_position,
                                   std::size(stream_events) - 1U)};
      const auto it{std::next(std::begin(stream_events),
                              static_cast<std::ptrdiff_t>(position))};
      switch (kind) {
      case mutation_kind::drop:
        stream_events.erase(it);
        break;
      case mutation_kind::duplicate:
        stream_events.insert(it, *it);
        break;
      case mutation_kind::swap_with_next:
        if (std::next(it) != std::end(stream_events)) {
          std::iter_swap(it, std::next(it));
        }
        break;
      }
    }
    repair_positions(stream_events);

    auto context{make_context(stream)};
    // the events since the last reported boundary
    std::vector<const events::event_storage *> pending;
    for (const auto &event : stream_events) {
      try {
        const events::event_view event_v{*context,
                                         util::const_byte_span{event.bytes}};
        [[maybe_unused]] const auto info_only{
            context->process_event_view(event_v)};
      } catch (const std::logic_error &) {
        return; // a clean rejection
      }
      pending.push_back(&event.bytes);
      if (!context->is_at_transaction_boundary()) {
        continue;
      }
      // a reported boundary: the transaction part of the pending events
      // (from its GTID event on) must start with exactly one GTID event and
      // have exactly the length that GTID event declares
      const auto start{std::ranges::find_if(
          pending,
          [](const events::event_storage *bytes) {
            return is_transaction_start(*bytes);
          })};
      if (start != std::end(pending) && !context->get_transaction_gtid()
                                              .is_empty()) {
        std::uint64_t length{0U};
        std::size_t gtid_events{0U};
        for (auto it{start}; it != std::end(pending); ++it) {
          length += std::size(**it);
          gtid_events += is_transaction_start(**it) ? 1U : 0U;
        }
        const events::event_view gtid_v{*context,
                                        util::const_byte_span{**start}};
        const events::event gtid_event{gtid_v};
        std::uint64_t declared{};
        if (const auto *body{std::get_if<
                events::generic_body<code_type::gtid_tagged_log>>(
                &gtid_event.get_generic_body())}) {
          declared = body->get_transaction_length_raw();
        } else {
          declared = std::get<events::gtid_log_body>(
                         gtid_event.get_generic_body())
                         .get_transaction_length_raw();
        }
        require(gtid_events == 1U,
                "a transaction with " + std::to_string(gtid_events) +
                    " GTID events was reported complete");
        require(length == declared,
                "a transaction of " + std::to_string(length) +
                    " bytes was reported complete, its GTID event declares " +
                    std::to_string(declared));
      }
      pending.clear();
    }
  });
}
