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

// Property-based tests for binlog event encoding / parsing
// (binsrv::events::event, binsrv::events::event_view), built with Hegel
// (https://hegel.dev).
//
// - round trips: an event created from generated fields and encoded must be
//   parsed back into an identical event, for every supported event type;
// - robustness: bytes received from the network (arbitrary ones, or valid
//   events with corrupted bytes) must either be parsed or rejected with one
//   of the exceptions the parsers use (std::logic_error and its descendants,
//   e.g. std::invalid_argument), never crash or fail in any other way;
// - checksums: flipping any single bit of an event with a CRC32 footer must
//   be detected.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <optional>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

// needed for binsrv::gtids::gtid_set_storage / binsrv::events::event_storage
#include <boost/container/small_vector.hpp> // IWYU pragma: keep

#define BOOST_TEST_MODULE EventPropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "property_test_helpers.hpp"

#include "binsrv/replication_mode_type.hpp"

#include "binsrv/gtids/common_types.hpp"
#include "binsrv/gtids/gtid_set.hpp"
#include "binsrv/gtids/tag.hpp"
#include "binsrv/gtids/uuid.hpp"

#include "binsrv/events/checksum_algorithm_type.hpp"
#include "binsrv/events/code_type.hpp"
#include "binsrv/events/common_header.hpp"
#include "binsrv/events/common_header_flag_type.hpp"
#include "binsrv/events/common_header_view.hpp"
#include "binsrv/events/common_types.hpp"
#include "binsrv/events/composite_binlog_name.hpp"
#include "binsrv/events/event.hpp"
#include "binsrv/events/event_view.hpp"
#include "binsrv/events/footer_view.hpp"
#include "binsrv/events/gtid_log_flag_type.hpp"
#include "binsrv/events/gtid_log_post_header.hpp"
#include "binsrv/events/heartbeat_log_v2_body_impl.hpp"
#include "binsrv/events/protocol_traits_fwd.hpp"
#include "binsrv/events/reader_context.hpp"

#include "util/byte_span.hpp"
#include "util/byte_span_fwd.hpp"
#include "util/byte_span_inserters.hpp"
#include "util/common_optional_types.hpp"
#include "util/crc_helpers.hpp"
#include "util/ctime_timestamp.hpp"
#include "util/semantic_version.hpp"
#include "util/timestamp_helpers.hpp"

namespace {

namespace gs = hegel::generators;
namespace events = binsrv::events;

using events::code_type;

// high_resolution_clock::duration is 64-bit nanoseconds, so time points
// generated from microseconds must stay below ~2^63 ns (around year 2262)
constexpr std::uint64_t max_generated_microseconds{9'000'000'000'000'000ULL};

// <tag> ::= [a-zA-Z_][a-zA-Z0-9_]{0,31} (see the grammar in gtid_set.cpp)
constexpr std::string_view tag_pattern{"[a-zA-Z_][a-zA-Z0-9_]{0,31}"};

using property_testing::require;
using property_testing::run_property;

[[nodiscard]] std::string to_string(const events::event &value) {
  std::ostringstream stream;
  stream << value;
  return stream.str();
}

[[nodiscard]] events::reader_context
make_context(std::uint32_t encoded_server_version, bool with_checksum) {
  return events::reader_context{encoded_server_version, with_checksum,
                                binsrv::replication_mode_type::gtid, "", 0U};
}

// ---------------------------------------------------------------------------
// field generators
// ---------------------------------------------------------------------------

template <typename T> [[nodiscard]] gs::Generator<T> any_integer() {
  return gs::integers<T>({.min_value = std::numeric_limits<T>::min(),
                          .max_value = std::numeric_limits<T>::max()});
}

[[nodiscard]] gs::Generator<std::uint64_t> signed_range_uint64() {
  // GNOs and logical clock values are stored as int64 on the wire
  return gs::integers<std::uint64_t>(
      {.min_value = 0ULL,
       .max_value = static_cast<std::uint64_t>(
           std::numeric_limits<std::int64_t>::max())});
}

[[nodiscard]] gs::Generator<std::uint64_t> generated_microseconds() {
  return gs::integers<std::uint64_t>(
      {.min_value = 0ULL, .max_value = max_generated_microseconds});
}

// servers this project speaks to: 8.0 (no tagged GTIDs) and 8.4
[[nodiscard]] util::semantic_version draw_server_version(
    const hegel::TestCase &tc, bool require_tagged_gtid_support) {
  static constexpr std::uint8_t major_version{8U};
  static constexpr std::uint8_t lts80_minor_version{0U};
  static constexpr std::uint8_t lts84_minor_version{4U};
  static constexpr std::uint8_t max_patch_version{99U};
  const auto minor{
      require_tagged_gtid_support
          ? lts84_minor_version
          : tc.draw(gs::sampled_from({lts80_minor_version, lts84_minor_version}))};
  const auto patch{tc.draw(gs::integers<std::uint8_t>(
      {.min_value = 0U, .max_value = max_patch_version}))};
  return util::semantic_version{major_version, minor, patch};
}

// arbitrary versions as recorded in GTID_LOG events (each component is
// encoded as a decimal pair: major * 10000 + minor * 100 + patch)
[[nodiscard]] util::semantic_version
draw_recorded_server_version(const hegel::TestCase &tc) {
  static constexpr std::uint8_t max_component{99U};
  const auto component_generator{gs::integers<std::uint8_t>(
      {.min_value = 0U, .max_value = max_component})};
  const auto major{tc.draw(component_generator)};
  const auto minor{tc.draw(component_generator)};
  const auto patch{tc.draw(component_generator)};
  return util::semantic_version{major, minor, patch};
}

[[nodiscard]] binsrv::gtids::uuid draw_uuid(const hegel::TestCase &tc) {
  const auto raw{tc.draw(gs::arrays<std::uint8_t, binsrv::gtids::uuid_length>(
      gs::integers<std::uint8_t>()))};
  binsrv::gtids::uuid_storage storage{};
  std::ranges::transform(raw, std::begin(storage),
                         [](std::uint8_t value) { return std::byte{value}; });
  return binsrv::gtids::uuid{storage};
}

[[nodiscard]] binsrv::gtids::uuid draw_non_nil_uuid(const hegel::TestCase &tc) {
  const auto result{draw_uuid(tc)};
  tc.assume(!result.is_empty());
  return result;
}

[[nodiscard]] binsrv::gtids::gtid_set
draw_gtid_set(const hegel::TestCase &tc) {
  static constexpr std::size_t max_intervals{6U};
  const auto number_of_intervals{tc.draw(
      gs::integers<std::size_t>({.min_value = 0U, .max_value = max_intervals}))};
  binsrv::gtids::gtid_set result{};
  for (std::size_t index{0U}; index < number_of_intervals; ++index) {
    const auto current_uuid{draw_non_nil_uuid(tc)};
    const auto tag_name{tc.draw(gs::one_of(
        {gs::just(std::string{}), gs::from_regex(std::string{tag_pattern})}))};
    const auto lower{tc.draw(gs::integers<binsrv::gtids::gno_t>(
        {.min_value = binsrv::gtids::min_gno,
         .max_value = binsrv::gtids::max_gno}))};
    const auto upper{tc.draw(gs::integers<binsrv::gtids::gno_t>(
        {.min_value = lower, .max_value = binsrv::gtids::max_gno}))};
    result.add_interval(current_uuid, binsrv::gtids::tag{tag_name}, lower,
                        upper);
  }
  return result;
}

template <typename T>
[[nodiscard]] std::optional<T>
draw_optional(const hegel::TestCase &tc,
              const std::function<T(const hegel::TestCase &)> &drawer) {
  if (!tc.draw(gs::booleans())) {
    return std::nullopt;
  }
  return drawer(tc);
}

[[nodiscard]] util::high_resolution_time_point
draw_time_point(const hegel::TestCase &tc) {
  return util::microseconds_to_high_resolution_time_point(
      tc.draw(generated_microseconds()));
}

// ---------------------------------------------------------------------------
// complete events
// ---------------------------------------------------------------------------

// an event created from generated fields together with its encoding and the
// parameters needed to parse it back
struct sample_event {
  std::uint32_t encoded_server_version;
  bool with_checksum;
  events::event generated;
  events::event_storage buffer;
};

std::ostream &operator<<(std::ostream &output, const sample_event &sample) {
  return output << "server version "
                << util::semantic_version{sample.encoded_server_version}
                       .get_string()
                << (sample.with_checksum ? ", with" : ", without")
                << " checksum\n"
                << sample.generated;
}

template <code_type Code>
[[nodiscard]] sample_event
make_sample(const hegel::TestCase &tc, const util::semantic_version &version,
            bool with_checksum, const events::generic_post_header<Code> &post_header,
            const events::generic_body<Code> &body,
            events::common_header_flag_set flags = {}) {
  const auto offset{tc.draw(any_integer<std::uint32_t>())};
  const util::ctime_timestamp timestamp{
      static_cast<std::time_t>(tc.draw(any_integer<std::uint32_t>()))};
  const auto server_id{tc.draw(any_integer<std::uint32_t>())};

  events::event_storage buffer;
  auto generated{events::event::create_event<Code>(
      offset, timestamp, server_id, flags, post_header, body, with_checksum,
      buffer)};
  return sample_event{.encoded_server_version = version.get_encoded(),
                      .with_checksum = with_checksum,
                      .generated = std::move(generated),
                      .buffer = std::move(buffer)};
}

// 'checksums' decides whether the event gets a CRC32 footer
// (FORMAT_DESCRIPTION events always have one)
using checksum_generator = gs::Generator<bool>;

[[nodiscard]] gs::Generator<sample_event>
rotate_events(const checksum_generator &checksums) {
  return gs::compose([checksums](const hegel::TestCase &tc) {
    static constexpr std::uint32_t max_sequence_number{999'999U};
    const auto version{draw_server_version(tc, false)};
    const events::composite_binlog_name binlog_name{
        tc.draw(gs::from_regex("[a-zA-Z0-9_-]{1,40}")),
        tc.draw(gs::integers<std::uint32_t>(
            {.min_value = 1U, .max_value = max_sequence_number}))};
    const events::common_header_flag_set flags{
        tc.draw(gs::booleans())
            ? events::common_header_flag_set{
                  events::common_header_flag_type::artificial}
            : events::common_header_flag_set{}};
    return make_sample<code_type::rotate>(
        tc, version, tc.draw(checksums),
        events::generic_post_header<code_type::rotate>{
            tc.draw(any_integer<std::uint64_t>())},
        events::generic_body<code_type::rotate>{binlog_name}, flags);
  });
}

[[nodiscard]] gs::Generator<sample_event> format_description_events() {
  return gs::compose([](const hegel::TestCase &tc) {
    const auto version{draw_server_version(tc, false)};
    const events::generic_post_header<code_type::format_description>
        post_header{events::default_binlog_version, version,
                    util::ctime_timestamp{static_cast<std::time_t>(
                        tc.draw(any_integer<std::uint32_t>()))},
                    events::default_common_header_length,
                    events::reader_context::get_hardcoded_post_header_lengths(
                        version.get_encoded())};
    const events::generic_body<code_type::format_description> body{
        tc.draw(gs::sampled_from({events::checksum_algorithm_type::off,
                                  events::checksum_algorithm_type::crc32}))};
    // FORMAT_DESCRIPTION events always include a footer
    return make_sample<code_type::format_description>(tc, version, true,
                                                      post_header, body);
  });
}

[[nodiscard]] gs::Generator<sample_event>
previous_gtids_log_events(const checksum_generator &checksums) {
  return gs::compose([checksums](const hegel::TestCase &tc) {
    const auto version{draw_server_version(tc, false)};
    return make_sample<code_type::previous_gtids_log>(
        tc, version, tc.draw(checksums),
        events::generic_post_header<code_type::previous_gtids_log>{},
        events::generic_body<code_type::previous_gtids_log>{
            draw_gtid_set(tc)});
  });
}

// GTID_LOG and ANONYMOUS_GTID_LOG share the same body layout
[[nodiscard]] events::gtid_log_body draw_gtid_log_body(const hegel::TestCase &tc) {
  const auto immediate_commit_timestamp{draw_time_point(tc)};
  const auto original_commit_timestamp{
      draw_optional<util::high_resolution_time_point>(tc, draw_time_point)};
  const auto transaction_length{tc.draw(any_integer<std::uint64_t>())};
  // the encoding stores the original server version and the commit group
  // ticket only after the immediate server version (as MySQL Server does)
  const auto immediate_server_version{
      draw_optional<util::semantic_version>(tc, draw_recorded_server_version)};
  util::optional_semantic_version original_server_version{};
  util::optional_uint64_t commit_group_ticket{};
  if (immediate_server_version.has_value()) {
    original_server_version =
        draw_optional<util::semantic_version>(tc, draw_recorded_server_version);
    // UINT64_MAX means "unset"
    commit_group_ticket = draw_optional<std::uint64_t>(
        tc, [](const hegel::TestCase &inner_tc) {
          return inner_tc.draw(gs::integers<std::uint64_t>(
              {.min_value = 0ULL,
               .max_value = std::numeric_limits<std::uint64_t>::max() - 1ULL}));
        });
  }
  return events::gtid_log_body{immediate_commit_timestamp,
                               original_commit_timestamp,
                               transaction_length,
                               immediate_server_version,
                               original_server_version,
                               commit_group_ticket};
}

[[nodiscard]] events::gtid_log_flag_set
draw_gtid_log_flags(const hegel::TestCase &tc) {
  return events::gtid_log_flag_set{
      tc.draw(any_integer<events::gtid_log_flag_set::underlying_type>())};
}

[[nodiscard]] gs::Generator<sample_event>
gtid_log_events(const checksum_generator &checksums) {
  return gs::compose([checksums](const hegel::TestCase &tc) {
    const auto version{draw_server_version(tc, false)};
    const events::generic_post_header<code_type::gtid_log> post_header{
        draw_gtid_log_flags(tc),
        draw_non_nil_uuid(tc),
        tc.draw(gs::integers<binsrv::gtids::gno_t>(
            {.min_value = binsrv::gtids::min_gno,
             .max_value = binsrv::gtids::max_gno})),
        events::gtid_log_post_header::known_logical_ts_code,
        tc.draw(signed_range_uint64()),
        tc.draw(signed_range_uint64())};
    return make_sample<code_type::gtid_log>(
        tc, version, tc.draw(checksums), post_header,
        events::generic_body<code_type::gtid_log>{draw_gtid_log_body(tc)});
  });
}

[[nodiscard]] gs::Generator<sample_event>
anonymous_gtid_log_events(const checksum_generator &checksums) {
  return gs::compose([checksums](const hegel::TestCase &tc) {
    const auto version{draw_server_version(tc, false)};
    const events::generic_post_header<code_type::anonymous_gtid_log>
        post_header{draw_gtid_log_flags(tc),
                    binsrv::gtids::uuid{},
                    binsrv::gtids::gno_t{0ULL},
                    events::gtid_log_post_header::known_logical_ts_code,
                    tc.draw(signed_range_uint64()),
                    tc.draw(signed_range_uint64())};
    return make_sample<code_type::anonymous_gtid_log>(
        tc, version, tc.draw(checksums), post_header,
        events::generic_body<code_type::anonymous_gtid_log>{
            draw_gtid_log_body(tc)});
  });
}

[[nodiscard]] gs::Generator<sample_event>
gtid_tagged_log_events(const checksum_generator &checksums) {
  return gs::compose([checksums](const hegel::TestCase &tc) {
    const auto version{draw_server_version(tc, true)};
    const auto flags{draw_gtid_log_flags(tc)};
    const auto current_uuid{draw_non_nil_uuid(tc)};
    const auto gno{tc.draw(gs::integers<binsrv::gtids::gno_t>(
        {.min_value = binsrv::gtids::min_gno,
         .max_value = binsrv::gtids::max_gno}))};
    const binsrv::gtids::tag current_tag{
        tc.draw(gs::from_regex(std::string{tag_pattern}))};
    const auto last_committed{tc.draw(signed_range_uint64())};
    const auto sequence_number{tc.draw(signed_range_uint64())};
    const auto immediate_commit_timestamp{draw_time_point(tc)};
    const auto original_commit_timestamp{
        draw_optional<util::high_resolution_time_point>(tc, draw_time_point)};
    const auto transaction_length{tc.draw(any_integer<std::uint64_t>())};
    const auto immediate_server_version{draw_recorded_server_version(tc)};
    const auto original_server_version{
        draw_optional<util::semantic_version>(tc, draw_recorded_server_version)};
    const auto commit_group_ticket{draw_optional<std::uint64_t>(
        tc, [](const hegel::TestCase &inner_tc) {
          return inner_tc.draw(gs::integers<std::uint64_t>(
              {.min_value = 0ULL,
               .max_value = std::numeric_limits<std::uint64_t>::max() - 1ULL}));
        })};
    const events::generic_body<code_type::gtid_tagged_log> body{
        flags,
        current_uuid,
        gno,
        current_tag,
        last_committed,
        sequence_number,
        immediate_commit_timestamp,
        original_commit_timestamp,
        transaction_length,
        immediate_server_version,
        original_server_version,
        commit_group_ticket};
    return make_sample<code_type::gtid_tagged_log>(
        tc, version, tc.draw(checksums),
        events::generic_post_header<code_type::gtid_tagged_log>{}, body);
  });
}

[[nodiscard]] gs::Generator<sample_event>
stop_events(const checksum_generator &checksums) {
  return gs::compose([checksums](const hegel::TestCase &tc) {
    const auto version{draw_server_version(tc, false)};
    return make_sample<code_type::stop>(
        tc, version, tc.draw(checksums),
        events::generic_post_header<code_type::stop>{},
        events::generic_body<code_type::stop>{});
  });
}

[[nodiscard]] gs::Generator<sample_event> any_events() {
  const auto checksums{gs::booleans()};
  return gs::one_of({rotate_events(checksums), format_description_events(),
                     previous_gtids_log_events(checksums),
                     gtid_log_events(checksums),
                     anonymous_gtid_log_events(checksums),
                     gtid_tagged_log_events(checksums), stop_events(checksums)});
}

// every event type except FORMAT_DESCRIPTION, whose checksum is verified or
// not depending on its own (corruptible) 'checksum_algorithm' field
[[nodiscard]] gs::Generator<sample_event> checksummed_non_fde_events() {
  const auto checksums{gs::just(true)};
  return gs::one_of({rotate_events(checksums),
                     previous_gtids_log_events(checksums),
                     gtid_log_events(checksums),
                     anonymous_gtid_log_events(checksums),
                     gtid_tagged_log_events(checksums), stop_events(checksums)});
}

void require_round_trip(const sample_event &sample) {
  const auto context{
      make_context(sample.encoded_server_version, sample.with_checksum)};
  const events::event_view parsed_view{context,
                                       util::const_byte_span{sample.buffer}};
  require(parsed_view.get_total_size() ==
              sample.generated.calculate_encoded_size(),
          "calculate_encoded_size() does not match the encoded event size");
  const events::event parsed{parsed_view};
  require(parsed == sample.generated,
          "parsed event differs from the generated one:\n" + to_string(parsed));
}

// ---------------------------------------------------------------------------
// corrupting encoded events
// ---------------------------------------------------------------------------

struct byte_corruption {
  std::size_t position;
  std::uint8_t mask;
};

// the event size in the common header and the CRC32 in the footer are
// optionally fixed up after the corruption, so that the parsers get past
// the framing checks and have to deal with the corrupted content
struct corruption_plan {
  std::vector<byte_corruption> corruptions;
  std::optional<std::size_t> new_size;
  bool fix_event_size;
  bool fix_checksum;
};

std::ostream &operator<<(std::ostream &output, const corruption_plan &plan) {
  output << "xor {";
  for (const auto &[position, mask] : plan.corruptions) {
    output << ' ' << position << ":0x" << std::hex
           << static_cast<unsigned int>(mask) << std::dec;
  }
  output << " }";
  if (plan.new_size.has_value()) {
    output << ", resize to " << *plan.new_size;
  }
  return output << (plan.fix_event_size ? ", fix event size" : "")
                << (plan.fix_checksum ? ", fix checksum" : "");
}

[[nodiscard]] gs::Generator<corruption_plan>
corruption_plans(std::size_t original_size) {
  return gs::compose([original_size](const hegel::TestCase &tc) {
    static constexpr std::size_t max_corruptions{8U};
    static constexpr std::size_t max_extra_bytes{64U};
    corruption_plan plan{};
    const auto number_of_corruptions{tc.draw(gs::integers<std::size_t>(
        {.min_value = 0U, .max_value = max_corruptions}))};
    for (std::size_t index{0U}; index < number_of_corruptions; ++index) {
      plan.corruptions.push_back(
          {.position = tc.draw(gs::integers<std::size_t>(
               {.min_value = 0U, .max_value = original_size - 1U})),
           .mask = tc.draw(gs::integers<std::uint8_t>(
               {.min_value = 1U, .max_value = 255U}))});
    }
    if (tc.draw(gs::booleans())) {
      plan.new_size = tc.draw(gs::integers<std::size_t>(
          {.min_value = 0U, .max_value = original_size + max_extra_bytes}));
    }
    plan.fix_event_size = tc.draw(gs::booleans());
    plan.fix_checksum = tc.draw(gs::booleans());
    return plan;
  });
}

[[nodiscard]] std::vector<std::byte> apply_corruption(const sample_event &sample,
                                                      const corruption_plan &plan) {
  std::vector<std::byte> result(std::cbegin(sample.buffer),
                                std::cend(sample.buffer));
  for (const auto &[position, mask] : plan.corruptions) {
    result.at(position) ^= std::byte{mask};
  }
  if (plan.new_size.has_value()) {
    result.resize(*plan.new_size, std::byte{0xAB});
  }
  const auto size{std::size(result)};
  if (plan.fix_event_size && size >= events::default_common_header_length) {
    // writing the field directly: common_header_updatable_view would
    // validate (and reject) a corrupted type code
    util::byte_span event_size_field{
        std::next(std::data(result),
                  static_cast<std::ptrdiff_t>(
                      events::common_header_view_base::event_size_offset)),
        sizeof(std::uint32_t)};
    util::insert_fixed_int_to_byte_span(event_size_field,
                                        static_cast<std::uint32_t>(size));
  }
  if (plan.fix_checksum && sample.with_checksum &&
      size >= events::default_common_header_length +
                  events::default_footer_length) {
    const auto payload_size{size - events::default_footer_length};
    const events::footer_updatable_view footer_uv{util::byte_span{
        std::next(std::data(result), static_cast<std::ptrdiff_t>(payload_size)),
        events::default_footer_length}};
    footer_uv.set_crc_raw(util::calculate_crc32(
        util::const_byte_span{std::data(result), payload_size}));
  }
  return result;
}

// parses 'bytes' treating the rejection exceptions used by the parsers
// (std::logic_error and its descendants) as a clean rejection; any other
// exception propagates and fails the property
[[nodiscard]] std::optional<events::event>
parse_or_reject(const events::reader_context &context,
                const std::vector<std::byte> &bytes) {
  try {
    const events::event_view view{context, util::const_byte_span{bytes}};
    return events::event{view};
  } catch (const std::logic_error &) {
    return std::nullopt;
  }
}

[[nodiscard]] gs::Generator<std::vector<std::byte>> arbitrary_bytes() {
  static constexpr std::size_t max_size{256U};
  return gs::binary({.min_size = 0U, .max_size = max_size})
      .map([](const std::vector<std::uint8_t> &raw) {
        std::vector<std::byte> result(std::size(raw));
        std::ranges::transform(
            raw, std::begin(result),
            [](std::uint8_t value) { return std::byte{value}; });
        return result;
      });
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// round trips
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(EventRotateRoundTrip) {
  run_property([](hegel::TestCase &tc) {
    require_round_trip(tc.draw("sample", rotate_events(gs::booleans())));
  });
}

BOOST_AUTO_TEST_CASE(EventFormatDescriptionRoundTrip) {
  run_property([](hegel::TestCase &tc) {
    require_round_trip(tc.draw("sample", format_description_events()));
  });
}

BOOST_AUTO_TEST_CASE(EventPreviousGtidsLogRoundTrip) {
  run_property([](hegel::TestCase &tc) {
    require_round_trip(
        tc.draw("sample", previous_gtids_log_events(gs::booleans())));
  });
}

BOOST_AUTO_TEST_CASE(EventGtidLogRoundTrip) {
  run_property([](hegel::TestCase &tc) {
    require_round_trip(tc.draw("sample", gtid_log_events(gs::booleans())));
  });
}

BOOST_AUTO_TEST_CASE(EventAnonymousGtidLogRoundTrip) {
  run_property([](hegel::TestCase &tc) {
    require_round_trip(
        tc.draw("sample", anonymous_gtid_log_events(gs::booleans())));
  });
}

BOOST_AUTO_TEST_CASE(EventGtidTaggedLogRoundTrip) {
  run_property([](hegel::TestCase &tc) {
    require_round_trip(
        tc.draw("sample", gtid_tagged_log_events(gs::booleans())));
  });
}

BOOST_AUTO_TEST_CASE(EventStopRoundTrip) {
  run_property([](hegel::TestCase &tc) {
    require_round_trip(tc.draw("sample", stop_events(gs::booleans())));
  });
}

// ---------------------------------------------------------------------------
// robustness of parsing untrusted bytes
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(EventParsingOfArbitraryBytesFailsCleanly) {
  run_property([](hegel::TestCase &tc) {
    const auto version{draw_server_version(tc, false)};
    const auto with_checksum{tc.draw("with_checksum", gs::booleans())};
    const auto bytes{tc.draw("bytes", arbitrary_bytes())};
    [[maybe_unused]] const auto parsed{
        parse_or_reject(make_context(version.get_encoded(), with_checksum),
                        bytes)};
  });
}

BOOST_AUTO_TEST_CASE(EventParsingOfCorruptedEventsFailsCleanly) {
  run_property([](hegel::TestCase &tc) {
    const auto sample{tc.draw("sample", any_events())};
    const auto plan{
        tc.draw("plan", corruption_plans(std::size(sample.buffer)))};
    [[maybe_unused]] const auto parsed{parse_or_reject(
        make_context(sample.encoded_server_version, sample.with_checksum),
        apply_corruption(sample, plan))};
  });
}

// parsed events are written to the log (operator<<); some bodies (e.g. the
// GTID set of PREVIOUS_GTIDS_LOG) are decoded lazily, only when printed or
// accessed, so printing may reject corrupted content, but it must do that
// the same clean way as parsing
BOOST_AUTO_TEST_CASE(EventPrintingOfCorruptedEventsFailsCleanly) {
  run_property([](hegel::TestCase &tc) {
    const auto sample{tc.draw("sample", any_events())};
    const auto plan{
        tc.draw("plan", corruption_plans(std::size(sample.buffer)))};
    const auto parsed{parse_or_reject(
        make_context(sample.encoded_server_version, sample.with_checksum),
        apply_corruption(sample, plan))};
    if (!parsed.has_value()) {
      return;
    }
    try {
      [[maybe_unused]] const auto text{to_string(*parsed)};
    } catch (const std::logic_error &) {
      // clean rejection of lazily decoded content
    }
  });
}

// ---------------------------------------------------------------------------
// checksums
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(EventChecksumDetectsSingleBitFlip) {
  run_property([](hegel::TestCase &tc) {
    const auto sample{tc.draw("sample", checksummed_non_fde_events())};
    static constexpr std::size_t bits_per_byte{8U};
    const auto bit_index{tc.draw(
        "bit_index",
        gs::integers<std::size_t>(
            {.min_value = 0U,
             .max_value = std::size(sample.buffer) * bits_per_byte - 1U}))};

    std::vector<std::byte> corrupted(std::cbegin(sample.buffer),
                                     std::cend(sample.buffer));
    corrupted.at(bit_index / bits_per_byte) ^=
        std::byte{static_cast<std::uint8_t>(1U << (bit_index % bits_per_byte))};

    const auto parsed{parse_or_reject(
        make_context(sample.encoded_server_version, true), corrupted)};
    if (parsed.has_value()) {
      throw std::runtime_error{
          "a single bit flip was not detected, the event was parsed as:\n" +
          to_string(*parsed)};
    }
  });
}

// ---------------------------------------------------------------------------
// HEARTBEAT_LOG_EVENT_V2 body decoding (PBS-30)
// ---------------------------------------------------------------------------

namespace {

using heartbeat_body =
    events::generic_body_impl<code_type::heartbeat_log_v2>;
using byte_list = std::vector<std::uint8_t>;

// the packed integer encodings of 'value': the shortest one, and a wider one
// (which the decoder accepts as well); 'width' 0 picks the shortest
[[nodiscard]] byte_list packed(std::uint64_t value, int width = 0) {
  static constexpr std::uint64_t single_limit{251U};
  byte_list result;
  const auto append_le{[&result](std::uint64_t number, std::size_t bytes) {
    for (std::size_t index{0U}; index < bytes; ++index) {
      result.push_back(static_cast<std::uint8_t>(number >> (8U * index)));
    }
  }};
  if (width == 0) {
    width = value < single_limit ? 1 : value < (1ULL << 16U) ? 2
            : value < (1ULL << 24U)                         ? 3
                                                            : 8;
  }
  switch (width) {
  case 1:
    result.push_back(static_cast<std::uint8_t>(value));
    break;
  case 2:
    result.push_back(0xFCU);
    append_le(value, 2U);
    break;
  case 3:
    result.push_back(0xFDU);
    append_le(value, 3U);
    break;
  default:
    result.push_back(0xFEU);
    append_le(value, 8U);
    break;
  }
  return result;
}

// the TLV decoding of MySQL's Heartbeat::decode (libs/mysql/binlog/event/
// codecs/binary.cpp), written independently of the decoder under test: a
// sequence of <type> <length> <value> triples of packed integers up to the
// end marker (type 0) or the end of the data; type 1 is the log file name
// (<length> bytes), type 2 the log position (a packed integer read after
// <length>, which is not used), any other type is skipped by <length>.
// Packed integers: below 251 one byte, 0xFC / 0xFD / 0xFE then 2 / 3 / 8
// little-endian bytes, 0xFB the maximum value, 0xFF invalid.
struct heartbeat_model_result {
  bool rejected;
  std::string binlog;
  std::uint64_t position;
};

[[nodiscard]] heartbeat_model_result decode_heartbeat_model(const byte_list &body) {
  std::size_t offset{0U};
  const auto read_packed{[&body, &offset]() -> std::optional<std::uint64_t> {
    if (offset >= std::size(body)) {
      return std::nullopt;
    }
    const auto first{body[offset++]};
    std::size_t bytes{0U};
    switch (first) {
    case 0xFBU:
      return std::numeric_limits<std::uint64_t>::max();
    case 0xFCU:
      bytes = 2U;
      break;
    case 0xFDU:
      bytes = 3U;
      break;
    case 0xFEU:
      bytes = 8U;
      break;
    case 0xFFU:
      return std::nullopt;
    default:
      return first;
    }
    if (std::size(body) - offset < bytes) {
      return std::nullopt;
    }
    std::uint64_t value{0U};
    for (std::size_t index{0U}; index < bytes; ++index) {
      value |= static_cast<std::uint64_t>(body[offset++]) << (8U * index);
    }
    return value;
  }};
  heartbeat_model_result result{.rejected = false, .binlog = {}, .position = 0U};
  while (offset < std::size(body)) {
    const auto type{read_packed()};
    if (!type.has_value()) {
      result.rejected = true;
      return result;
    }
    if (*type == 0U) {
      break;
    }
    const auto length{read_packed()};
    if (!length.has_value()) {
      result.rejected = true;
      return result;
    }
    if (*type == 2U) {
      const auto position{read_packed()};
      if (!position.has_value()) {
        result.rejected = true;
        return result;
      }
      result.position = *position;
      continue;
    }
    if (*length > std::size(body) - offset) {
      result.rejected = true;
      return result;
    }
    if (*type == 1U) {
      result.binlog.assign(
          reinterpret_cast<const char *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
              std::next(std::data(body), static_cast<std::ptrdiff_t>(offset))),
          *length);
    }
    offset += *length;
  }
  return result;
}

// a heartbeat body built from its fields: the log file name, the position
// (with its declared length right or wrong), unknown fields, an optional end
// marker and trailing bytes after it, packed integers in their shortest or a
// wider encoding; sometimes corrupted (cut short, a byte replaced by a packed
// integer prefix, or an inflated length)
[[nodiscard]] gs::Generator<byte_list> heartbeat_bodies() {
  return gs::compose([](const hegel::TestCase &tc) {
    const auto width{[&tc] {
      return tc.draw(gs::sampled_from(std::vector<int>{0, 0, 0, 2, 3, 8}));
    }};
    byte_list body;
    const auto append{[&body](const byte_list &bytes) {
      body.insert(std::end(body), std::begin(bytes), std::end(bytes));
    }};
    const auto fields{tc.draw(gs::integers<std::size_t>(
        {.min_value = 0U, .max_value = 5U}))};
    for (std::size_t field{0U}; field < fields; ++field) {
      switch (tc.draw(gs::integers<int>({.min_value = 0, .max_value = 2}))) {
      case 0: {
        // a binlog name, or any bytes (a name on the wire is just bytes)
        std::string name;
        if (tc.draw(gs::booleans())) {
          name = tc.draw(gs::from_regex("binlog\\.[0-9]{6}", true));
        } else {
          for (const auto value : tc.draw(gs::vectors(
                   gs::integers<std::uint8_t>(), {.max_size = 70U}))) {
            name.push_back(static_cast<char>(value));
          }
        }
        append(packed(1U, width()));
        append(packed(std::size(name), width()));
        body.insert(std::end(body), std::begin(name), std::end(name));
      } break;
      case 1: {
        const auto position{tc.draw(gs::one_of(
            {gs::integers<std::uint64_t>({.min_value = 0U, .max_value = 300U}),
             gs::integers<std::uint64_t>()}))};
        const auto value{packed(position, width())};
        const bool wrong_length{tc.draw(gs::booleans())};
        append(packed(2U, width()));
        append(packed(wrong_length ? tc.draw(gs::integers<std::uint64_t>(
                                         {.min_value = 0U, .max_value = 20U}))
                                   : std::size(value),
                      width()));
        append(value);
      } break;
      default: {
        const auto type{tc.draw(gs::integers<std::uint64_t>(
            {.min_value = 3U, .max_value = 1U << 20U}))};
        const auto value{tc.draw(gs::vectors(
            gs::integers<std::uint8_t>(), {.max_size = 40U}))};
        append(packed(type, width()));
        append(packed(std::size(value), width()));
        append(value);
      } break;
      }
    }
    if (tc.draw(gs::booleans())) {
      append(packed(0U, width()));
      append(tc.draw(gs::vectors(gs::integers<std::uint8_t>(),
                                 {.max_size = 8U})));
    }
    switch (tc.draw(gs::integers<int>({.min_value = 0, .max_value = 5}))) {
    case 0:
      if (!body.empty()) {
        body.resize(tc.draw(gs::integers<std::size_t>(
            {.min_value = 0U, .max_value = std::size(body) - 1U})));
      }
      break;
    case 1:
      if (!body.empty()) {
        body[tc.draw(gs::integers<std::size_t>(
            {.min_value = 0U, .max_value = std::size(body) - 1U}))] =
            tc.draw(gs::sampled_from(
                std::vector<std::uint8_t>{0xFBU, 0xFCU, 0xFDU, 0xFEU, 0xFFU}));
      }
      break;
    default:
      break;
    }
    return body;
  });
}

[[nodiscard]] std::string describe_bytes(const byte_list &bytes) {
  static constexpr std::string_view digits{"0123456789abcdef"};
  std::string result;
  for (const auto byte : bytes) {
    result += digits[byte >> 4U];
    result += digits[byte & 0x0FU];
  }
  return result;
}

} // namespace

// a HEARTBEAT_LOG_EVENT_V2 body (PBS-30) decodes as MySQL decodes it, over
// generated field sequences and corruptions of them: the decoder throws
// std::invalid_argument exactly when the model rejects the body (a truncated
// or invalid packed integer, a file name or unknown field longer than the
// rest of the body), and otherwise reports the same file name and position,
// skipping unknown fields and tolerating a missing end marker
BOOST_AUTO_TEST_CASE(HeartbeatLogV2BodyDecodesLikeMysql) {
  run_property([](hegel::TestCase &tc) {
    const auto body{tc.draw("body", heartbeat_bodies())};
    const auto expected{decode_heartbeat_model(body)};
    const auto label{"body " + describe_bytes(body)};
    std::vector<std::byte> bytes(std::size(body));
    std::ranges::transform(body, std::begin(bytes), [](std::uint8_t value) {
      return static_cast<std::byte>(value);
    });
    try {
      const heartbeat_body decoded{util::const_byte_span{bytes}};
      require(!expected.rejected,
              label + ": accepted, but MySQL's decoding rejects it");
      require(decoded.get_readable_binlog() == expected.binlog,
              label + ": file name '" +
                  std::string{decoded.get_readable_binlog()} + "', expected '" +
                  expected.binlog + "'");
      require(decoded.get_position() == expected.position,
              label + ": position " + std::to_string(decoded.get_position()) +
                  ", expected " + std::to_string(expected.position));
    } catch (const std::invalid_argument &) {
      require(expected.rejected,
              label + ": rejected, but MySQL's decoding accepts it (file name '" +
                  expected.binlog + "', position " +
                  std::to_string(expected.position) + ")");
    }
  });
}
