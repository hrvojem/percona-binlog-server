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

// Property-based tests for binsrv::indexed_event_block, built with Hegel
// (https://hegel.dev).
//
// indexed_event_block splits a block of bytes read from a binlog file into
// events, using the event size from each common header, and keeps only
// complete events. Its input comes from storage files, which can be
// truncated (a crash) or corrupted, so for any bytes it must either reject
// them cleanly (std::logic_error and its descendants, as the event parsers
// do) or produce an index that is consistent with the bytes.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <vector>

#define BOOST_TEST_MODULE IndexedEventBlockPropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "property_test_helpers.hpp"

#include "binsrv/indexed_event_block.hpp"

#include "binsrv/events/common_header_view.hpp"

#include "util/byte_span_extractors.hpp"
#include "util/byte_span_fwd.hpp"
#include "util/byte_span_inserters.hpp"
#include "util/dynamic_byte_buffer_fwd.hpp"

namespace {

namespace gs = hegel::generators;

using property_testing::require;
using property_testing::run_property;

using header = binsrv::events::common_header_view_base;

// raw type codes accepted by the common header parser
[[nodiscard]] const std::vector<std::uint8_t> &valid_type_codes() {
  static const std::vector<std::uint8_t> codes{[] {
    std::vector<std::uint8_t> result;
    std::vector<std::byte> probe(header::size_in_bytes);
    for (unsigned int code{0U}; code <= 0xFFU; ++code) {
      probe[header::type_code_offset] =
          static_cast<std::byte>(static_cast<std::uint8_t>(code));
      try {
        [[maybe_unused]] const binsrv::events::common_header_view view{
            util::const_byte_span{probe}};
        result.push_back(static_cast<std::uint8_t>(code));
      } catch (const std::logic_error &) {
        // not a valid type code
      }
    }
    return result;
  }()};
  return codes;
}

// a piece of the buffer that looks like an event: a common header followed
// by 'body_size' bytes, whose 'event_size' field may or may not match the
// actual size
struct event_chunk {
  std::uint8_t type_code;
  std::uint32_t event_size_field;
  std::size_t body_size;
};

std::ostream &operator<<(std::ostream &output, const event_chunk &chunk) {
  return output << "{type " << static_cast<unsigned int>(chunk.type_code)
                << ", event_size " << chunk.event_size_field << ", "
                << header::size_in_bytes + chunk.body_size << " bytes}";
}

[[nodiscard]] gs::Generator<event_chunk> event_chunks() {
  return gs::compose([](const hegel::TestCase &tc) {
    static constexpr std::size_t max_body_size{64U};
    const auto body_size{tc.draw(gs::integers<std::size_t>(
        {.min_value = 0U, .max_value = max_body_size}))};
    const auto actual_size{
        static_cast<std::uint32_t>(header::size_in_bytes + body_size)};
    const auto type_code{tc.draw(gs::one_of(
        {gs::sampled_from(valid_type_codes()),
         gs::integers<std::uint8_t>(
             {.min_value = 0U, .max_value = 0xFFU})}))};
    // mostly well-formed events, plus sizes that are too small for a common
    // header (including 0), slightly off or arbitrary
    const auto event_size_field{tc.draw(gs::one_of(
        {gs::just(actual_size), gs::just(actual_size),
         gs::integers<std::uint32_t>(
             {.min_value = 0U,
              .max_value = static_cast<std::uint32_t>(header::size_in_bytes)}),
         gs::integers<std::uint32_t>(
             {.min_value = actual_size == 0U ? 0U : actual_size - 1U,
              .max_value = actual_size + 1U}),
         gs::integers<std::uint32_t>(
             {.min_value = 0U, .max_value = 0xFFFFFFFFU})}))};
    return event_chunk{.type_code = type_code,
                       .event_size_field = event_size_field,
                       .body_size = body_size};
  });
}

[[nodiscard]] util::dynamic_byte_buffer
build_buffer(const std::vector<event_chunk> &chunks,
             const std::vector<std::uint8_t> &tail) {
  util::dynamic_byte_buffer result;
  for (const auto &chunk : chunks) {
    const auto start{std::size(result)};
    result.resize(start + header::size_in_bytes + chunk.body_size,
                  std::byte{0x5A});
    util::byte_span type_code_field{
        std::next(std::data(result),
                  static_cast<std::ptrdiff_t>(start + header::type_code_offset)),
        sizeof chunk.type_code};
    util::insert_fixed_int_to_byte_span(type_code_field, chunk.type_code);
    util::byte_span event_size_field{
        std::next(std::data(result), static_cast<std::ptrdiff_t>(
                                         start + header::event_size_offset)),
        sizeof chunk.event_size_field};
    util::insert_fixed_int_to_byte_span(event_size_field,
                                        chunk.event_size_field);
  }
  for (const auto value : tail) {
    result.push_back(static_cast<std::byte>(value));
  }
  return result;
}

[[nodiscard]] std::uint32_t read_event_size(const util::dynamic_byte_buffer &buffer,
                                            std::size_t offset) {
  util::const_byte_span field{
      util::const_byte_span{buffer}.subspan(offset + header::event_size_offset,
                                            sizeof(std::uint32_t))};
  std::uint32_t result{};
  util::extract_fixed_int_from_byte_span(field, result);
  return result;
}

} // anonymous namespace

// for any bytes: either a clean rejection, or an index of complete events
// that covers a prefix of the buffer, where whatever follows the prefix is
// not a complete event
BOOST_AUTO_TEST_CASE(IndexedEventBlockIndexesOnlyCompleteEvents) {
  run_property([](hegel::TestCase &tc) {
    static constexpr std::size_t max_tail_size{40U};
    const auto chunks{tc.draw("chunks", gs::vectors(event_chunks()))};
    const auto tail{tc.draw(
        "tail", gs::vectors(gs::integers<std::uint8_t>(),
                            {.min_size = 0U, .max_size = max_tail_size}))};
    const auto original{build_buffer(chunks, tail)};

    std::optional<binsrv::indexed_event_block> block;
    try {
      block.emplace(original);
    } catch (const std::logic_error &) {
      return; // clean rejection
    }

    require(block->get_actual_size() <= std::size(original),
            "the block is larger than the input");
    std::size_t offset{0U};
    for (std::size_t index{0U}; index < block->get_number_of_events();
         ++index) {
      const auto event{block->get_event(index)};
      const auto label{"event " + std::to_string(index + 1U) + " at offset " +
                       std::to_string(offset) + ": "};
      require(std::size(event) >= header::size_in_bytes,
              label + "shorter than a common header (" +
                  std::to_string(std::size(event)) + " bytes)");
      require(read_event_size(original, offset) == std::size(event),
              label + "size differs from its event_size field");
      require(std::equal(std::cbegin(event), std::cend(event),
                         std::next(std::cbegin(original),
                                   static_cast<std::ptrdiff_t>(offset))),
              label + "bytes differ from the input");
      offset += std::size(event);
    }
    require(offset == block->get_actual_size(),
            "the indexed events do not add up to the block size");

    const auto remaining{std::size(original) - offset};
    if (remaining >= header::size_in_bytes) {
      const auto next_event_size{read_event_size(original, offset)};
      require(next_event_size > remaining,
              "a complete event of " + std::to_string(next_event_size) +
                  " bytes at offset " + std::to_string(offset) +
                  " was left out of the index");
    }
  });
}
