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

// Property-based tests for operations::sender_context, the COM_BINLOG_DUMP
// streamer of the replication source listener ('pull' mode), built with Hegel
// (https://hegel.dev). sender_context was reworked into a state machine
// (PBS-42) that fetches the real FORMAT_DESCRIPTION from the data files and
// streams:
//
//   from the beginning (position == magic offset 4):
//     an artificial ROTATE (names the binlog, position 4, 'artificial' flag,
//       timestamp and next_event_position 0, checksum per the session's
//       @source_binlog_checksum) -> the real FORMAT_DESCRIPTION as-is -> the
//       remaining stored events;
//   from a position (position != 4, position-based resume):
//     an artificial ROTATE (position set to the requested one) -> an
//       artificial FORMAT_DESCRIPTION derived from the real one
//       (next_event_position zeroed) -> the stored events from that position.
//
// The MTR suite exercises this end to end against a live server; here a
// storage is seeded with a real FDE followed by synthetic events, and the
// emitted stream is checked event by event (the artificial preamble by its
// parsed header / position / checksum, the data events byte for byte against
// what was stored).

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#define BOOST_TEST_MODULE SenderContextPropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "property_test_helpers.hpp"

#include "operations/event_generation_helpers.hpp"
#include "operations/sender_context.hpp"

#include "binsrv/main_config.hpp"
#include "binsrv/null_logger.hpp"
#include "binsrv/storage.hpp"
#include "binsrv/storage_core.hpp"

#include "binsrv/gtids/gtid.hpp"

#include "binsrv/events/code_type.hpp"
#include "binsrv/events/common_header_flag_type.hpp"
#include "binsrv/events/common_header_view.hpp"
#include "binsrv/events/composite_binlog_name.hpp"
#include "binsrv/events/event_fwd.hpp"
#include "binsrv/events/generic_post_header.hpp"
#include "binsrv/events/protocol_traits_fwd.hpp"
#include "binsrv/events/rotate_post_header_impl.hpp"

#include "util/byte_span.hpp"
#include "util/byte_span_fwd.hpp"
#include "util/byte_span_inserters.hpp"
#include "util/ctime_timestamp.hpp"
#include "util/semantic_version.hpp"

namespace {

namespace gs = hegel::generators;
namespace events = binsrv::events;

using events::code_type;

using property_testing::require;
using property_testing::run_property;

const util::semantic_version server_version{8U, 4U, 8U};
constexpr std::uint32_t server_id{42U};
const events::composite_binlog_name the_binlog{"binlog", 1U};

using byte_vector = std::vector<std::byte>;

// a unique temporary directory removed when the test case ends
class temp_dir {
public:
  temp_dir() {
    static std::atomic<std::uint64_t> counter{0ULL};
    path_ = std::filesystem::temp_directory_path() /
            ("sender_ctx_" + std::to_string(counter.fetch_add(1ULL)));
    std::filesystem::create_directories(path_);
  }
  temp_dir(const temp_dir &) = delete;
  temp_dir &operator=(const temp_dir &) = delete;
  temp_dir(temp_dir &&) = delete;
  temp_dir &operator=(temp_dir &&) = delete;
  ~temp_dir() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path &path() const { return path_; }

private:
  std::filesystem::path path_;
};

// a minimal filesystem-backed, position-mode storage configuration
[[nodiscard]] std::filesystem::path
write_config(const std::filesystem::path &directory) {
  const auto storage_directory{directory / "storage"};
  std::filesystem::create_directories(storage_directory);
  const auto config_path{directory / "config.json"};
  std::ofstream config{config_path};
  config << R"({
  "logger": { "level": "error", "file": "" },
  "connection": {
    "host": "127.0.0.1", "port": 3306, "user": "root", "password": "",
    "connect_timeout": 20, "read_timeout": 60, "write_timeout": 60
  },
  "replication": {
    "server_id": 42, "idle_time": 10, "verify_checksum": true,
    "mode": "position"
  },
  "replication_source": {
    "port": 3307, "read_timeout": 60, "write_timeout": 60,
    "authentication": {
      "user": "rpl", "password": "password", "plugin": "caching_sha2_password"
    }
  },
  "storage": { "backend": "file", "uri": "file://)"
         << storage_directory.generic_string() << R"(" }
})";
  config.close();
  if (!config) {
    throw std::runtime_error{"cannot write the configuration file"};
  }
  return config_path;
}

[[nodiscard]] binsrv::storage_ptr
make_storage(const std::filesystem::path &config_path) {
  const binsrv::main_config config{config_path.string()};
  return std::make_shared<binsrv::storage>(
      std::make_shared<binsrv::null_logger>(), config,
      binsrv::storage_construction_mode_type::streaming);
}

// a real FORMAT_DESCRIPTION event, the first event of a binlog (offset 4)
[[nodiscard]] byte_vector make_fde() {
  events::event_storage buffer;
  operations::generate_format_description_event_ex(
      buffer, server_version, events::magic_binlog_offset, server_id,
      /*enable_checksum_algorithm=*/true, /*artificial=*/false);
  return byte_vector{std::cbegin(buffer), std::cend(buffer)};
}

// a synthetic event with a valid common header (type code and event size,
// which is all the streaming side relies on) and distinct content
[[nodiscard]] byte_vector make_event(std::size_t size, std::uint32_t id) {
  using header = events::common_header_view_base;
  byte_vector result(size);
  for (std::size_t index{0U}; index != size; ++index) {
    result[index] =
        static_cast<std::byte>(((id * 131ULL) + (index * 7ULL)) & 0xFFULL);
  }
  const auto write_field{[&result](std::size_t offset, auto value) {
    util::byte_span field{std::next(std::data(result),
                                    static_cast<std::ptrdiff_t>(offset)),
                          sizeof value};
    util::insert_fixed_int_to_byte_span(field, value);
  }};
  write_field(header::type_code_offset,
              static_cast<std::uint8_t>(code_type::query));
  write_field(header::event_size_offset, static_cast<std::uint32_t>(size));
  return result;
}

struct seeded_storage {
  binsrv::storage_ptr storage;
  // [FDE, event_1, event_2, ...] exactly as written to disk
  std::vector<byte_vector> events;
  // the start offset of each entry in 'events'
  std::vector<std::uint64_t> offsets;
};

// writes a single binlog: a real FDE followed by synthetic events of the given
// sizes, flushed to disk
[[nodiscard]] seeded_storage seed(const std::filesystem::path &directory,
                                  const std::vector<std::size_t> &event_sizes) {
  seeded_storage result{
      .storage = make_storage(write_config(directory)), .events = {},
      .offsets = {}};
  require(result.storage->open_binlog(the_binlog) ==
              binsrv::open_binlog_status::created,
          "the binlog was not created");

  const binsrv::gtids::gtid no_gtid{};
  std::uint64_t offset{events::magic_binlog_offset};
  const auto store{[&](byte_vector bytes) {
    result.offsets.push_back(offset);
    offset += std::size(bytes);
    result.storage->write_event(util::const_byte_span{bytes},
                                /*at_transaction_boundary=*/true, no_gtid,
                                util::ctime_timestamp{std::time_t{1}}, 0ULL);
    result.events.push_back(std::move(bytes));
  }};

  store(make_fde());
  std::uint32_t id{1U};
  for (const auto size : event_sizes) {
    store(make_event(size, id++));
  }
  result.storage->flush_event_buffer();
  return result;
}

// reads the whole stream a sender_context produces (get_event returns true and
// an empty span at EOF, false on error)
[[nodiscard]] std::vector<byte_vector>
drain(operations::sender_context &reader, std::size_t limit) {
  std::vector<byte_vector> result;
  util::const_byte_span event;
  for (std::size_t guard{0U}; guard != limit + 1U; ++guard) {
    require(reader.get_event(event), "sender_context::get_event() failed");
    if (std::empty(event)) {
      return result;
    }
    result.emplace_back(std::cbegin(event), std::cend(event));
  }
  throw std::runtime_error{"sender_context produced more events than expected"};
}

[[nodiscard]] events::common_header_view header_of(const byte_vector &bytes) {
  return events::common_header_view{util::const_byte_span{bytes}.subspan(
      0U, events::common_header_view_base::size_in_bytes)};
}

// the ROTATE position field (the post header follows the common header)
[[nodiscard]] std::uint64_t rotate_position(const byte_vector &bytes) {
  const util::const_byte_span post_header{
      util::const_byte_span{bytes}.subspan(
          events::common_header_view_base::size_in_bytes,
          events::generic_post_header<code_type::rotate>::size_in_bytes)};
  return events::generic_post_header<code_type::rotate>{post_header}
      .get_position_raw();
}

// whether an artificial ROTATE carries a CRC32 footer, from its total size:
// common header + rotate post header + the binlog name + optional footer
[[nodiscard]] bool rotate_has_checksum(const byte_vector &bytes) {
  const auto without_footer{events::common_header_view_base::size_in_bytes +
                            events::generic_post_header<
                                code_type::rotate>::size_in_bytes +
                            the_binlog.str().size()};
  return std::size(bytes) == without_footer + 4U;
}

[[nodiscard]] gs::Generator<std::vector<std::size_t>> event_size_lists() {
  return gs::vectors(gs::integers<std::size_t>(
      {.min_value = events::common_header_view_base::size_in_bytes,
       .max_value = 200U}));
}

[[nodiscard]] gs::Generator<std::size_t> block_sizes() {
  // the reader needs at least a full common header per block
  return gs::integers<std::size_t>(
      {.min_value = events::common_header_view_base::size_in_bytes,
       .max_value = 512U});
}

BOOST_AUTO_TEST_CASE(FromBeginningStreamsPreambleAndEvents) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    const auto sizes{tc.draw("sizes", event_size_lists())};
    const bool session_checksum{tc.draw("checksum", gs::booleans())};
    const auto block_size{tc.draw("block", block_sizes())};

    const auto seeded{seed(directory.path(), sizes)};
    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(), seeded.storage, block_size,
        std::string_view{}, events::magic_binlog_offset, session_checksum};

    const auto emitted{drain(reader, std::size(seeded.events) + 1U)};

    // artificial ROTATE + the stored events (FDE first)
    require(std::size(emitted) == std::size(seeded.events) + 1U,
            "expected an artificial ROTATE followed by every stored event");

    const auto rotate_header{header_of(emitted.front())};
    require(rotate_header.get_type_code() == code_type::rotate,
            "the first emitted event must be a ROTATE");
    require(rotate_header.get_flags().has_element(
                events::common_header_flag_type::artificial),
            "the leading ROTATE must be artificial");
    require(rotate_header.get_timestamp_raw() == 0U,
            "the artificial ROTATE must have a zero timestamp");
    require(rotate_header.get_next_event_position_raw() == 0U,
            "the artificial ROTATE must have a zero next_event_position");
    require(rotate_position(emitted.front()) == events::magic_binlog_offset,
            "a from-beginning ROTATE must point at the magic offset");
    require(rotate_has_checksum(emitted.front()) == session_checksum,
            "the artificial ROTATE's checksum must follow the session setting");

    // from the beginning the FDE and all following events are streamed as-is
    require(header_of(emitted[1U]).get_type_code() ==
                code_type::format_description,
            "the event after the ROTATE must be the FORMAT_DESCRIPTION");
    for (std::size_t index{0U}; index != std::size(seeded.events); ++index) {
      require(emitted[index + 1U] == seeded.events[index],
              "stored event " + std::to_string(index) +
                  " was not streamed byte for byte");
    }
  });
}

BOOST_AUTO_TEST_CASE(FromPositionStreamsArtificialPreambleAndRemainingEvents) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    // at least one synthetic event, so there is a non-beginning boundary
    auto sizes{tc.draw("sizes", event_size_lists())};
    if (sizes.empty()) {
      sizes.push_back(events::common_header_view_base::size_in_bytes);
    }
    const bool session_checksum{tc.draw("checksum", gs::booleans())};
    const auto block_size{tc.draw("block", block_sizes())};

    const auto seeded{seed(directory.path(), sizes)};
    // resume at the boundary of one of the synthetic events (index 1..N;
    // index 0 is the FDE, which is the from-beginning case)
    const auto resume_index{tc.draw(
        "resume",
        gs::integers<std::size_t>({.min_value = 1U,
                                   .max_value = std::size(seeded.events) - 1U}))};
    const auto resume_position{seeded.offsets[resume_index]};

    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(), seeded.storage, block_size,
        std::string_view{the_binlog.str()}, resume_position, session_checksum};

    const auto remaining{std::size(seeded.events) - resume_index};
    const auto emitted{drain(reader, remaining + 2U)};

    // artificial ROTATE + artificial FDE + the events from the position on
    require(std::size(emitted) == remaining + 2U,
            "expected an artificial ROTATE and FDE then the remaining events");

    const auto rotate_header{header_of(emitted[0U])};
    require(rotate_header.get_type_code() == code_type::rotate,
            "the first emitted event must be a ROTATE");
    require(rotate_header.get_flags().has_element(
                events::common_header_flag_type::artificial),
            "the leading ROTATE must be artificial");
    require(rotate_position(emitted[0U]) == resume_position,
            "a from-position ROTATE must carry the requested position");
    require(rotate_has_checksum(emitted[0U]) == session_checksum,
            "the artificial ROTATE's checksum must follow the session setting");

    // the FDE here is artificial: derived from the real one with a zero
    // next_event_position, not the stored bytes
    const auto fde_header{header_of(emitted[1U])};
    require(fde_header.get_type_code() == code_type::format_description,
            "the event after the ROTATE must be a FORMAT_DESCRIPTION");
    require(fde_header.get_next_event_position_raw() == 0U,
            "the artificial FDE must have a zero next_event_position");

    // the remaining events are the stored ones from the resume index on
    for (std::size_t index{0U}; index != remaining; ++index) {
      require(emitted[index + 2U] == seeded.events[resume_index + index],
              "stored event " + std::to_string(resume_index + index) +
                  " was not streamed byte for byte from the position");
    }
  });
}

} // namespace
