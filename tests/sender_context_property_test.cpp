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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#define BOOST_TEST_MODULE SenderContextPropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "property_test_helpers.hpp"

#include "operations/event_generation_helpers.hpp"
#include "binsrv/basic_storage_backend.hpp"
#include "binsrv/storage_backend_factory.hpp"

#include "operations/sender_context.hpp"
#include "operations/sender_single_file_context.hpp"

#include "binsrv/basic_logger.hpp"
#include "binsrv/log_severity.hpp"
#include "binsrv/main_config.hpp"
#include "binsrv/null_logger.hpp"
#include "binsrv/storage.hpp"
#include "binsrv/storage_core.hpp"

#include "binsrv/gtids/gtid.hpp"

#include "binsrv/events/event.hpp"
#include "binsrv/events/generic_body.hpp"
#include "binsrv/events/generic_post_header.hpp"
#include "binsrv/gtids/gtid_set.hpp"
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
const events::composite_binlog_name the_next_binlog{"binlog", 2U};

using byte_vector = std::vector<std::byte>;

// a unique temporary directory removed when the test case ends
class temp_dir {
public:
  temp_dir() {
    // a per-process random token keeps the directory unique even when several
    // test-case processes run concurrently (as in the nightly) under the same
    // temporary root
    static const std::string process_token{[] {
      std::random_device device;
      return std::to_string(device()) + "_" + std::to_string(device());
    }()};
    static std::atomic<std::uint64_t> counter{0ULL};
    path_ = std::filesystem::temp_directory_path() /
            ("sender_ctx_" + process_token + "_" +
             std::to_string(counter.fetch_add(1ULL)));
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
    "port": 3307, "server_id": 142,
    "server_uuid": "00000000-0000-0000-0000-00000000008e",
    "read_timeout": 60, "write_timeout": 60,
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

// the PREVIOUS_GTIDS_LOG event a server writes right after the
// FORMAT_DESCRIPTION (empty GTID set, CRC32 like that FORMAT_DESCRIPTION);
// the sender requires both at the start of every binlog (PBS-29)
[[nodiscard]] std::vector<std::byte>
make_previous_gtids_log(std::uint64_t offset, std::uint32_t event_server_id) {
  binsrv::events::event_storage buffer;
  [[maybe_unused]] const auto event{
      binsrv::events::event::create_event<
          binsrv::events::code_type::previous_gtids_log>(
          static_cast<std::uint32_t>(offset),
          util::ctime_timestamp{std::time_t{1}}, event_server_id,
          binsrv::events::common_header_flag_set{},
          binsrv::events::generic_post_header<
              binsrv::events::code_type::previous_gtids_log>{},
          binsrv::events::generic_body<
              binsrv::events::code_type::previous_gtids_log>{
              binsrv::gtids::gtid_set{}},
          /*include_checksum=*/true, buffer)};
  return {std::cbegin(buffer), std::cend(buffer)};
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
  // [FDE, PREVIOUS_GTIDS_LOG, event_1, ...] exactly as written to disk
  std::vector<byte_vector> events;
  // the start offset of each entry in 'events'
  std::vector<std::uint64_t> offsets;
};

// writes a real FDE and PREVIOUS_GTIDS_LOG followed by synthetic events of
// the given sizes to the
// binlog already open in 'storage', flushed to disk
[[nodiscard]] seeded_storage
fill_open_binlog(binsrv::storage_ptr storage,
                 const std::vector<std::size_t> &event_sizes,
                 std::uint32_t first_id = 1U) {
  seeded_storage result{.storage = std::move(storage), .events = {},
                        .offsets = {}};
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
  store(make_previous_gtids_log(offset, server_id));
  std::uint32_t id{first_id};
  for (const auto size : event_sizes) {
    store(make_event(size, id++));
  }
  result.storage->flush_event_buffer();
  return result;
}

// opens a single binlog in 'storage' and fills it (see 'fill_open_binlog()')
[[nodiscard]] seeded_storage
write_binlog(binsrv::storage_ptr storage,
             const std::vector<std::size_t> &event_sizes) {
  require(storage->open_binlog(the_binlog) ==
              binsrv::open_binlog_status::created,
          "the binlog was not created");
  return fill_open_binlog(std::move(storage), event_sizes);
}

// a storage with a single binlog (see 'write_binlog()')
[[nodiscard]] seeded_storage seed(const std::filesystem::path &directory,
                                  const std::vector<std::size_t> &event_sizes) {
  return write_binlog(make_storage(write_config(directory)), event_sizes);
}

// the offset of each event 'write_binlog()' will store for 'event_sizes', FDE
// and PREVIOUS_GTIDS_LOG first, so a test can pick a position before anything
// is written
[[nodiscard]] std::vector<std::uint64_t>
planned_offsets(const std::vector<std::size_t> &event_sizes) {
  std::vector<std::uint64_t> result{events::magic_binlog_offset};
  result.push_back(result.back() + std::size(make_fde()));
  result.push_back(result.back() +
                   std::size(make_previous_gtids_log(result.back(), server_id)));
  for (const auto size : event_sizes) {
    result.push_back(result.back() + size);
  }
  result.pop_back(); // the end of the last event is not an event offset
  return result;
}

// appends 'bytes' to the open binlog and flushes them; the storage does not
// parse what it stores, so writing only a prefix of an event is how a
// partially written (or truncated) last event becomes visible to readers
void append_flushed(binsrv::storage &storage, util::const_byte_span bytes) {
  storage.write_event(bytes, /*at_transaction_boundary=*/true,
                      binsrv::gtids::gtid{},
                      util::ctime_timestamp{std::time_t{1}}, 0ULL);
  storage.flush_event_buffer();
}

struct stream_outcome {
  std::vector<byte_vector> events;
  // get_event() threw (as opposed to reaching the end of the data)
  bool failed;
};

// reads a sender_context's stream up to the end of the stored data or the
// first error (get_event() returns 'waiting' at the end, throws on an error)
[[nodiscard]] stream_outcome stream_until_end(operations::sender_context &reader,
                                              std::size_t limit) {
  stream_outcome result{.events = {}, .failed = false};
  util::const_byte_span event;
  for (std::size_t guard{0U}; guard != limit + 1U; ++guard) {
    // an error is an exception; 'waiting' (and an empty event) once no
    // more data is stored
    try {
      if (reader.get_event(event) !=
          operations::event_fetching_result_type::success) {
        event = {};
      }
    } catch (const std::exception &) {
      result.failed = true;
      return result;
    }
    if (std::empty(event)) {
      return result;
    }
    result.events.emplace_back(std::cbegin(event), std::cend(event));
  }
  throw std::runtime_error{"sender_context produced more events than expected"};
}

// reads the whole stream a sender_context produces, up to EOF
[[nodiscard]] std::vector<byte_vector>
drain(operations::sender_context &reader, std::size_t limit) {
  auto outcome{stream_until_end(reader, limit)};
  require(!outcome.failed, "sender_context::get_event() failed");
  return std::move(outcome.events);
}

// the byte ranges read from one storage object, recorded by
// 'read_recording_backend' (the storage offers no other way to observe the
// sender's reads)
struct object_reads {
  std::string name;
  std::vector<std::pair<std::uint64_t, std::uint64_t>> ranges;
};

// forwards to the real backend and records every read of 'reads.name'
class read_recording_backend final : public binsrv::basic_storage_backend {
public:
  read_recording_backend(binsrv::basic_storage_backend_ptr inner,
                         object_reads &reads)
      : inner_{std::move(inner)}, reads_{&reads} {}

private:
  binsrv::basic_storage_backend_ptr inner_;
  object_reads *reads_;

  [[nodiscard]] binsrv::storage_object_name_container
  do_list_objects() override {
    return inner_->list_objects();
  }
  [[nodiscard]] util::dynamic_byte_buffer
  do_get_object(std::string_view name, const util::byte_range &range) override {
    auto result{inner_->get_object(name, range)};
    if (name == reads_->name) {
      reads_->ranges.emplace_back(range.get_offset(), std::size(result));
    }
    return result;
  }
  void do_put_object(std::string_view name,
                     util::const_byte_span content) override {
    inner_->put_object(name, content);
  }
  void do_resize_object(std::string_view name,
                        std::uint64_t new_size) override {
    inner_->resize_object(name, new_size);
  }
  void do_remove_object(std::string_view name) override {
    inner_->remove_object(name);
  }
  void do_fsync() override {}
  [[nodiscard]] std::uint64_t
  do_open_stream(std::string_view name,
                 binsrv::storage_backend_open_stream_mode mode) override {
    return inner_->open_stream(name, mode);
  }
  void do_write_data_to_stream(util::const_byte_span data) override {
    inner_->write_data_to_stream(data);
  }
  void do_close_stream() override { inner_->close_stream(); }
  [[nodiscard]] std::string do_get_description() const override {
    return inner_->get_description();
  }
  [[nodiscard]] std::string
  do_get_object_uri(std::string_view name) const override {
    return inner_->get_object_uri(name);
  }
};

// installs a 'read_recording_backend' for every backend the factory creates
// while it lives
class read_recording_scope {
public:
  explicit read_recording_scope(object_reads &reads) {
    binsrv::storage_backend_factory::set_test_decorator(
        [&reads](binsrv::basic_storage_backend_ptr inner)
            -> binsrv::basic_storage_backend_ptr {
          return std::make_unique<read_recording_backend>(std::move(inner),
                                                          reads);
        });
  }
  read_recording_scope(const read_recording_scope &) = delete;
  read_recording_scope &operator=(const read_recording_scope &) = delete;
  read_recording_scope(read_recording_scope &&) = delete;
  read_recording_scope &operator=(read_recording_scope &&) = delete;
  ~read_recording_scope() {
    binsrv::storage_backend_factory::set_test_decorator({});
  }
};

[[nodiscard]] std::string describe_reads(const object_reads &reads) {
  std::string result;
  for (const auto &[offset, length] : reads.ranges) {
    result += (result.empty() ? "" : ", ") + std::to_string(offset) + "+" +
              std::to_string(length);
  }
  return "[" + result + "]";
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

// whether 'bytes' is an artificial ROTATE that names 'binlog_name' at
// 'position' (the name follows the post header, then an optional footer)
[[nodiscard]] bool
is_artificial_rotate_to(const byte_vector &bytes,
                        const events::composite_binlog_name &binlog_name,
                        std::uint64_t position) {
  const auto name_offset{
      events::common_header_view_base::size_in_bytes +
      events::generic_post_header<code_type::rotate>::size_in_bytes};
  const auto name{binlog_name.str()};
  if (std::size(bytes) != name_offset + std::size(name) &&
      std::size(bytes) != name_offset + std::size(name) + 4U) {
    return false;
  }
  const auto header{header_of(bytes)};
  const std::string stored_name(
      reinterpret_cast<const char *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
          std::next(std::data(bytes),
                    static_cast<std::ptrdiff_t>(name_offset))),
      std::size(name));
  return header.get_type_code() == code_type::rotate &&
         header.get_flags().has_element(
             events::common_header_flag_type::artificial) &&
         rotate_position(bytes) == position && stored_name == name;
}

// mostly small events, some larger than any block (block_sizes()), which
// the sender has to fetch again with the event's size
[[nodiscard]] gs::Generator<std::vector<std::size_t>> event_size_lists() {
  static constexpr std::size_t max_large_event_size{
      3U * operations::sender_single_file_context::min_allowed_block_size};
  return gs::vectors(gs::one_of(
      {gs::integers<std::size_t>(
           {.min_value = events::common_header_view_base::size_in_bytes,
            .max_value = 200U}),
       gs::integers<std::size_t>(
           {.min_value = events::common_header_view_base::size_in_bytes,
            .max_value = max_large_event_size})}));
}

// the size of a single event that can be split into a non-empty prefix and a
// non-empty rest
[[nodiscard]] gs::Generator<std::size_t> splittable_event_sizes() {
  return gs::integers<std::size_t>(
      {.min_value = events::common_header_view_base::size_in_bytes,
       .max_value = 200U});
}

[[nodiscard]] gs::Generator<std::size_t> block_sizes() {
  // the sender requires blocks of at least min_allowed_block_size (PBS-29;
  // the listener uses 1 MiB)
  static constexpr auto min_block_size{
      operations::sender_single_file_context::min_allowed_block_size};
  return gs::integers<std::size_t>(
      {.min_value = min_block_size, .max_value = 2U * min_block_size});
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

// every stored byte from the starting position on is fetched from storage
// exactly once (PBS-65): the trailing partial event of a block is carried
// over in memory and topped up, never read again
BOOST_AUTO_TEST_CASE(EveryStoredByteIsFetchedOnce) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    const auto sizes{tc.draw("sizes", event_size_lists())};
    const auto block_size{tc.draw("block", block_sizes())};

    object_reads reads{.name = the_binlog.str(), .ranges = {}};
    const read_recording_scope scope{reads};
    const auto seeded{seed(directory.path(), sizes)};
    // 0 streams from the beginning, any other index resumes at that event
    const auto start_index{tc.draw(
        "start",
        gs::integers<std::size_t>({.min_value = 0U,
                                   .max_value = std::size(seeded.events) - 1U}))};
    const bool from_beginning{start_index == 0U};
    const auto start_position{seeded.offsets[start_index]};
    const std::string binlog_name{from_beginning ? std::string{}
                                                 : the_binlog.str()};

    reads.ranges.clear();
    operations::sender_context reader{std::make_shared<binsrv::null_logger>(),
                                      seeded.storage,
                                      block_size,
                                      binlog_name,
                                      start_position,
                                      /*session_source_binlog_checksum=*/true};

    // artificial ROTATE (+ artificial FDE when resuming) + the stored events
    const auto remaining{std::size(seeded.events) - start_index};
    const auto preamble{from_beginning ? 1U : 2U};
    const auto emitted{drain(reader, remaining + preamble)};
    require(std::size(emitted) == remaining + preamble,
            "expected the preamble followed by every remaining stored event");

    const auto file_size{seeded.offsets.back() +
                         std::size(seeded.events.back())};
    // the first read takes the preamble (FORMAT_DESCRIPTION and
    // PREVIOUS_GTIDS_LOG, PBS-29) and may cover events after it, which the
    // event loop can read once more; the event loop itself reads every byte
    // from the starting position on exactly once, unless the preamble read
    // already covered it
    require(!reads.ranges.empty(), "the binlog was never read");
    const auto [preamble_offset, preamble_length]{reads.ranges.front()};
    std::vector<std::size_t> times_read(file_size, 0U);
    for (std::size_t index{1U}; index < std::size(reads.ranges); ++index) {
      const auto [offset, length]{reads.ranges[index]};
      for (auto position{offset};
           position < offset + length && position < file_size; ++position) {
        ++times_read[position];
      }
    }
    for (auto position{start_position}; position < file_size; ++position) {
      const bool in_preamble_read{position >= preamble_offset &&
                                  position <
                                      preamble_offset + preamble_length};
      require(times_read[position] == 1U ||
                  (times_read[position] == 0U && in_preamble_read),
              "byte " + std::to_string(position) + " of " +
                  std::to_string(file_size) + " was read " +
                  std::to_string(times_read[position]) +
                  " time(s) by the event loop to stream from position " +
                  std::to_string(start_position) + "; reads " +
                  describe_reads(reads));
    }
  });
}

// a binlog whose last event is incomplete is corrupt: the collector only
// stores complete events (whole transactions), and the reader of the active
// binlog sees only what the storage has recorded, so since PBS-29 the reader
// streams the complete events, then fails rather than streaming part of the
// last one or waiting for the rest (the reader of PBS-65 waited instead)
BOOST_AUTO_TEST_CASE(PartialEventAtEofFailsCleanly) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    const auto sizes{tc.draw("sizes", event_size_lists())};
    const auto block_size{tc.draw("block", block_sizes())};
    const auto pending_size{tc.draw("pending_size", splittable_event_sizes())};
    const auto kept{tc.draw(
        "kept", gs::integers<std::size_t>(
                    {.min_value = 1U, .max_value = pending_size - 1U}))};

    const auto seeded{seed(directory.path(), sizes)};
    const auto pending{make_event(pending_size, 1'000'000U)};
    append_flushed(*seeded.storage, util::const_byte_span{pending}.first(kept));
    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        seeded.storage,
        block_size,
        std::string_view{},
        events::magic_binlog_offset,
        /*session_source_binlog_checksum=*/true};

    // the limit leaves room for the partial event, so a reader that streams
    // it ends at the end of the data instead of tripping the guard
    const auto outcome{stream_until_end(reader, std::size(seeded.events) + 3U)};
    require(outcome.failed,
            "the binlog ending in a partial event was streamed without a "
            "failure (" +
                std::to_string(std::size(outcome.events)) +
                " event(s) emitted)");
    require(std::size(outcome.events) == std::size(seeded.events) + 1U,
            "expected the artificial ROTATE and the complete stored events "
            "before the failure, got " +
                std::to_string(std::size(outcome.events)) + " event(s)");
    for (std::size_t index{0U}; index != std::size(seeded.events); ++index) {
      require(outcome.events[index + 1U] == seeded.events[index],
              "stored event " + std::to_string(index) +
                  " was not streamed byte for byte");
    }
  });
}

// a binlog that ends in a partial event, followed by the next binlog, is
// corrupt: the reader streams the complete events, then fails rather than
// dropping the partial bytes and carrying on with the next file
BOOST_AUTO_TEST_CASE(PartialEventBeforeRotationFails) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    const auto sizes{tc.draw("sizes", event_size_lists())};
    const auto block_size{tc.draw("block", block_sizes())};
    const auto truncated_size{
        tc.draw("truncated_size", splittable_event_sizes())};
    const auto kept{tc.draw(
        "kept", gs::integers<std::size_t>(
                    {.min_value = 1U, .max_value = truncated_size - 1U}))};
    const auto next_sizes{tc.draw("next_sizes", event_size_lists())};

    const auto seeded{seed(directory.path(), sizes)};
    const auto truncated{make_event(truncated_size, 1'000'000U)};
    append_flushed(*seeded.storage, util::const_byte_span{truncated}.first(kept));
    seeded.storage->close_binlog();

    require(seeded.storage->open_binlog(the_next_binlog) ==
                binsrv::open_binlog_status::created,
            "the next binlog was not created");
    append_flushed(*seeded.storage, util::const_byte_span{make_fde()});
    std::uint32_t id{2'000'000U};
    for (const auto size : next_sizes) {
      append_flushed(*seeded.storage,
                     util::const_byte_span{make_event(size, id++)});
    }

    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        seeded.storage,
        block_size,
        std::string_view{},
        events::magic_binlog_offset,
        /*session_source_binlog_checksum=*/true};

    // the limit leaves room for every event of both files, so a reader that
    // skips the partial bytes ends at EOF instead of tripping the guard
    const auto outcome{stream_until_end(
        reader, std::size(seeded.events) + std::size(next_sizes) + 4U)};
    require(outcome.failed,
            "the partial event at the end of the first binlog was dropped and "
            "streaming went on (" +
                std::to_string(std::size(outcome.events)) +
                " event(s) emitted)");
    require(std::size(outcome.events) == std::size(seeded.events) + 1U,
            "expected the artificial ROTATE and the complete events of the "
            "first binlog before the failure, got " +
                std::to_string(std::size(outcome.events)) + " event(s)");
    for (std::size_t index{0U}; index != std::size(seeded.events); ++index) {
      require(outcome.events[index + 1U] == seeded.events[index],
              "stored event " + std::to_string(index) +
                  " was not streamed byte for byte");
    }
  });
}

// a replica that connects before PBS has pulled anything (PBS-91) sees EOF,
// not an error, for as long as the storage stays empty, whether it names the
// binlog or not; once the binlog is written it is streamed from the beginning
BOOST_AUTO_TEST_CASE(EmptyStorageIsPolledUntilTheBinlogArrives) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    const bool name_specified{tc.draw("name_specified", gs::booleans())};
    const auto polls{
        tc.draw("polls", gs::integers<std::size_t>(
                             {.min_value = 1U, .max_value = 5U}))};
    const auto sizes{tc.draw("sizes", event_size_lists())};
    const auto block_size{tc.draw("block", block_sizes())};

    const auto storage{make_storage(write_config(directory.path()))};
    const std::string binlog_name{name_specified ? the_binlog.str()
                                                 : std::string{}};
    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        storage,
        block_size,
        binlog_name,
        events::magic_binlog_offset,
        /*session_source_binlog_checksum=*/true};

    for (std::size_t poll{0U}; poll != polls; ++poll) {
      const auto outcome{stream_until_end(reader, 0U)};
      require(!outcome.failed,
              "poll " + std::to_string(poll + 1U) +
                  " of an empty storage failed instead of reporting EOF");
      require(outcome.events.empty(),
              "an event was streamed from an empty storage");
    }

    const auto seeded{write_binlog(storage, sizes)};

    const auto emitted{drain(reader, std::size(seeded.events) + 1U)};
    require(std::size(emitted) == std::size(seeded.events) + 1U,
            "expected an artificial ROTATE followed by every stored event, "
            "got " +
                std::to_string(std::size(emitted)) + " event(s)");
    require(header_of(emitted.front()).get_type_code() == code_type::rotate,
            "the first emitted event must be a ROTATE");
    require(rotate_position(emitted.front()) == events::magic_binlog_offset,
            "a from-beginning ROTATE must point at the magic offset");
    for (std::size_t index{0U}; index != std::size(seeded.events); ++index) {
      require(emitted[index + 1U] == seeded.events[index],
              "stored event " + std::to_string(index) +
                  " was not streamed byte for byte");
    }
  });
}

// the same for a replica resuming from a position in the binlog it names:
// EOF while the storage is empty, then the artificial ROTATE and FDE and the
// events from that position once the binlog is written (the PBS-91 commit
// treats an empty storage as "not yet", so that a blocking client can poll
// until the requested binlog becomes available)
BOOST_AUTO_TEST_CASE(EmptyStorageIsPolledUntilTheBinlogArrivesFromPosition) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    const auto polls{
        tc.draw("polls", gs::integers<std::size_t>(
                             {.min_value = 1U, .max_value = 5U}))};
    // at least one synthetic event, so there is a non-beginning boundary
    auto sizes{tc.draw("sizes", event_size_lists())};
    if (sizes.empty()) {
      sizes.push_back(events::common_header_view_base::size_in_bytes);
    }
    const auto block_size{tc.draw("block", block_sizes())};
    const auto offsets{planned_offsets(sizes)};
    // index 0 is the FDE, which is the from-beginning case
    const auto resume_index{tc.draw(
        "resume",
        gs::integers<std::size_t>({.min_value = 1U,
                                   .max_value = std::size(offsets) - 1U}))};
    const auto resume_position{offsets[resume_index]};

    const auto storage{make_storage(write_config(directory.path()))};
    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        storage,
        block_size,
        the_binlog.str(),
        resume_position,
        /*session_source_binlog_checksum=*/true};

    for (std::size_t poll{0U}; poll != polls; ++poll) {
      const auto outcome{stream_until_end(reader, 0U)};
      require(!outcome.failed,
              "poll " + std::to_string(poll + 1U) +
                  " of an empty storage, resuming at position " +
                  std::to_string(resume_position) +
                  ", failed instead of reporting EOF");
      require(outcome.events.empty(),
              "an event was streamed from an empty storage");
    }

    const auto seeded{write_binlog(storage, sizes)};
    require(seeded.offsets == offsets,
            "the binlog was not laid out as planned");

    const auto remaining{std::size(seeded.events) - resume_index};
    const auto emitted{drain(reader, remaining + 2U)};
    require(std::size(emitted) == remaining + 2U,
            "expected an artificial ROTATE and FDE then the remaining "
            "events, got " +
                std::to_string(std::size(emitted)) + " event(s)");
    require(rotate_position(emitted[0U]) == resume_position,
            "a from-position ROTATE must carry the requested position");
    require(header_of(emitted[1U]).get_type_code() ==
                code_type::format_description,
            "the event after the ROTATE must be a FORMAT_DESCRIPTION");
    for (std::size_t index{0U}; index != remaining; ++index) {
      require(emitted[index + 2U] == seeded.events[resume_index + index],
              "stored event " + std::to_string(resume_index + index) +
                  " was not streamed byte for byte from the position");
    }
  });
}

// a reader that crosses into the next binlog announces it with an artificial
// ROTATE naming that binlog at the magic offset, as MySQL does, so that a
// client (mysqlbinlog --raw, a replica) knows which file the following
// events belong to - also when it reaches the end of a binlog while the next
// one has been created but holds nothing but the binlog magic yet (the
// collector creates the file on rotation and flushes its first events later)
BOOST_AUTO_TEST_CASE(SwitchToTheNextBinlogEmitsArtificialRotate) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    const auto sizes{tc.draw("sizes", event_size_lists())};
    const auto next_sizes{tc.draw("next_sizes", event_size_lists())};
    const auto block_size{tc.draw("block", block_sizes())};
    // whether the next binlog already holds its events when the reader
    // reaches the end of the first one
    const bool next_filled_before_read{
        tc.draw("next_filled_before_read", gs::booleans())};

    const auto seeded{seed(directory.path(), sizes)};
    seeded.storage->close_binlog();
    require(seeded.storage->open_binlog(the_next_binlog) ==
                binsrv::open_binlog_status::created,
            "the next binlog was not created");

    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        seeded.storage,
        block_size,
        std::string_view{},
        events::magic_binlog_offset,
        /*session_source_binlog_checksum=*/true};

    const auto limit{std::size(seeded.events) + std::size(next_sizes) + 4U};
    std::optional<seeded_storage> next;
    if (next_filled_before_read) {
      next = fill_open_binlog(seeded.storage, next_sizes, 1'000'000U);
    }
    auto emitted{drain(reader, limit)};
    if (!next_filled_before_read) {
      next = fill_open_binlog(seeded.storage, next_sizes, 1'000'000U);
      for (auto &event : drain(reader, limit)) {
        emitted.push_back(std::move(event));
      }
    }

    // ROTATE + first binlog, ROTATE + next binlog (FDE first)
    const auto expected_count{std::size(seeded.events) +
                              std::size(next->events) + 2U};
    require(std::size(emitted) == expected_count,
            "expected " + std::to_string(expected_count) +
                " event(s) across both binlogs, got " +
                std::to_string(std::size(emitted)));
    require(is_artificial_rotate_to(emitted.front(), the_binlog,
                                    events::magic_binlog_offset),
            "the stream must start with an artificial ROTATE to the first "
            "binlog");
    for (std::size_t index{0U}; index != std::size(seeded.events); ++index) {
      require(emitted[index + 1U] == seeded.events[index],
              "event " + std::to_string(index) +
                  " of the first binlog was not streamed byte for byte");
    }
    const auto switch_index{std::size(seeded.events) + 1U};
    require(is_artificial_rotate_to(emitted[switch_index], the_next_binlog,
                                    events::magic_binlog_offset),
            "no artificial ROTATE to " + the_next_binlog.str() +
                " was emitted when the reader moved to it");
    for (std::size_t index{0U}; index != std::size(next->events); ++index) {
      require(emitted[switch_index + 1U + index] == next->events[index],
              "event " + std::to_string(index) +
                  " of the next binlog was not streamed byte for byte");
    }
  });
}

// a replica that names a binlog which has been created but holds only the
// binlog magic yet (the collector opens the file before its first flush)
// sees EOF, not an error, until the events arrive - from the beginning and
// from a position (PBS-91 covers an empty storage; PBS-96 the position case
// on an empty storage; this is the same wait one step later)
BOOST_AUTO_TEST_CASE(MagicOnlyBinlogIsPolledUntilItsEventsArrive) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    const bool from_position{tc.draw("from_position", gs::booleans())};
    const auto polls{
        tc.draw("polls", gs::integers<std::size_t>(
                             {.min_value = 1U, .max_value = 5U}))};
    auto sizes{tc.draw("sizes", event_size_lists())};
    if (from_position && sizes.empty()) {
      sizes.push_back(events::common_header_view_base::size_in_bytes);
    }
    const auto block_size{tc.draw("block", block_sizes())};
    const auto offsets{planned_offsets(sizes)};
    std::size_t start_index{0U};
    if (from_position) {
      start_index = tc.draw(
          "resume", gs::integers<std::size_t>(
                        {.min_value = 1U, .max_value = std::size(offsets) - 1U}));
    }
    const auto start_position{offsets[start_index]};

    const auto storage{make_storage(write_config(directory.path()))};
    require(storage->open_binlog(the_binlog) ==
                binsrv::open_binlog_status::created,
            "the binlog was not created");
    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        storage,
        block_size,
        the_binlog.str(),
        start_position,
        /*session_source_binlog_checksum=*/true};

    for (std::size_t poll{0U}; poll != polls; ++poll) {
      const auto outcome{stream_until_end(reader, 2U)};
      require(!outcome.failed,
              "poll " + std::to_string(poll + 1U) +
                  " of a binlog holding only the magic, starting at position " +
                  std::to_string(start_position) +
                  ", failed instead of reporting EOF");
      require(std::size(outcome.events) <= (from_position ? 0U : 1U),
              "a stored event was streamed from a binlog holding only the "
              "magic");
    }

    const auto seeded{fill_open_binlog(storage, sizes)};
    require(seeded.offsets == offsets,
            "the binlog was not laid out as planned");
    const auto remaining{std::size(seeded.events) - start_index};
    const auto emitted{drain(reader, remaining + 2U)};
    // the from-beginning ROTATE may already have been sent while polling;
    // the stored events must follow, after an artificial FDE when resuming
    require(std::size(emitted) >= remaining,
            "expected the stored events from position " +
                std::to_string(start_position) + ", got " +
                std::to_string(std::size(emitted)) + " event(s)");
    const auto first_stored{std::size(emitted) - remaining};
    for (std::size_t index{0U}; index != remaining; ++index) {
      require(emitted[first_stored + index] ==
                  seeded.events[start_index + index],
              "stored event " + std::to_string(start_index + index) +
                  " was not streamed byte for byte");
    }
  });
}

// the retention cycle a 'pull' deployment runs (README: stop pull, run
// purge_binlogs, start pull again): after the oldest binlogs are purged, a
// reader on the restarted storage streams every surviving binlog byte for
// byte, each announced by an artificial ROTATE, and a request for a purged
// binlog fails without streaming anything
BOOST_AUTO_TEST_CASE(PurgedStorageServesTheSurvivingBinlogs) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    const auto binlog_sizes{tc.draw(
        "binlogs",
        gs::vectors(event_size_lists(), {.min_size = 2U, .max_size = 5U}))};
    const auto purge_count{tc.draw(
        "purge_count",
        gs::integers<std::size_t>({.min_value = 1U,
                                   .max_value = std::size(binlog_sizes) - 1U}))};
    const auto block_size{tc.draw("block", block_sizes())};
    const bool request_purged{tc.draw("request_purged", gs::booleans())};

    const auto config_path{write_config(directory.path())};
    std::vector<events::composite_binlog_name> names;
    std::vector<std::vector<byte_vector>> stored;
    {
      // 'pull' running: writes the binlogs, then is stopped
      auto storage{make_storage(config_path)};
      events::composite_binlog_name name{the_binlog};
      std::uint32_t first_id{1U};
      for (const auto &sizes : binlog_sizes) {
        if (!names.empty()) {
          storage->close_binlog();
        }
        require(storage->open_binlog(name) ==
                    binsrv::open_binlog_status::created,
                "binlog " + name.str() + " was not created");
        auto seeded{fill_open_binlog(storage, sizes, first_id)};
        first_id += 1'000U;
        names.push_back(name);
        stored.push_back(std::move(seeded.events));
        name = name.next();
      }
    }
    {
      // 'purge_binlogs' up to the last purged binlog
      const binsrv::main_config config{config_path.string()};
      binsrv::storage purger{
          std::make_shared<binsrv::null_logger>(), config,
          binsrv::storage_construction_mode_type::purging};
      const auto [purged, warning]{
          purger.purge_binlogs(names[purge_count - 1U])};
      require(warning.empty(), "purge reported a warning: " + warning);
      require(std::size(purged) == purge_count,
              "purge removed " + std::to_string(std::size(purged)) +
                  " binlog(s) instead of " + std::to_string(purge_count));
    }

    // 'pull' started again
    const auto storage{make_storage(config_path)};
    if (request_purged) {
      const auto purged_index{tc.draw(
          "purged_index",
          gs::integers<std::size_t>({.min_value = 0U,
                                     .max_value = purge_count - 1U}))};
      operations::sender_context reader{
          std::make_shared<binsrv::null_logger>(),
          storage,
          block_size,
          names[purged_index].str(),
          events::magic_binlog_offset,
          /*session_source_binlog_checksum=*/true};
      const auto outcome{stream_until_end(reader, 4U)};
      require(outcome.failed && outcome.events.empty(),
              "a request for purged binlog " + names[purged_index].str() +
                  " did not fail cleanly (" +
                  std::to_string(std::size(outcome.events)) +
                  " event(s) streamed)");
      return;
    }

    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        storage,
        block_size,
        std::string_view{},
        events::magic_binlog_offset,
        /*session_source_binlog_checksum=*/true};
    std::size_t expected_count{0U};
    for (std::size_t index{purge_count}; index != std::size(names); ++index) {
      expected_count += std::size(stored[index]) + 1U;
    }
    const auto emitted{drain(reader, expected_count)};
    require(std::size(emitted) == expected_count,
            "expected " + std::to_string(expected_count) +
                " event(s) from the surviving binlogs, got " +
                std::to_string(std::size(emitted)));
    std::size_t position{0U};
    for (std::size_t index{purge_count}; index != std::size(names); ++index) {
      require(is_artificial_rotate_to(emitted[position], names[index],
                                      events::magic_binlog_offset),
              "no artificial ROTATE to surviving binlog " +
                  names[index].str());
      ++position;
      for (std::size_t event{0U}; event != std::size(stored[index]);
           ++event) {
        require(emitted[position] == stored[index][event],
                "event " + std::to_string(event) + " of surviving binlog " +
                    names[index].str() + " was not streamed byte for byte");
        ++position;
      }
    }
  });
}

// a dump position that is not the start of an event (a client error, or a
// position remembered from a different binlog) cannot be streamed from:
// the reader must never hand out bytes from the middle of an event as if
// they were events - MySQL rejects such a position (ER 1236) - and must not
// crash or hang on whatever the bytes at that offset look like (ticket 7:
// an event size field below the common header size)
BOOST_AUTO_TEST_CASE(MisalignedDumpPositionNeverStreamsGarbage) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    auto sizes{tc.draw("sizes", event_size_lists())};
    if (sizes.empty()) {
      sizes.push_back(events::common_header_view_base::size_in_bytes);
    }
    const auto block_size{tc.draw("block", block_sizes())};

    const auto seeded{seed(directory.path(), sizes)};
    // a byte inside one of the stored events (the FDE included), never its
    // first byte
    const auto event_index{tc.draw(
        "event", gs::integers<std::size_t>(
                     {.min_value = 0U,
                      .max_value = std::size(seeded.events) - 1U}))};
    const auto inside{tc.draw(
        "inside", gs::integers<std::size_t>(
                      {.min_value = 1U,
                       .max_value = std::size(seeded.events[event_index]) -
                                    1U}))};
    const auto position{seeded.offsets[event_index] + inside};

    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        seeded.storage,
        block_size,
        the_binlog.str(),
        position,
        /*session_source_binlog_checksum=*/true};

    const auto outcome{
        stream_until_end(reader, std::size(seeded.events) + 2U)};
    // at most the artificial ROTATE and FDE, which describe the request, not
    // the misaligned data
    require(std::size(outcome.events) <= 2U,
            "streaming from position " + std::to_string(position) + " (" +
                std::to_string(inside) + " byte(s) into stored event " +
                std::to_string(event_index) + ") emitted " +
                std::to_string(std::size(outcome.events) - 2U) +
                " event(s) cut out of the middle of the stored data");
  });
}

} // namespace
