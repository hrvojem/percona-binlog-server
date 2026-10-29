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
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#define BOOST_TEST_MODULE SenderContextPropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "heartbeat_test_helpers.hpp"
#include "property_test_helpers.hpp"

#include "operations/event_generation_helpers.hpp"
#include "binsrv/basic_storage_backend.hpp"
#include "binsrv/replication_mode_type.hpp"
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
#include "binsrv/gtids/tag.hpp"
#include "binsrv/gtids/uuid.hpp"
#include "binsrv/events/code_type.hpp"
#include "binsrv/events/common_header_flag_type.hpp"
#include "binsrv/events/common_header_view.hpp"
#include "binsrv/events/composite_binlog_name.hpp"
#include "binsrv/events/event_fwd.hpp"
#include "binsrv/events/generic_post_header.hpp"
#include "binsrv/events/gtid_log_body.hpp"
#include "binsrv/events/gtid_log_flag_type.hpp"
#include "binsrv/events/gtid_log_post_header.hpp"
#include "binsrv/events/protocol_traits_fwd.hpp"
#include "binsrv/events/rotate_post_header_impl.hpp"

#include "util/byte_span.hpp"
#include "util/byte_span_fwd.hpp"
#include "util/byte_span_inserters.hpp"
#include "util/crc_helpers.hpp"
#include "util/ctime_timestamp.hpp"
#include "util/semantic_version.hpp"
#include "util/timestamp_helpers.hpp"

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
    "require_secure_transport": false,
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
make_previous_gtids_log(std::uint64_t offset, std::uint32_t event_server_id,
                        const binsrv::gtids::gtid_set &gtids = {}) {
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
              binsrv::events::code_type::previous_gtids_log>{gtids},
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
                 std::uint32_t first_id = 1U,
                 const binsrv::gtids::gtid_set &previous_gtids = {}) {
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
  store(make_previous_gtids_log(offset, server_id, previous_gtids));
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
             const std::vector<std::size_t> &event_sizes,
             const binsrv::gtids::gtid_set &previous_gtids = {}) {
  require(storage->open_binlog(the_binlog) ==
              binsrv::open_binlog_status::created,
          "the binlog was not created");
  return fill_open_binlog(std::move(storage), event_sizes, 1U, previous_gtids);
}

// a storage with a single binlog (see 'write_binlog()')
[[nodiscard]] seeded_storage
seed(const std::filesystem::path &directory,
     const std::vector<std::size_t> &event_sizes,
     const binsrv::gtids::gtid_set &previous_gtids = {}) {
  return write_binlog(make_storage(write_config(directory)), event_sizes,
                      previous_gtids);
}

// the offset of each event 'write_binlog()' will store for 'event_sizes', FDE
// and PREVIOUS_GTIDS_LOG first, so a test can pick a position before anything
// is written
[[nodiscard]] std::vector<std::uint64_t>
planned_offsets(const std::vector<std::size_t> &event_sizes,
                const binsrv::gtids::gtid_set &previous_gtids = {}) {
  std::vector<std::uint64_t> result{events::magic_binlog_offset};
  result.push_back(result.back() + std::size(make_fde()));
  result.push_back(result.back() +
                   std::size(make_previous_gtids_log(result.back(), server_id,
                                                     previous_gtids)));
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

// the GTID set of a binlog's PREVIOUS_GTIDS_LOG: mostly a few UUIDs, some
// with hundreds (each UUID takes 40 bytes), so that the event outgrows the
// sender's first preamble read (a block from the beginning, about 2 KiB
// when resuming) and the rest has to be fetched
[[nodiscard]] gs::Generator<binsrv::gtids::gtid_set> previous_gtid_sets() {
  return gs::compose([](const hegel::TestCase &tc) {
    const auto count{tc.draw(gs::one_of(
        {gs::integers<std::size_t>({.min_value = 0U, .max_value = 3U}),
         gs::integers<std::size_t>({.min_value = 50U, .max_value = 400U})}))};
    binsrv::gtids::gtid_set result;
    for (std::size_t index{0U}; index < count; ++index) {
      binsrv::gtids::uuid_storage data{};
      // distinct and never the nil UUID
      const auto value{index + 1U};
      for (std::size_t byte{0U}; byte < sizeof value; ++byte) {
        data[byte] = static_cast<std::byte>((value >> (8U * byte)) & 0xFFU);
      }
      data[std::size(data) - 1U] = std::byte{0x5A};
      result.add(binsrv::gtids::uuid{data}, binsrv::gtids::tag{}, 1U);
    }
    return result;
  });
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

    const auto previous_gtids{
        tc.draw("previous_gtids", previous_gtid_sets())};
    const auto seeded{seed(directory.path(), sizes, previous_gtids)};
    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        seeded.storage,
        block_size,
        binsrv::replication_mode_type::position,
        std::string_view{},
        events::magic_binlog_offset,
        binsrv::gtids::gtid_set{},
        session_checksum};

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

    const auto previous_gtids{
        tc.draw("previous_gtids", previous_gtid_sets())};
    const auto seeded{seed(directory.path(), sizes, previous_gtids)};
    // resume at the boundary of one of the synthetic events (index 1..N;
    // index 0 is the FDE, which is the from-beginning case)
    const auto resume_index{tc.draw(
        "resume",
        gs::integers<std::size_t>({.min_value = 1U,
                                   .max_value = std::size(seeded.events) - 1U}))};
    const auto resume_position{seeded.offsets[resume_index]};

    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        seeded.storage,
        block_size,
        binsrv::replication_mode_type::position,
        std::string_view{the_binlog.str()},
        resume_position,
        binsrv::gtids::gtid_set{},
        session_checksum};

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
    const auto previous_gtids{
        tc.draw("previous_gtids", previous_gtid_sets())};
    const auto seeded{seed(directory.path(), sizes, previous_gtids)};
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
    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        seeded.storage,
        block_size,
        binsrv::replication_mode_type::position,
        binlog_name,
        start_position,
        binsrv::gtids::gtid_set{},
        /*session_source_binlog_checksum=*/true};

    // artificial ROTATE (+ artificial FDE when resuming) + the stored events
    const auto remaining{std::size(seeded.events) - start_index};
    const auto preamble{from_beginning ? 1U : 2U};
    const auto emitted{drain(reader, remaining + preamble)};
    require(std::size(emitted) == remaining + preamble,
            "expected the preamble followed by every remaining stored event");

    const auto file_size{seeded.offsets.back() +
                         std::size(seeded.events.back())};
    // the preamble (FORMAT_DESCRIPTION and PREVIOUS_GTIDS_LOG, PBS-29) takes
    // one read of an estimated size, and one more for the rest of a large
    // PREVIOUS_GTIDS_LOG; a dump that resumes inside the preamble, or right
    // after it, reads part of it again in the event loop. Beyond the
    // preamble, the first read and the starting position, every byte is
    // read exactly once (PBS-65: a partial trailing event is carried over
    // in memory, not re-read), and every byte from the starting position on
    // is read at least once.
    require(!reads.ranges.empty(), "the binlog was never read");
    const auto preamble_end{std::size(seeded.offsets) > 2U ? seeded.offsets[2]
                                                            : file_size};
    const auto [first_offset, first_length]{reads.ranges.front()};
    const auto strict_from{
        std::max({start_position, preamble_end, first_offset + first_length})};
    std::vector<std::size_t> times_read(file_size, 0U);
    for (const auto &[offset, length] : reads.ranges) {
      for (auto position{offset};
           position < offset + length && position < file_size; ++position) {
        ++times_read[position];
      }
    }
    for (auto position{start_position}; position < file_size; ++position) {
      require(times_read[position] >= 1U &&
                  (position < strict_from || times_read[position] == 1U),
              "byte " + std::to_string(position) + " of " +
                  std::to_string(file_size) + " was read " +
                  std::to_string(times_read[position]) +
                  " time(s) to stream from position " +
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
        binsrv::replication_mode_type::position,
        std::string_view{},
        events::magic_binlog_offset,
        binsrv::gtids::gtid_set{},
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
        binsrv::replication_mode_type::position,
        std::string_view{},
        events::magic_binlog_offset,
        binsrv::gtids::gtid_set{},
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
        binsrv::replication_mode_type::position,
        binlog_name,
        events::magic_binlog_offset,
        binsrv::gtids::gtid_set{},
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
        binsrv::replication_mode_type::position,
        the_binlog.str(),
        resume_position,
        binsrv::gtids::gtid_set{},
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

    const auto previous_gtids{
        tc.draw("previous_gtids", previous_gtid_sets())};
    const auto next_previous_gtids{
        tc.draw("next_previous_gtids", previous_gtid_sets())};
    const auto seeded{seed(directory.path(), sizes, previous_gtids)};
    seeded.storage->close_binlog();
    require(seeded.storage->open_binlog(the_next_binlog) ==
                binsrv::open_binlog_status::created,
            "the next binlog was not created");

    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        seeded.storage,
        block_size,
        binsrv::replication_mode_type::position,
        std::string_view{},
        events::magic_binlog_offset,
        binsrv::gtids::gtid_set{},
        /*session_source_binlog_checksum=*/true};

    const auto limit{std::size(seeded.events) + std::size(next_sizes) + 4U};
    std::optional<seeded_storage> next;
    if (next_filled_before_read) {
      next = fill_open_binlog(seeded.storage, next_sizes, 1'000'000U,
                              next_previous_gtids);
    }
    auto emitted{drain(reader, limit)};
    if (!next_filled_before_read) {
      next = fill_open_binlog(seeded.storage, next_sizes, 1'000'000U,
                              next_previous_gtids);
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

// while a dump sits idle, the heartbeat the listener sends (PBS-30) reports
// the replica's current coordinates, as a MySQL source does: the binlog named
// by the last artificial ROTATE and the end of the last event sent in it -
// also when the stream resumed from a position, after a switch to the next
// binlog, and while the next binlog has been created but holds only the
// binlog magic (the collector opens the file before its first flush)
BOOST_AUTO_TEST_CASE(IdleHeartbeatReportsTheReplicaCoordinates) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    const bool from_position{tc.draw("from_position", gs::booleans())};
    auto sizes{tc.draw("sizes", event_size_lists())};
    if (from_position && sizes.empty()) {
      sizes.push_back(events::common_header_view_base::size_in_bytes);
    }
    const bool session_checksum{tc.draw("checksum", gs::booleans())};
    const auto block_size{tc.draw("block", block_sizes())};
    // 0: no next binlog; 1: the next binlog holds only the magic; 2: it is
    // filled before the reader reaches it; 3: it is filled after the reader
    // has been idle at the end of the first binlog
    const auto next_state{tc.draw(
        "next_state",
        gs::integers<std::size_t>({.min_value = 0U, .max_value = 3U}))};
    const auto next_sizes{tc.draw("next_sizes", event_size_lists())};

    const auto seeded{seed(directory.path(), sizes)};
    std::size_t start_index{0U};
    if (from_position) {
      start_index = tc.draw(
          "resume",
          gs::integers<std::size_t>(
              {.min_value = 1U, .max_value = std::size(seeded.events) - 1U}));
    }
    if (next_state != 0U) {
      seeded.storage->close_binlog();
      require(seeded.storage->open_binlog(the_next_binlog) ==
                  binsrv::open_binlog_status::created,
              "the next binlog was not created");
    }
    if (next_state == 2U) {
      std::ignore = fill_open_binlog(seeded.storage, next_sizes, 1'000'000U);
    }

    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        seeded.storage,
        block_size,
        binsrv::replication_mode_type::position,
        from_position ? the_binlog.str() : std::string{},
        seeded.offsets[start_index],
        binsrv::gtids::gtid_set{},
        session_checksum};

    heartbeat_testing::replica_coordinates coordinates;
    const auto limit{std::size(seeded.events) + std::size(next_sizes) + 6U};
    const auto read_until_idle{[&](std::string_view phase) {
      for (const auto &event : drain(reader, limit)) {
        heartbeat_testing::follow(coordinates, event, session_checksum,
                                  /*format_description_checksum=*/true);
      }
      util::const_byte_span heartbeat;
      require(reader.generate_heartbeat(heartbeat),
              std::string{phase} + ": no heartbeat while idle at " +
                  coordinates.binlog + ":" +
                  std::to_string(coordinates.position));
      const auto error{heartbeat_testing::heartbeat_error(
          byte_vector{std::cbegin(heartbeat), std::cend(heartbeat)},
          coordinates, server_id, /*with_checksum=*/true)};
      require(error.empty(), std::string{phase} + ": " + error);
    }};

    read_until_idle("idle after the first binlog");
    if (next_state == 3U) {
      std::ignore = fill_open_binlog(seeded.storage, next_sizes, 1'000'000U);
      read_until_idle("idle after the next binlog");
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
        binsrv::replication_mode_type::position,
        the_binlog.str(),
        start_position,
        binsrv::gtids::gtid_set{},
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
          binsrv::replication_mode_type::position,
          names[purged_index].str(),
          events::magic_binlog_offset,
          binsrv::gtids::gtid_set{},
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
        binsrv::replication_mode_type::position,
        std::string_view{},
        events::magic_binlog_offset,
        binsrv::gtids::gtid_set{},
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
        binsrv::replication_mode_type::position,
        the_binlog.str(),
        position,
        binsrv::gtids::gtid_set{},
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

// ---------------------------------------------------------------------------
// GTID mode (COM_BINLOG_DUMP_GTID, PBS-29 part 2)
//
// In GTID mode the sender starts at the first stored binlog and, after the
// artificial ROTATE, always sends the FORMAT_DESCRIPTION and the
// PREVIOUS_GTIDS_LOG. It then streams whole transactions: a transaction is a
// GTID event (GTID_LOG, GTID_TAGGED_LOG or ANONYMOUS_GTID_LOG) and the events
// its transaction_length covers; one whose GTID the replica has is skipped
// whole, every other one (anonymous ones included) is streamed byte for byte.
// A transaction larger than the read block is fetched whole by its
// transaction_length (indexed_event_block, mode by_transaction).
// ---------------------------------------------------------------------------

namespace {

enum class gtid_kind : std::uint8_t { untagged, tagged, anonymous };

constexpr std::array<std::string_view, 2> uuid_pool{
    "3e11fa47-71ca-11e1-9e33-c80aa9429562",
    "11111111-2222-3333-4444-555555555555"};
constexpr std::array<std::string_view, 2> tag_pool{"alpha", "beta"};
const binsrv::gtids::uuid foreign_uuid{"77777777-8888-9999-aaaa-bbbbccccdddd"};

struct transaction_spec {
  gtid_kind kind;
  std::size_t uuid_index;
  std::size_t tag_index;
  // the events after the GTID event (synthetic, only their size matters)
  std::vector<std::size_t> other_event_sizes;
  friend bool operator==(const transaction_spec &,
                         const transaction_spec &) = default;
};

std::ostream &operator<<(std::ostream &output, const transaction_spec &value) {
  static constexpr std::array<std::string_view, 3> kinds{"untagged", "tagged",
                                                         "anonymous"};
  output << kinds.at(static_cast<std::size_t>(value.kind)) << " uuid "
         << value.uuid_index << " tag " << value.tag_index << " events [";
  for (const auto size : value.other_event_sizes) {
    output << ' ' << size;
  }
  return output << " ]";
}

// the GTID event of a transaction at 'offset', its transaction_length
// (which counts the GTID event itself) consistent with the other events
[[nodiscard]] byte_vector make_gtid_event(gtid_kind kind,
                                          const binsrv::gtids::gtid &gtid,
                                          std::uint64_t offset,
                                          std::size_t other_events_size,
                                          events::seq_no_t sequence_number) {
  const auto timestamp{
      util::microseconds_to_high_resolution_time_point(1700000000000000ULL)};
  const util::ctime_timestamp header_timestamp{1700000000};
  const events::seq_no_t last_committed{sequence_number - 1U};
  std::uint64_t transaction_length{other_events_size};
  for (std::size_t iteration{0U}; iteration < 10U; ++iteration) {
    events::event_storage buffer;
    std::size_t event_size{};
    if (kind == gtid_kind::tagged) {
      const events::generic_body<code_type::gtid_tagged_log> body{
          events::gtid_log_flag_set{},
          gtid.get_uuid(),
          gtid.get_gno(),
          gtid.get_tag(),
          last_committed,
          sequence_number,
          timestamp,
          std::nullopt,
          transaction_length,
          server_version,
          std::nullopt,
          std::nullopt};
      event_size = events::event::create_event<code_type::gtid_tagged_log>(
                       static_cast<std::uint32_t>(offset), header_timestamp,
                       server_id, {}, {}, body, /*include_checksum=*/true,
                       buffer)
                       .calculate_encoded_size();
    } else {
      const bool anonymous{kind == gtid_kind::anonymous};
      const events::gtid_log_post_header post_header{
          events::gtid_log_flag_set{},
          anonymous ? binsrv::gtids::uuid{} : gtid.get_uuid(),
          anonymous ? binsrv::gtids::gno_t{0U} : gtid.get_gno(),
          events::gtid_log_post_header::known_logical_ts_code,
          last_committed,
          sequence_number};
      const events::gtid_log_body body{timestamp,          std::nullopt,
                                       transaction_length, server_version,
                                       std::nullopt,       std::nullopt};
      if (anonymous) {
        event_size =
            events::event::create_event<code_type::anonymous_gtid_log>(
                static_cast<std::uint32_t>(offset), header_timestamp,
                server_id, {}, post_header, body, /*include_checksum=*/true,
                buffer)
                .calculate_encoded_size();
      } else {
        event_size = events::event::create_event<code_type::gtid_log>(
                         static_cast<std::uint32_t>(offset), header_timestamp,
                         server_id, {}, post_header, body,
                         /*include_checksum=*/true, buffer)
                         .calculate_encoded_size();
      }
    }
    if (transaction_length == event_size + other_events_size) {
      return {std::cbegin(buffer), std::cend(buffer)};
    }
    transaction_length = event_size + other_events_size;
  }
  throw std::logic_error{"transaction_length did not converge"};
}

struct stored_transaction {
  // empty for an anonymous transaction
  binsrv::gtids::gtid gtid;
  // the GTID event (if any) first, as stored
  std::vector<byte_vector> events;
};

struct stored_gtid_binlog {
  events::composite_binlog_name name;
  byte_vector format_description;
  byte_vector previous_gtids_log;
  std::vector<stored_transaction> transactions;
};

// a damage to one transaction, for GtidModeRejectsDamagedTransactionsCleanly
enum class transaction_damage : std::uint8_t {
  none,
  // transaction_length one event short / a few bytes too long
  length_short,
  length_long,
  // the transaction starts with an ordinary event: its GTID event is missing
  missing_gtid_event,
  // a byte of the GTID event's body flipped: its CRC32 does not match
  bad_checksum
};

// changes 'transaction_length' of an encoded GTID event (re-encoding it with
// a different length could change the event's size, so the field is patched
// in place: it is the varlen / 8-byte field the body codec reads; the
// simplest exact way is to rebuild the event with the wanted length and keep
// it only if its size did not change)
[[nodiscard]] std::optional<byte_vector>
gtid_event_with_length(gtid_kind kind, const binsrv::gtids::gtid &gtid,
                       std::uint64_t offset, std::size_t original_size,
                       std::uint64_t transaction_length,
                       events::seq_no_t sequence_number) {
  // make_gtid_event() derives the length from the other events' size, so ask
  // for the one that yields 'transaction_length'
  if (transaction_length < original_size) {
    return std::nullopt;
  }
  auto result{make_gtid_event(kind, gtid, offset,
                              transaction_length - original_size,
                              sequence_number)};
  if (std::size(result) != original_size) {
    return std::nullopt;
  }
  return result;
}

// writes 'binlogs' (each a list of transactions) into a fresh storage: per
// binlog a FORMAT_DESCRIPTION, a PREVIOUS_GTIDS_LOG holding the GTIDs of
// the binlogs before it, then the transactions; every binlog but the last is
// closed. GTIDs are numbered per UUID and tag from 1. 'damage' applies to
// transaction 'damaged_index' of the last binlog ('overrun': the bytes a
// length_long transaction_length is too long by).
[[nodiscard]] std::vector<stored_gtid_binlog>
store_gtid_binlogs(binsrv::storage &storage,
                   const std::vector<std::vector<transaction_spec>> &binlogs,
                   transaction_damage damage = transaction_damage::none,
                   std::size_t damaged_index = 0U, std::size_t overrun = 7U) {
  std::vector<stored_gtid_binlog> result;
  std::map<std::pair<std::size_t, std::size_t>, binsrv::gtids::gno_t> next_gno;
  binsrv::gtids::gtid_set previous;
  std::uint32_t event_id{1U};
  events::seq_no_t sequence_number{1U};
  auto name{the_binlog};
  for (std::size_t binlog_index{0U}; binlog_index != std::size(binlogs);
       ++binlog_index) {
    if (binlog_index != 0U) {
      storage.close_binlog();
      name = name.next();
    }
    require(storage.open_binlog(name) == binsrv::open_binlog_status::created,
            "a binlog was not created");
    std::uint64_t offset{events::magic_binlog_offset};
    const auto store{[&](const byte_vector &bytes, bool at_boundary) {
      storage.write_event(util::const_byte_span{bytes}, at_boundary,
                          binsrv::gtids::gtid{},
                          util::ctime_timestamp{std::time_t{1}}, 0ULL);
      offset += std::size(bytes);
    }};
    stored_gtid_binlog binlog{.name = name,
                              .format_description = make_fde(),
                              .previous_gtids_log = {},
                              .transactions = {}};
    store(binlog.format_description, true);
    binlog.previous_gtids_log =
        make_previous_gtids_log(offset, server_id, previous);
    store(binlog.previous_gtids_log, true);
    const auto &specs{binlogs[binlog_index]};
    for (std::size_t index{0U}; index != std::size(specs); ++index) {
      const auto &spec{specs[index]};
      stored_transaction transaction{.gtid = {}, .events = {}};
      if (spec.kind != gtid_kind::anonymous) {
        const binsrv::gtids::uuid uuid{uuid_pool.at(spec.uuid_index)};
        const auto tag{spec.kind == gtid_kind::tagged
                           ? binsrv::gtids::tag{tag_pool.at(spec.tag_index)}
                           : binsrv::gtids::tag{}};
        auto &gno{next_gno[{spec.uuid_index, spec.kind == gtid_kind::tagged
                                                 ? spec.tag_index + 1U
                                                 : 0U}]};
        ++gno;
        transaction.gtid = binsrv::gtids::gtid{uuid, tag, gno};
        previous.add(transaction.gtid);
      }
      std::size_t other_size{0U};
      for (const auto size : spec.other_event_sizes) {
        other_size += size;
      }
      const bool damaged{damage != transaction_damage::none &&
                         binlog_index + 1U == std::size(binlogs) &&
                         index == damaged_index};
      auto gtid_event{make_gtid_event(spec.kind, transaction.gtid, offset,
                                      other_size, sequence_number)};
      if (damaged && (damage == transaction_damage::length_short ||
                      damage == transaction_damage::length_long)) {
        const auto actual{std::size(gtid_event) + other_size};
        const auto wanted{damage == transaction_damage::length_short
                              ? actual - spec.other_event_sizes.back()
                              : actual + overrun};
        if (auto patched{gtid_event_with_length(
                spec.kind, transaction.gtid, offset, std::size(gtid_event),
                wanted, sequence_number)};
            patched.has_value()) {
          gtid_event = std::move(*patched);
        }
      }
      if (damaged && damage == transaction_damage::bad_checksum) {
        // a byte of the body (after the common header and the post header,
        // before the 4-byte footer)
        gtid_event[std::size(gtid_event) - 6U] ^= std::byte{0x5A};
      }
      ++sequence_number;
      if (!(damaged && damage == transaction_damage::missing_gtid_event)) {
        store(gtid_event, false);
        transaction.events.push_back(std::move(gtid_event));
      }
      for (std::size_t event{0U}; event != std::size(spec.other_event_sizes);
           ++event) {
        auto bytes{make_event(spec.other_event_sizes[event], event_id++)};
        store(bytes, event + 1U == std::size(spec.other_event_sizes));
        transaction.events.push_back(std::move(bytes));
      }
      binlog.transactions.push_back(std::move(transaction));
    }
    storage.flush_event_buffer();
    result.push_back(std::move(binlog));
  }
  return result;
}

// the FORMAT_DESCRIPTION a GTID-mode dump sends for a stored one: the same,
// with the post header's create_timestamp zeroed (the simplified rule of
// sender_single_file_context::process_format_description(); MySQL clears it
// when auto-positioning chose the file) and the CRC32 footer recomputed
[[nodiscard]] byte_vector gtid_mode_format_description(byte_vector fde) {
  // binlog_version (2 bytes) and server_version (50 bytes) come first
  static constexpr std::size_t create_timestamp_offset{
      events::common_header_view_base::size_in_bytes + 2U + 50U};
  static constexpr std::size_t footer_size{4U};
  for (std::size_t index{0U}; index != 4U; ++index) {
    fde[create_timestamp_offset + index] = std::byte{0};
  }
  const auto payload_size{std::size(fde) - footer_size};
  const auto crc{util::calculate_crc32(
      util::const_byte_span{std::data(fde), payload_size})};
  for (std::size_t index{0U}; index != footer_size; ++index) {
    fde[payload_size + index] =
        static_cast<std::byte>((crc >> (8U * index)) & 0xFFU);
  }
  return fde;
}

// what a GTID-mode dump for 'replica' must stream, after each binlog's
// artificial ROTATE: the FORMAT_DESCRIPTION (see
// gtid_mode_format_description()), the PREVIOUS_GTIDS_LOG, then every
// transaction the replica lacks (anonymous ones always)
[[nodiscard]] std::vector<std::vector<byte_vector>>
expected_gtid_stream(const std::vector<stored_gtid_binlog> &binlogs,
                     const binsrv::gtids::gtid_set &replica) {
  std::vector<std::vector<byte_vector>> result;
  for (const auto &binlog : binlogs) {
    std::vector<byte_vector> events{
        gtid_mode_format_description(binlog.format_description),
        binlog.previous_gtids_log};
    for (const auto &transaction : binlog.transactions) {
      if (transaction.gtid.is_empty() || !replica.contains(transaction.gtid)) {
        events.insert(std::end(events), std::cbegin(transaction.events),
                      std::cend(transaction.events));
      }
    }
    result.push_back(std::move(events));
  }
  return result;
}

// small events, and now and then one larger than any block, so that some
// transactions are larger than the sender's read block
[[nodiscard]] gs::Generator<std::size_t> other_event_sizes() {
  return gs::one_of(
      {gs::integers<std::size_t>(
           {.min_value = events::common_header_view_base::size_in_bytes,
            .max_value = 300U}),
       gs::integers<std::size_t>(
           {.min_value = events::common_header_view_base::size_in_bytes,
            .max_value = 300U}),
       gs::integers<std::size_t>({.min_value = 3000U, .max_value = 9000U})});
}

[[nodiscard]] gs::Generator<transaction_spec> transaction_specs() {
  return gs::compose([](const hegel::TestCase &tc) {
    return transaction_spec{
        .kind = tc.draw(gs::sampled_from<gtid_kind>(
            {gtid_kind::untagged, gtid_kind::untagged, gtid_kind::tagged,
             gtid_kind::anonymous})),
        .uuid_index = tc.draw(gs::integers<std::size_t>(
            {.min_value = 0U, .max_value = std::size(uuid_pool) - 1U})),
        .tag_index = tc.draw(gs::integers<std::size_t>(
            {.min_value = 0U, .max_value = std::size(tag_pool) - 1U})),
        .other_event_sizes = tc.draw(
            gs::vectors(other_event_sizes(), {.min_size = 1U, .max_size = 4U}))};
  });
}

// the replica's executed GTIDs: any subset of the stored ones, sometimes
// with GTIDs of another server
[[nodiscard]] binsrv::gtids::gtid_set
draw_replica_set(hegel::TestCase &tc,
                 const std::vector<stored_gtid_binlog> &binlogs) {
  binsrv::gtids::gtid_set replica;
  for (const auto &binlog : binlogs) {
    for (const auto &transaction : binlog.transactions) {
      if (!transaction.gtid.is_empty() && tc.draw(gs::booleans())) {
        replica.add(transaction.gtid);
      }
    }
  }
  const auto foreign{tc.draw(
      "foreign_gtids",
      gs::integers<binsrv::gtids::gno_t>({.min_value = 0U, .max_value = 3U}))};
  for (binsrv::gtids::gno_t gno{1U}; gno <= foreign; ++gno) {
    replica.add(foreign_uuid, binsrv::gtids::tag{}, gno);
  }
  return replica;
}

} // anonymous namespace

// a GTID-mode dump streams, per binlog, an artificial ROTATE (at the magic
// offset; its checksum per the session for the first binlog, per the
// previous FORMAT_DESCRIPTION after that), the FORMAT_DESCRIPTION (its
// create_timestamp zeroed, CRC32 valid) and the PREVIOUS_GTIDS_LOG as
// stored, then exactly the transactions the replica
// lacks, whole and in stored order: anonymous ones always, ones whose GTID
// (untagged or tagged) the replica has never, also when a transaction is
// larger than the read block
BOOST_AUTO_TEST_CASE(GtidModeStreamsExactlyTheMissingTransactions) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    const auto binlogs{tc.draw(
        "binlogs", gs::vectors(gs::vectors(transaction_specs(),
                                           {.min_size = 0U, .max_size = 6U}),
                               {.min_size = 1U, .max_size = 3U}))};
    const bool session_checksum{tc.draw("checksum", gs::booleans())};
    const auto block_size{tc.draw("block", block_sizes())};

    const auto storage{make_storage(write_config(directory.path()))};
    const auto stored{store_gtid_binlogs(*storage, binlogs)};
    const auto replica{draw_replica_set(tc, stored)};
    const auto expected{expected_gtid_stream(stored, replica)};
    std::size_t expected_count{0U};
    for (const auto &binlog : expected) {
      expected_count += 1U + std::size(binlog);
    }

    std::ostringstream label;
    label << "replica has '" << replica << "', block " << block_size;
    operations::sender_context reader{std::make_shared<binsrv::null_logger>(),
                                      storage,
                                      block_size,
                                      binsrv::replication_mode_type::gtid,
                                      std::string_view{},
                                      events::magic_binlog_offset,
                                      replica,
                                      session_checksum};
    const auto outcome{stream_until_end(reader, expected_count + 1U)};
    require(!outcome.failed, label.str() + ": the GTID dump failed after " +
                                 std::to_string(std::size(outcome.events)) +
                                 " event(s)");
    require(std::size(outcome.events) == expected_count,
            label.str() + ": " + std::to_string(std::size(outcome.events)) +
                " event(s) streamed, expected " +
                std::to_string(expected_count));
    std::size_t index{0U};
    for (std::size_t binlog{0U}; binlog != std::size(expected); ++binlog) {
      const auto &rotate{outcome.events[index]};
      require(is_artificial_rotate_to(rotate, stored[binlog].name,
                                      events::magic_binlog_offset),
              label.str() + ": event " + std::to_string(index) +
                  " is not the artificial ROTATE to " +
                  stored[binlog].name.str());
      require(rotate_has_checksum(rotate) ==
                  (binlog == 0U ? session_checksum : true),
              label.str() + ": the artificial ROTATE to " +
                  stored[binlog].name.str() + " has the wrong checksum");
      ++index;
      for (std::size_t event{0U}; event != std::size(expected[binlog]);
           ++event, ++index) {
        require(outcome.events[index] == expected[binlog][event],
                label.str() + ": event " + std::to_string(index) + " (" +
                    stored[binlog].name.str() + ", expected event " +
                    std::to_string(event) +
                    ") differs from the stored transaction data");
      }
    }
  });
}

// a damaged transaction in the storage (a transaction_length one event
// short or 1-64 bytes too long, a missing GTID event, a GTID event whose
// checksum does not match) never makes a GTID-mode dump stream anything but
// stored events (the FORMAT_DESCRIPTION as a GTID-mode dump sends it), nor
// end the process: the dump either fails with an
// exception or streams exactly what it would without the damage
BOOST_AUTO_TEST_CASE(GtidModeRejectsDamagedTransactionsCleanly) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    auto transactions{tc.draw(
        "transactions",
        gs::vectors(transaction_specs(), {.min_size = 1U, .max_size = 6U}))};
    const auto damaged_index{tc.draw(
        "damaged_index",
        gs::integers<std::size_t>(
            {.min_value = 0U, .max_value = std::size(transactions) - 1U}))};
    const auto damage{tc.draw(
        "damage", gs::sampled_from<transaction_damage>(
                      {transaction_damage::length_short,
                       transaction_damage::length_long,
                       transaction_damage::missing_gtid_event,
                       transaction_damage::bad_checksum}))};
    if (damage == transaction_damage::length_short &&
        std::size(transactions[damaged_index].other_event_sizes) < 2U) {
      // a transaction one event short must keep at least one other event
      transactions[damaged_index].other_event_sizes.push_back(
          events::common_header_view_base::size_in_bytes);
    }
    const auto overrun{tc.draw(
        "overrun",
        gs::integers<std::size_t>({.min_value = 1U, .max_value = 64U}))};
    const bool session_checksum{tc.draw("checksum", gs::booleans())};
    const auto block_size{tc.draw("block", block_sizes())};

    // the undamaged stream, from a storage of its own
    const temp_dir clean_directory;
    const auto clean_storage{make_storage(write_config(clean_directory.path()))};
    const auto clean{store_gtid_binlogs(*clean_storage, {transactions})};

    const auto storage{make_storage(write_config(directory.path()))};
    const auto stored{
        store_gtid_binlogs(*storage, {transactions}, damage, damaged_index,
                           overrun)};
    const auto replica{draw_replica_set(tc, clean)};
    // the undamaged transactions with the damaged storage's own preamble
    // (the FORMAT_DESCRIPTION carries the time it was generated at)
    auto undamaged{clean};
    undamaged.front().format_description = stored.front().format_description;
    undamaged.front().previous_gtids_log = stored.front().previous_gtids_log;
    const auto expected{expected_gtid_stream(undamaged, replica)};

    std::vector<byte_vector> stored_events{
        gtid_mode_format_description(stored.front().format_description),
        stored.front().previous_gtids_log};
    for (const auto &transaction : stored.front().transactions) {
      stored_events.insert(std::end(stored_events),
                           std::cbegin(transaction.events),
                           std::cend(transaction.events));
    }
    std::ostringstream label;
    label << "damage " << static_cast<int>(damage) << " to transaction "
          << damaged_index << ", replica has '" << replica << "', block "
          << block_size;
    operations::sender_context reader{std::make_shared<binsrv::null_logger>(),
                                      storage,
                                      block_size,
                                      binsrv::replication_mode_type::gtid,
                                      std::string_view{},
                                      events::magic_binlog_offset,
                                      replica,
                                      session_checksum};
    const auto outcome{stream_until_end(reader, std::size(stored_events) + 1U)};
    for (std::size_t index{1U}; index < std::size(outcome.events); ++index) {
      require(std::ranges::find(stored_events, outcome.events[index]) !=
                  std::cend(stored_events),
              label.str() + ": event " + std::to_string(index) +
                  " is not a stored event");
    }
    if (!outcome.failed) {
      const auto &clean_events{expected.front()};
      bool same{std::size(outcome.events) == std::size(clean_events) + 1U};
      for (std::size_t index{0U}; same && index != std::size(clean_events);
           ++index) {
        same = outcome.events[index + 1U] == clean_events[index];
      }
      require(same, label.str() + ": the dump accepted the damage and "
                                  "streamed " +
                        std::to_string(std::size(outcome.events)) +
                        " event(s) that differ from the undamaged stream (" +
                        std::to_string(std::size(clean_events) + 1U) + ")");
    }
  });
}

// what GtidModeRejectsDamagedTransactionsCleanly found, made deterministic:
// a transaction the replica already has, whose transaction_length is too
// long, is skipped by that length to a point inside the next transaction's
// GTID event, whose bytes are then read as an event header. With an invalid
// type code there, the exception escapes a noexcept accessor of
// indexed_event_block and the process terminates (ticket 39, PBS-112). Every
// overrun of 1-64 bytes into a following untagged, tagged or anonymous
// transaction must instead fail the dump with an exception, or (when the
// bytes happen to line up) stream what the undamaged binlog holds
BOOST_AUTO_TEST_CASE(GtidModeSkipsATooLongTransactionWithoutTerminating) {
  for (const auto next_kind :
       {gtid_kind::untagged, gtid_kind::tagged, gtid_kind::anonymous}) {
    for (std::size_t overrun{1U}; overrun <= 64U; ++overrun) {
      const temp_dir directory;
      const std::vector<transaction_spec> transactions{
          {.kind = gtid_kind::untagged,
           .uuid_index = 0U,
           .tag_index = 0U,
           .other_event_sizes = {100U}},
          {.kind = next_kind,
           .uuid_index = 1U,
           .tag_index = 0U,
           .other_event_sizes = {100U}}};
      const auto storage{make_storage(write_config(directory.path()))};
      const auto stored{store_gtid_binlogs(*storage, {transactions},
                                           transaction_damage::length_long,
                                           0U, overrun)};
      binsrv::gtids::gtid_set replica;
      replica.add(stored.front().transactions.front().gtid);
      operations::sender_context reader{
          std::make_shared<binsrv::null_logger>(),
          storage,
          operations::sender_single_file_context::min_allowed_block_size,
          binsrv::replication_mode_type::gtid,
          std::string_view{},
          events::magic_binlog_offset,
          replica,
          /*session_source_binlog_checksum=*/true};
      const auto outcome{stream_until_end(reader, 8U)};
      const auto &second{stored.front().transactions.back().events};
      BOOST_TEST_INFO("overrun " << overrun << " into a "
                                 << static_cast<int>(next_kind)
                                 << " transaction");
      BOOST_CHECK(outcome.failed ||
                  (std::size(outcome.events) == 3U + std::size(second) &&
                   std::equal(std::cbegin(second), std::cend(second),
                              std::next(std::cbegin(outcome.events), 3))));
    }
  }
}

// a closed binlog whose PREVIOUS_GTIDS_LOG is damaged (its event_size claims
// 1-50000 bytes more than are stored, or the binlog ends inside its common
// header) makes a dump fail with an exception before streaming anything,
// from the beginning or from a position, whatever the size of the event (the
// sender's first preamble read may or may not hold it; when it does not, the
// sender reads the rest by the event's size: 'Failed to fetch the remainder
// of PREVIOUS_GTIDS_LOG event' / 'Failed to determine size of
// PREVIOUS_GTIDS_LOG event')
BOOST_AUTO_TEST_CASE(CorruptedPreviousGtidsLogFailsCleanly) {
  run_property([](hegel::TestCase &tc) {
    const temp_dir directory;
    const auto previous_gtids{
        tc.draw("previous_gtids", previous_gtid_sets())};
    const bool truncate_header{tc.draw("truncate_header", gs::booleans())};
    const auto excess{tc.draw(
        "excess",
        gs::integers<std::uint32_t>({.min_value = 1U, .max_value = 50'000U}))};
    const auto kept_header_bytes{tc.draw(
        "kept_header_bytes",
        gs::integers<std::size_t>(
            {.min_value = 1U,
             .max_value = events::common_header_view_base::size_in_bytes -
                          1U}))};
    const bool from_position{tc.draw("from_position", gs::booleans())};
    const bool session_checksum{tc.draw("checksum", gs::booleans())};
    const auto block_size{tc.draw("block", block_sizes())};

    const auto storage{make_storage(write_config(directory.path()))};
    require(storage->open_binlog(the_binlog) ==
                binsrv::open_binlog_status::created,
            "the binlog was not created");
    const auto fde{make_fde()};
    append_flushed(*storage, util::const_byte_span{fde});
    auto previous_gtids_log{make_previous_gtids_log(
        events::magic_binlog_offset + std::size(fde), server_id,
        previous_gtids)};
    const auto stored_end{events::magic_binlog_offset + std::size(fde) +
                          std::size(previous_gtids_log)};
    if (truncate_header) {
      previous_gtids_log.resize(kept_header_bytes);
    } else {
      util::byte_span size_field{
          std::next(std::data(previous_gtids_log),
                    static_cast<std::ptrdiff_t>(
                        events::common_header_view_base::event_size_offset)),
          sizeof(std::uint32_t)};
      util::insert_fixed_int_to_byte_span(
          size_field,
          static_cast<std::uint32_t>(std::size(previous_gtids_log) + excess));
    }
    append_flushed(*storage, util::const_byte_span{previous_gtids_log});
    storage->close_binlog();
    require(storage->open_binlog(the_next_binlog) ==
                binsrv::open_binlog_status::created,
            "the next binlog was not created");

    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        storage,
        block_size,
        binsrv::replication_mode_type::position,
        from_position ? the_binlog.str() : std::string{},
        from_position ? stored_end : events::magic_binlog_offset,
        binsrv::gtids::gtid_set{},
        session_checksum};
    const auto outcome{stream_until_end(reader, 4U)};
    require(outcome.failed,
            std::string{truncate_header ? "a PREVIOUS_GTIDS_LOG cut inside its "
                                          "header"
                                        : "a PREVIOUS_GTIDS_LOG longer than "
                                          "the binlog"} +
                " did not fail the dump (" +
                std::to_string(std::size(outcome.events)) +
                " event(s) streamed)");
    require(outcome.events.empty(),
            "events were streamed before the damaged preamble was refused");
  });
}
