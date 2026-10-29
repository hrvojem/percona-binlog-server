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

// libFuzzer target: stored binlog data read back by the replication source
// of 'pull' (a corrupted or foreign binlog in the storage). The input after
// two selector bytes is stored as the content of binlog.000001 (after the
// binlog magic) through a real filesystem storage, and an
// operations::sender_context streams it as a COM_BINLOG_DUMP would, until it
// waits for more data or rejects the data with an exception. The selectors
// pick: a dump from the beginning or from an event boundary (the second
// byte chooses which), the session's @source_binlog_checksum, the block size,
// and whether the binlog is closed with binlog.000002 opened after it (the
// switch to the next binlog). A crash, a std::terminate or a sanitizer report
// is a bug.
//
// Known bug filters (known_bug_filters.hpp): a FORMAT_DESCRIPTION with an
// unparsable server version (PBS-68), and an event header with an invalid
// type code on the chain of events the sender walks (PBS-112, which a
// position inside an event or corrupted data reaches); the input then counts
// as rejected. -DFUZZ_KEEP_PBS_68 / -DFUZZ_KEEP_PBS_112 keep the crashes.
//
// Each input gets a fresh storage in a directory under $TMPDIR (default
// /dev/shm, a RAM disk), removed when the process ends.

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

#include "known_bug_filters.hpp"

#include "binsrv/main_config.hpp"
#include "binsrv/null_logger.hpp"
#include "binsrv/replication_mode_type.hpp"
#include "binsrv/storage.hpp"

#include "binsrv/events/common_header_view.hpp"
#include "binsrv/events/composite_binlog_name.hpp"
#include "binsrv/gtids/gtid.hpp"
#include "binsrv/gtids/gtid_set.hpp"

#include "operations/sender_context.hpp"
#include "operations/sender_single_file_context.hpp"

#include "util/byte_span.hpp"
#include "util/ctime_timestamp.hpp"

namespace {

using header = binsrv::events::common_header_view_base;

// the events a COM_BINLOG_DUMP may stream before the stream must have
// stopped; well above what a 64 KiB input can hold
constexpr std::size_t max_events{100'000U};

class workspace {
public:
  workspace() {
    const char *base{std::getenv("TMPDIR")}; // NOLINT(concurrency-mt-unsafe)
    root_ = std::filesystem::path{base != nullptr ? base : "/dev/shm"} /
            ("pbs-stored-binlog-fuzzer-" + std::to_string(::getpid()));
    std::filesystem::create_directories(root_);
    config_path_ = root_ / "config.json";
    std::ofstream config{config_path_};
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
           << storage_path().generic_string() << R"(" }
})";
  }
  workspace(const workspace &) = delete;
  workspace &operator=(const workspace &) = delete;
  workspace(workspace &&) = delete;
  workspace &operator=(workspace &&) = delete;
  ~workspace() {
    std::error_code ignored;
    std::filesystem::remove_all(root_, ignored);
  }

  [[nodiscard]] std::filesystem::path storage_path() const {
    return root_ / "storage";
  }
  [[nodiscard]] const std::filesystem::path &config_path() const {
    return config_path_;
  }

private:
  std::filesystem::path root_;
  std::filesystem::path config_path_;
};

[[nodiscard]] workspace &get_workspace() {
  static workspace instance;
  return instance;
}

[[nodiscard]] std::uint32_t read_event_size(util::const_byte_span bytes,
                                            std::size_t offset) {
  std::uint32_t size{0U};
  for (std::size_t index{0U}; index < sizeof size; ++index) {
    size |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(
                bytes[offset + header::event_size_offset + index]))
            << (8U * index);
  }
  return size;
}

// the offsets (in the stored data, after the magic) of the events chained by
// their event_size fields, up to the end or the first size that does not fit;
// std::nullopt when a header on the chain trips a known bug filter
[[nodiscard]] std::optional<std::vector<std::size_t>>
event_chain(util::const_byte_span data) {
  std::vector<std::size_t> offsets;
  std::size_t offset{0U};
  while (std::size(data) - offset >= header::size_in_bytes) {
    const auto event{data.subspan(offset)};
#ifndef FUZZ_KEEP_PBS_112
    if (fuzz_filters::has_invalid_type_code(event)) {
      return std::nullopt;
    }
#endif
#ifndef FUZZ_KEEP_PBS_68
    if (fuzz_filters::has_unparsable_server_version(event)) {
      return std::nullopt;
    }
#endif
    offsets.push_back(offset);
    const auto size{read_event_size(data, offset)};
    if (size < header::size_in_bytes || size > std::size(data) - offset) {
      break;
    }
    offset += size;
  }
  return offsets;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *input,
                                      std::size_t size) {
  static constexpr std::size_t selector_size{2U};
  if (size < selector_size) {
    return 0;
  }
  const auto flags{input[0]};
  const auto boundary_choice{input[1]};
  const bool from_position{(flags & 1U) != 0U};
  const bool session_checksum{(flags & 2U) != 0U};
  const bool switch_to_next{(flags & 4U) != 0U};
  const std::size_t block_size{
      operations::sender_single_file_context::min_allowed_block_size *
      (1U + ((flags >> 3U) & 3U))};
  const util::const_byte_span data{
      reinterpret_cast<const std::byte *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
          std::next(input, selector_size)),
      size - selector_size};

  const auto chain{event_chain(data)};
  if (!chain.has_value()) {
    return 0;
  }
  std::uint64_t position{binsrv::events::magic_binlog_offset};
  if (from_position && !chain->empty()) {
    position += (*chain)[boundary_choice % std::size(*chain)];
  }

  auto &space{get_workspace()};
  std::error_code ignored;
  std::filesystem::remove_all(space.storage_path(), ignored);
  std::filesystem::create_directories(space.storage_path());
  try {
    const binsrv::main_config config{space.config_path().string()};
    const auto storage{std::make_shared<binsrv::storage>(
        std::make_shared<binsrv::null_logger>(), config,
        binsrv::storage_construction_mode_type::streaming)};
    const binsrv::events::composite_binlog_name first{"binlog", 1U};
    if (storage->open_binlog(first) != binsrv::open_binlog_status::created) {
      return 0;
    }
    if (!data.empty()) {
      // the storage does not parse what it stores: the whole input is the
      // binlog's content after the magic
      storage->write_event(data, /*at_transaction_boundary=*/true,
                           binsrv::gtids::gtid{},
                           util::ctime_timestamp{std::time_t{1}}, 0ULL);
      storage->flush_event_buffer();
    }
    if (switch_to_next) {
      storage->close_binlog();
      [[maybe_unused]] const auto status{
          storage->open_binlog(first.next())};
    }

    operations::sender_context reader{
        std::make_shared<binsrv::null_logger>(),
        storage,
        block_size,
        binsrv::replication_mode_type::position,
        from_position ? first.str() : std::string{},
        position,
        binsrv::gtids::gtid_set{},
        session_checksum};
    util::const_byte_span event{};
    for (std::size_t count{0U}; count < max_events; ++count) {
      if (reader.get_event(event) !=
          operations::event_fetching_result_type::success) {
        break;
      }
    }
  } catch (const std::exception &) {
    // a clean rejection
  }
  return 0;
}
