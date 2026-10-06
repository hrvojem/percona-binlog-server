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

// Stateful property-based tests for binsrv::storage (streaming mode, local
// filesystem backend), built with Hegel (https://hegel.dev).
//
// Each test case generates a sequence of operations - writing events (parts
// of transactions or transaction boundaries), discarding the incomplete
// transaction, explicit flushes, binlog rotation, restarting the storage and
// "crashing" it (copying the storage directory as it is on disk at that
// moment and opening the copy) - and runs it both against a real storage in
// a temporary directory and against a simple model: a list of binlogs, each
// holding its complete transactions, plus the bytes of the transaction that
// is still being received.
//
// After every operation the files on disk are compared with the model:
// - a binlog file holds the binlog magic followed by a prefix of the
//   binlog's complete transactions (never a part of a transaction);
// - binlogs that were closed hold all of their transactions;
// - binlog records (size, GTIDs, timestamps, last sequence number) describe
//   exactly what is on disk;
// - the current position accounts for every byte written so far;
// - with 'storage.checkpoint_size' set to S, complete transactions that are
//   not yet on disk never add up to S bytes or more;
// - reading the binlogs back through operations::sender_context (the reader
//   behind COM_BINLOG_DUMP) returns exactly the events of the complete
//   transactions on disk, in order, for any block size - both for a reader
//   that follows the storage as it grows and for a fresh reader.
//
// The generated events are not real binlog events, but each one starts with
// a valid 19-byte common header (type code, event size), which is all the
// reading side relies on.

#include <algorithm>
#include <cassert>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

// needed for binsrv::gtids::gtid_set_storage
#include <boost/container/small_vector.hpp> // IWYU pragma: keep

#define BOOST_TEST_MODULE StoragePropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "property_test_helpers.hpp"

#include "binsrv/main_config.hpp"
#include "binsrv/null_logger.hpp"
#include "binsrv/storage.hpp"
#include "binsrv/storage_core.hpp"

#include "binsrv/events/code_type.hpp"
#include "binsrv/events/common_header_view.hpp"
#include "binsrv/events/composite_binlog_name.hpp"
#include "binsrv/events/protocol_traits_fwd.hpp"

#include "binsrv/gtids/common_types.hpp"
#include "binsrv/gtids/gtid.hpp"
#include "binsrv/gtids/gtid_set.hpp"
#include "binsrv/gtids/uuid.hpp"

#include "operations/sender_context.hpp"

#include "util/byte_span_fwd.hpp"
#include "util/byte_span_inserters.hpp"
#include "util/ctime_timestamp.hpp"
#include "util/ctime_timestamp_range.hpp"

namespace {

namespace gs = hegel::generators;

using property_testing::require;
using property_testing::run_property;

constexpr std::string_view binlog_base_name{"binlog"};
constexpr std::string_view transaction_uuid{
    "11111111-aaaa-1111-aaaa-111111111111"};

// ---------------------------------------------------------------------------
// temporary directories and configuration
// ---------------------------------------------------------------------------

// a uniquely named directory under the system temporary directory, removed
// together with its content on destruction
class scratch_directory {
public:
  scratch_directory() : path_{make_unique_path()} {
    std::filesystem::create_directories(path_);
  }
  scratch_directory(const scratch_directory &) = delete;
  scratch_directory &operator=(const scratch_directory &) = delete;
  scratch_directory(scratch_directory &&) = delete;
  scratch_directory &operator=(scratch_directory &&) = delete;
  ~scratch_directory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  [[nodiscard]] const std::filesystem::path &path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;

  [[nodiscard]] static std::filesystem::path make_unique_path() {
    static std::atomic<std::uint64_t> counter{0ULL};
    const auto ticks{
        std::chrono::steady_clock::now().time_since_epoch().count()};
    return std::filesystem::temp_directory_path() /
           ("pbs_storage_property_" + std::to_string(ticks) + "_" +
            std::to_string(counter++));
  }
};

struct storage_settings {
  bool gtid_mode;
  std::optional<std::uint64_t> checkpoint_size;
  // block sizes used by the sender_context readers
  std::size_t tailing_reader_block_size;
  std::size_t fresh_reader_block_size;
};

std::ostream &operator<<(std::ostream &output,
                         const storage_settings &settings) {
  output << (settings.gtid_mode ? "gtid" : "position") << " mode, ";
  if (settings.checkpoint_size.has_value()) {
    output << "checkpoint_size " << *settings.checkpoint_size;
  } else {
    output << "no checkpoint_size";
  }
  return output << ", reader block sizes " << settings.tailing_reader_block_size
                << " / " << settings.fresh_reader_block_size;
}

// writes a configuration file for a local filesystem storage in
// 'storage_directory'; the connection / replication source sections are
// required by the configuration schema but are not used by the storage
[[nodiscard]] std::filesystem::path
write_config(const std::filesystem::path &directory,
             const std::filesystem::path &storage_directory,
             const storage_settings &settings) {
  std::ostringstream storage_section;
  storage_section << R"("backend": "file", "uri": "file://)"
                  << storage_directory.generic_string() << '"';
  if (settings.checkpoint_size.has_value()) {
    storage_section << R"(, "checkpoint_size": ")" << *settings.checkpoint_size
                    << '"';
  }

  const auto config_path{directory /
                         ("config_" + storage_directory.filename().string() +
                          ".json")};
  std::ofstream config{config_path};
  config << R"({
  "logger": { "level": "error", "file": "" },
  "connection": {
    "host": "127.0.0.1", "port": 3306, "user": "root", "password": "",
    "connect_timeout": 20, "read_timeout": 60, "write_timeout": 60
  },
  "replication": {
    "server_id": 42, "idle_time": 10, "verify_checksum": true,
    "mode": ")"
         << (settings.gtid_mode ? "gtid" : "position") << R"("
  },
  "replication_source": {
    "port": 3307, "read_timeout": 60, "write_timeout": 60,
    "authentication": {
      "user": "rpl", "password": "password", "plugin": "caching_sha2_password"
    }
  },
  "storage": { )"
         << storage_section.str() << R"( }
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

[[nodiscard]] std::vector<std::byte>
read_file(const std::filesystem::path &path) {
  std::ifstream input{path, std::ios::binary};
  if (!input) {
    throw std::runtime_error{"cannot open '" + path.string() + "'"};
  }
  std::vector<char> raw{std::istreambuf_iterator<char>{input},
                        std::istreambuf_iterator<char>{}};
  std::vector<std::byte> result(std::size(raw));
  std::ranges::transform(raw, std::begin(result), [](char value) {
    return static_cast<std::byte>(static_cast<unsigned char>(value));
  });
  return result;
}

// ---------------------------------------------------------------------------
// operations
// ---------------------------------------------------------------------------

enum class operation_kind : std::uint8_t {
  write_event,
  discard_incomplete_transaction,
  flush,
  rotate,
  restart,
  crash
};

struct operation {
  operation_kind kind;
  // only for write_event
  std::size_t event_size;
  bool at_transaction_boundary;
  std::uint32_t timestamp;
};

std::ostream &operator<<(std::ostream &output, const operation &op) {
  switch (op.kind) {
  case operation_kind::write_event:
    return output << "write_event(" << op.event_size << " bytes"
                  << (op.at_transaction_boundary ? ", end of transaction" : "")
                  << ", ts " << op.timestamp << ')';
  case operation_kind::discard_incomplete_transaction:
    return output << "discard_incomplete_transaction";
  case operation_kind::flush:
    return output << "flush";
  case operation_kind::rotate:
    return output << "rotate";
  case operation_kind::restart:
    return output << "restart";
  case operation_kind::crash:
    return output << "crash";
  }
  return output;
}

constexpr std::size_t min_event_size{
    binsrv::events::common_header_view_base::size_in_bytes};
constexpr std::size_t max_event_size{512U};

[[nodiscard]] gs::Generator<operation> operations() {
  return gs::compose([](const hegel::TestCase &tc) {
    // writes are listed several times so that most sequences build up
    // enough data to exercise checkpointing
    const auto kind{tc.draw(gs::sampled_from(
        {operation_kind::write_event, operation_kind::write_event,
         operation_kind::write_event, operation_kind::write_event,
         operation_kind::write_event, operation_kind::write_event,
         operation_kind::discard_incomplete_transaction, operation_kind::flush,
         operation_kind::rotate, operation_kind::restart,
         operation_kind::crash}))};
    operation result{.kind = kind,
                     .event_size = 0U,
                     .at_transaction_boundary = false,
                     .timestamp = 0U};
    if (kind == operation_kind::write_event) {
      result.event_size = tc.draw(gs::integers<std::size_t>(
          {.min_value = min_event_size, .max_value = max_event_size}));
      result.at_transaction_boundary = tc.draw(gs::booleans());
      result.timestamp = tc.draw(gs::integers<std::uint32_t>(
          {.min_value = 0U,
           .max_value = static_cast<std::uint32_t>(
               std::numeric_limits<std::int32_t>::max())}));
    }
    return result;
  });
}

[[nodiscard]] gs::Generator<storage_settings> storage_settings_generator() {
  return gs::compose([](const hegel::TestCase &tc) {
    static constexpr std::uint64_t min_checkpoint_size{64ULL};
    static constexpr std::uint64_t max_checkpoint_size{4096ULL};
    // block sizes smaller than one event make the reader re-fetch with the
    // exact event size, larger ones make it split blocks into several events;
    // a block must hold at least one common header (the server uses 1 MiB
    // blocks, and sender_context treats a shorter block as corruption)
    static constexpr std::size_t max_small_block_size{64U};
    static constexpr std::size_t max_block_size{4096U};
    const auto block_sizes{gs::one_of(
        {gs::integers<std::size_t>(
             {.min_value = min_event_size, .max_value = max_small_block_size}),
         gs::integers<std::size_t>(
             {.min_value = min_event_size, .max_value = max_block_size})})};
    storage_settings result{.gtid_mode = tc.draw(gs::booleans()),
                            .checkpoint_size = std::nullopt,
                            .tailing_reader_block_size = 0U,
                            .fresh_reader_block_size = 0U};
    if (tc.draw(gs::booleans())) {
      result.checkpoint_size = tc.draw(gs::integers<std::uint64_t>(
          {.min_value = min_checkpoint_size,
           .max_value = max_checkpoint_size}));
    }
    result.tailing_reader_block_size = tc.draw(block_sizes);
    result.fresh_reader_block_size = tc.draw(block_sizes);
    return result;
  });
}

// ---------------------------------------------------------------------------
// the model
// ---------------------------------------------------------------------------

struct model_event {
  std::vector<std::byte> bytes;
  util::ctime_timestamp timestamp;
};

struct model_transaction {
  std::vector<model_event> events;
  binsrv::gtids::gno_t gno;

  [[nodiscard]] std::size_t size() const {
    return std::accumulate(std::cbegin(events), std::cend(events), 0UZ,
                           [](std::size_t total, const model_event &event) {
                             return total + std::size(event.bytes);
                           });
  }
};

struct model_binlog {
  binsrv::events::composite_binlog_name name;
  std::vector<model_transaction> transactions;
  bool closed;
};

class storage_model {
public:
  storage_model() {
    binlogs_.push_back({.name = binsrv::events::composite_binlog_name{
                            binlog_base_name, 1U},
                        .transactions = {},
                        .closed = false});
  }

  [[nodiscard]] const std::vector<model_binlog> &get_binlogs() const noexcept {
    return binlogs_;
  }
  [[nodiscard]] const model_binlog &get_current_binlog() const {
    return binlogs_.back();
  }
  [[nodiscard]] const std::vector<model_event> &
  get_incomplete_events() const noexcept {
    return incomplete_.events;
  }
  [[nodiscard]] std::size_t get_incomplete_size() const {
    return incomplete_.size();
  }
  // the GNO / sequence number of the transaction being received: every
  // complete transaction takes the next one, a discarded transaction gives
  // its number back (it is going to be received again)
  [[nodiscard]] binsrv::gtids::gno_t get_current_gno() const noexcept {
    return committed_transactions_ + 1U;
  }
  [[nodiscard]] bool is_first_event_of_transaction() const noexcept {
    return incomplete_.events.empty();
  }

  void write_event(std::vector<std::byte> bytes,
                   util::ctime_timestamp timestamp, bool at_boundary) {
    incomplete_.gno = get_current_gno();
    incomplete_.events.push_back({.bytes = std::move(bytes),
                                  .timestamp = timestamp});
    if (at_boundary) {
      binlogs_.back().transactions.push_back(std::move(incomplete_));
      incomplete_ = {};
      ++committed_transactions_;
    }
  }

  // discarding the incomplete transaction, closing the binlog, restarting
  // and crashing all lose the events of the transaction being received
  void drop_incomplete_transaction() { incomplete_ = {}; }

  void rotate() {
    drop_incomplete_transaction();
    binlogs_.back().closed = true;
    binlogs_.push_back({.name = binlogs_.back().name.next(),
                        .transactions = {},
                        .closed = false});
  }

private:
  std::vector<model_binlog> binlogs_;
  model_transaction incomplete_{};
  std::uint64_t committed_transactions_{0ULL};
};

// ---------------------------------------------------------------------------
// comparing what is on disk with the model
// ---------------------------------------------------------------------------

[[nodiscard]] std::string describe_binlog(const model_binlog &binlog) {
  return "binlog '" + binlog.name.str() + "'";
}

// returns how many of the binlog's transactions are on disk, checking that
// the file holds the magic followed by exactly that many complete
// transactions
[[nodiscard]] std::size_t
match_file_with_model(const std::vector<std::byte> &file_content,
                      const model_binlog &binlog) {
  const auto magic_size{std::size(binsrv::events::magic_binlog_payload)};
  require(std::size(file_content) >= magic_size &&
              std::equal(std::cbegin(binsrv::events::magic_binlog_payload),
                         std::cend(binsrv::events::magic_binlog_payload),
                         std::cbegin(file_content)),
          describe_binlog(binlog) + " does not start with the binlog magic");

  std::size_t offset{magic_size};
  std::size_t transactions_on_disk{0U};
  while (offset < std::size(file_content)) {
    require(transactions_on_disk < std::size(binlog.transactions),
            describe_binlog(binlog) +
                " holds more data than its complete transactions (" +
                std::to_string(std::size(file_content)) + " bytes)");
    for (const auto &event :
         binlog.transactions[transactions_on_disk].events) {
      require(offset + std::size(event.bytes) <= std::size(file_content),
              describe_binlog(binlog) + " ends in the middle of transaction " +
                  std::to_string(transactions_on_disk + 1U) + " (" +
                  std::to_string(std::size(file_content)) + " bytes)");
      require(std::equal(std::cbegin(event.bytes), std::cend(event.bytes),
                         std::next(std::cbegin(file_content),
                                   static_cast<std::ptrdiff_t>(offset))),
              describe_binlog(binlog) + " has unexpected content at offset " +
                  std::to_string(offset));
      offset += std::size(event.bytes);
    }
    ++transactions_on_disk;
  }
  return transactions_on_disk;
}

[[nodiscard]] binsrv::gtids::gtid_set
model_gtids(const model_binlog &binlog, std::size_t number_of_transactions) {
  const binsrv::gtids::uuid source_uuid{transaction_uuid};
  binsrv::gtids::gtid_set result{};
  for (std::size_t index{0U}; index < number_of_transactions; ++index) {
    result += binsrv::gtids::gtid{source_uuid, binlog.transactions[index].gno};
  }
  return result;
}

void require_record_matches(const binsrv::binlog_record &record,
                            const model_binlog &binlog,
                            std::size_t transactions_on_disk,
                            std::uint64_t file_size,
                            const binsrv::gtids::gtid_set &earlier_gtids,
                            bool gtid_mode) {
  const auto label{describe_binlog(binlog) + " record: "};
  require(record.name == binlog.name, label + "unexpected name '" +
                                          record.name.str() + "'");
  require(record.size == file_size,
          label + "size " + std::to_string(record.size) +
              " does not match the file size " + std::to_string(file_size));

  if (gtid_mode) {
    const auto expected_added{model_gtids(binlog, transactions_on_disk)};
    require(record.added_gtids.has_value() &&
                *record.added_gtids == expected_added,
            label + "added_gtids '" +
                (record.added_gtids ? record.added_gtids->str() : "<none>") +
                "' instead of '" + expected_added.str() + "'");
    require(record.previous_gtids.has_value() &&
                *record.previous_gtids == earlier_gtids,
            label + "previous_gtids '" +
                (record.previous_gtids ? record.previous_gtids->str()
                                       : "<none>") +
                "' instead of '" + earlier_gtids.str() + "'");
  } else {
    require(!record.added_gtids.has_value() &&
                !record.previous_gtids.has_value(),
            label + "has GTIDs in position-based replication mode");
  }

  util::ctime_timestamp_range expected_timestamps{};
  for (std::size_t index{0U}; index < transactions_on_disk; ++index) {
    for (const auto &event : binlog.transactions[index].events) {
      expected_timestamps.add_timestamp(event.timestamp);
    }
  }
  const auto &actual_timestamps{record.timestamps};
  const bool timestamps_match{
      actual_timestamps.is_empty() == expected_timestamps.is_empty() &&
      (expected_timestamps.is_empty() ||
       (actual_timestamps.get_min_timestamp() ==
            expected_timestamps.get_min_timestamp() &&
        actual_timestamps.get_max_timestamp() ==
            expected_timestamps.get_max_timestamp()))};
  require(timestamps_match,
          label + "timestamps do not match the events on disk");

  const std::uint64_t expected_sequence_number{
      transactions_on_disk == 0U
          ? 0ULL
          : binlog.transactions[transactions_on_disk - 1U].gno};
  require(record.last_sequence_number == expected_sequence_number,
          label + "last_sequence_number " +
              std::to_string(record.last_sequence_number) + " instead of " +
              std::to_string(expected_sequence_number));
}

struct disk_state {
  // for the current (last) binlog
  std::uint64_t file_size;
  std::size_t transactions_on_disk;
  // for every binlog, in order
  std::vector<std::size_t> transactions_on_disk_per_binlog;
};

// checks every binlog file and record against the model
[[nodiscard]] disk_state
require_disk_matches_model(const std::filesystem::path &storage_directory,
                           const binsrv::binlog_record_container &records,
                           const storage_model &model, bool gtid_mode) {
  const auto &binlogs{model.get_binlogs()};
  require(std::size(records) == std::size(binlogs),
          std::to_string(std::size(records)) + " binlog records instead of " +
              std::to_string(std::size(binlogs)));

  binsrv::gtids::gtid_set earlier_gtids{};
  disk_state result{.file_size = 0ULL,
                    .transactions_on_disk = 0U,
                    .transactions_on_disk_per_binlog = {}};
  for (std::size_t index{0U}; index < std::size(binlogs); ++index) {
    const auto &binlog{binlogs[index]};
    const auto file_content{read_file(storage_directory / binlog.name.str())};
    const auto transactions_on_disk{
        match_file_with_model(file_content, binlog)};
    if (binlog.closed) {
      require(transactions_on_disk == std::size(binlog.transactions),
              describe_binlog(binlog) + " was closed with " +
                  std::to_string(std::size(binlog.transactions) -
                                 transactions_on_disk) +
                  " complete transaction(s) missing");
    }
    require_record_matches(records[index], binlog, transactions_on_disk,
                           std::size(file_content), earlier_gtids, gtid_mode);
    earlier_gtids += model_gtids(binlog, transactions_on_disk);
    result.file_size = std::size(file_content);
    result.transactions_on_disk = transactions_on_disk;
    result.transactions_on_disk_per_binlog.push_back(transactions_on_disk);
  }
  return result;
}

[[nodiscard]] std::uint64_t
unflushed_complete_size(const model_binlog &binlog,
                        std::size_t transactions_on_disk) {
  std::uint64_t result{0ULL};
  for (std::size_t index{transactions_on_disk};
       index < std::size(binlog.transactions); ++index) {
    result += binlog.transactions[index].size();
  }
  return result;
}

// ---------------------------------------------------------------------------
// reading binlogs back
// ---------------------------------------------------------------------------

using event_list = std::vector<std::vector<std::byte>>;

// the events a reader must return: those of the complete transactions on
// disk, binlog after binlog
[[nodiscard]] event_list expected_events(const storage_model &model,
                                         const disk_state &state) {
  event_list result;
  const auto &binlogs{model.get_binlogs()};
  for (std::size_t index{0U}; index < std::size(binlogs); ++index) {
    const auto &binlog{binlogs[index]};
    for (std::size_t transaction_index{0U};
         transaction_index < state.transactions_on_disk_per_binlog[index];
         ++transaction_index) {
      for (const auto &event : binlog.transactions[transaction_index].events) {
        result.push_back(event.bytes);
      }
    }
  }
  return result;
}

// reads events until the reader reports that there is no more data
void read_until_end(operations::sender_context &reader, event_list &received,
                    std::size_t max_events, std::string_view label) {
  util::const_byte_span event{};
  for (std::size_t reads{0U};; ++reads) {
    require(reads <= max_events,
            std::string{label} + ": the reader returned more events than "
                                 "are on disk");
    require(reader.get_event(event),
            std::string{label} + ": sender_context::get_event() failed after " +
                std::to_string(std::size(received)) + " event(s)");
    if (event.empty()) {
      return;
    }
    received.emplace_back(std::cbegin(event), std::cend(event));
  }
}

void require_same_events(const event_list &received,
                         const event_list &expected, std::string_view label) {
  const auto common{std::min(std::size(received), std::size(expected))};
  for (std::size_t index{0U}; index < common; ++index) {
    require(received[index] == expected[index],
            std::string{label} + ": event " + std::to_string(index + 1U) +
                " differs from the one on disk (" +
                std::to_string(std::size(received[index])) + " bytes read, " +
                std::to_string(std::size(expected[index])) + " expected)");
  }
  require(std::size(received) == std::size(expected),
          std::string{label} + ": read " + std::to_string(std::size(received)) +
              " event(s) instead of " + std::to_string(std::size(expected)));
}

// ---------------------------------------------------------------------------
// running a sequence of operations
// ---------------------------------------------------------------------------

enum class checked_properties : std::uint8_t {
  model_agreement,
  checkpoint_size_bound,
  read_back
};

class storage_harness {
public:
  storage_harness(const storage_settings &settings,
                  checked_properties properties)
      : settings_{settings}, properties_{properties},
        storage_directory_{root_.path() / "storage"},
        config_path_{prepare_storage_directory(storage_directory_)} {
    storage_ = make_storage(config_path_);
    const auto status{storage_->open_binlog(model_.get_current_binlog().name)};
    require(status == binsrv::open_binlog_status::created,
            "the first binlog was not reported as created");
    restart_tailing_reader();
    check();
  }

  void apply(const operation &op) {
    switch (op.kind) {
    case operation_kind::write_event:
      write_event(op);
      break;
    case operation_kind::discard_incomplete_transaction:
      storage_->discard_incomplete_transaction_events();
      model_.drop_incomplete_transaction();
      break;
    case operation_kind::flush:
      storage_->flush_event_buffer();
      break;
    case operation_kind::rotate:
      // the collector closes a binlog only at a transaction boundary (ROTATE
      // / STOP events) or after discarding the incomplete transaction on a
      // disconnect ('storage::open_binlog()' asserts that no incomplete
      // transaction state is left), so the same order is followed here
      storage_->discard_incomplete_transaction_events();
      storage_->close_binlog();
      model_.rotate();
      require(storage_->open_binlog(model_.get_current_binlog().name) ==
                  binsrv::open_binlog_status::created,
              "the next binlog was not reported as created");
      break;
    case operation_kind::restart:
      // destroying the storage flushes complete transactions, the
      // incomplete one is lost (the tailing reader shares ownership of the
      // storage, so it has to go first for the storage to be destroyed here)
      tailing_reader_.reset();
      storage_.reset();
      model_.drop_incomplete_transaction();
      storage_ = make_storage(config_path_);
      reopen_current_binlog(*storage_, "restart");
      // a reader belongs to a storage instance, a replica reconnecting after
      // a restart starts reading from the beginning again
      restart_tailing_reader();
      break;
    case operation_kind::crash:
      crash();
      break;
    }
    check();
  }

private:
  storage_settings settings_;
  checked_properties properties_;
  scratch_directory root_{};
  std::filesystem::path storage_directory_;
  std::filesystem::path config_path_;
  binsrv::storage_ptr storage_{};
  storage_model model_{};
  std::uint64_t event_counter_{0ULL};
  // a reader that is kept across operations and follows the storage as it
  // grows, as a connected replica does
  std::unique_ptr<operations::sender_context> tailing_reader_{};
  event_list tailing_received_{};

  void restart_tailing_reader() {
    tailing_reader_.reset();
    tailing_received_.clear();
    if (properties_ == checked_properties::read_back) {
      tailing_reader_ = std::make_unique<operations::sender_context>(
          std::make_shared<binsrv::null_logger>(), storage_,
          settings_.tailing_reader_block_size);
    }
  }

  [[nodiscard]] std::filesystem::path
  prepare_storage_directory(const std::filesystem::path &directory) {
    std::filesystem::create_directories(directory);
    return write_config(root_.path(), directory, settings_);
  }

  [[nodiscard]] std::vector<std::byte>
  next_event_bytes(std::size_t size, std::uint32_t timestamp) {
    using header = binsrv::events::common_header_view_base;
    // distinct content for every event, so that misplaced data is detected
    static constexpr std::uint64_t event_multiplier{131ULL};
    static constexpr std::uint64_t byte_multiplier{7ULL};
    std::vector<std::byte> result(size);
    const auto seed{event_counter_++ * event_multiplier};
    for (std::size_t index{0U}; index < size; ++index) {
      result[index] =
          static_cast<std::byte>((seed + index * byte_multiplier) & 0xFFULL);
    }

    // a valid common header (the reader relies on the type code and the
    // event size), the event-specific part is left as the pattern above
    assert(size >= header::size_in_bytes);
    const auto write_field{[&result](std::size_t offset, auto value) {
      util::byte_span field{std::next(std::data(result),
                                      static_cast<std::ptrdiff_t>(offset)),
                            sizeof value};
      util::insert_fixed_int_to_byte_span(field, value);
    }};
    write_field(header::timestamp_offset, timestamp);
    write_field(header::type_code_offset,
                static_cast<std::uint8_t>(binsrv::events::code_type::query));
    write_field(header::event_size_offset, static_cast<std::uint32_t>(size));
    return result;
  }

  void write_event(const operation &op) {
    auto bytes{next_event_bytes(op.event_size, op.timestamp)};
    const util::ctime_timestamp timestamp{
        static_cast<std::time_t>(op.timestamp)};
    const auto gno{model_.get_current_gno()};
    // as in the collector, the sequence number comes with the first event
    // of a transaction (the GTID event) and the GTID applies to all of them
    const auto sequence_number{
        model_.is_first_event_of_transaction() ? gno : 0ULL};
    const auto transaction_gtid{
        settings_.gtid_mode
            ? binsrv::gtids::gtid{binsrv::gtids::uuid{transaction_uuid}, gno}
            : binsrv::gtids::gtid{}};
    storage_->write_event(util::const_byte_span{bytes},
                          op.at_transaction_boundary, transaction_gtid,
                          timestamp, sequence_number);
    model_.write_event(std::move(bytes), timestamp,
                       op.at_transaction_boundary);
  }

  void reopen_current_binlog(binsrv::storage &target, std::string_view label) {
    const auto status{target.open_binlog(model_.get_current_binlog().name)};
    require(status != binsrv::open_binlog_status::created,
            std::string{label} + ": the existing binlog was created anew");
  }

  // a copy of the storage directory taken now is what a killed process
  // leaves behind: it must be accepted by a new storage, and resuming must
  // start right after what is on disk
  void crash() {
    const auto crash_directory{root_.path() / "crash"};
    std::filesystem::remove_all(crash_directory);
    std::filesystem::copy(storage_directory_, crash_directory,
                          std::filesystem::copy_options::recursive);
    const auto crash_config{
        write_config(root_.path(), crash_directory, settings_)};
    {
      auto crashed{make_storage(crash_config)};
      const auto state{require_disk_matches_model(
          crash_directory, crashed->get_binlog_records(), model_,
          settings_.gtid_mode)};
      reopen_current_binlog(*crashed, "crash");
      require(crashed->get_current_position() == state.file_size,
              "crash: resuming at position " +
                  std::to_string(crashed->get_current_position()) +
                  " instead of right after the data on disk (" +
                  std::to_string(state.file_size) + ")");
    }
    std::filesystem::remove_all(crash_directory);
  }

  void check() {
    const auto state{require_disk_matches_model(
        storage_directory_, storage_->get_binlog_records(), model_,
        settings_.gtid_mode)};
    const auto &current{model_.get_current_binlog()};
    const auto unflushed{
        unflushed_complete_size(current, state.transactions_on_disk)};

    if (properties_ == checked_properties::model_agreement) {
      const auto expected_position{state.file_size + unflushed +
                                   model_.get_incomplete_size()};
      require(storage_->get_current_position() == expected_position,
              "current position " +
                  std::to_string(storage_->get_current_position()) +
                  " instead of " + std::to_string(expected_position));
      return;
    }

    if (properties_ == checked_properties::read_back) {
      const auto expected{expected_events(model_, state)};
      read_until_end(*tailing_reader_, tailing_received_, std::size(expected),
                     "tailing reader");
      require_same_events(tailing_received_, expected, "tailing reader");

      operations::sender_context fresh_reader{
          std::make_shared<binsrv::null_logger>(), storage_,
          settings_.fresh_reader_block_size};
      event_list fresh_received;
      read_until_end(fresh_reader, fresh_received, std::size(expected),
                     "fresh reader");
      require_same_events(fresh_received, expected, "fresh reader");
      return;
    }

    if (settings_.checkpoint_size.has_value()) {
      require(unflushed < *settings_.checkpoint_size,
              std::to_string(unflushed) +
                  " bytes of complete transactions are not on disk with "
                  "checkpoint_size " +
                  std::to_string(*settings_.checkpoint_size));
    }
  }
};

void run_storage_property(hegel::TestCase &tc, checked_properties properties) {
  const auto settings{tc.draw("settings", storage_settings_generator())};
  const auto ops{tc.draw("ops", gs::vectors(operations()))};
  storage_harness harness{settings, properties};
  for (const auto &op : ops) {
    harness.apply(op);
  }
}

} // anonymous namespace

BOOST_AUTO_TEST_CASE(StorageMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    run_storage_property(tc, checked_properties::model_agreement);
  });
}

BOOST_AUTO_TEST_CASE(StorageRespectsCheckpointSize) {
  run_property([](hegel::TestCase &tc) {
    run_storage_property(tc, checked_properties::checkpoint_size_bound);
  });
}

BOOST_AUTO_TEST_CASE(StorageReadBackMatchesDisk) {
  run_property([](hegel::TestCase &tc) {
    run_storage_property(tc, checked_properties::read_back);
  });
}
