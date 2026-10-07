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
//   that follows the storage as it grows and for a fresh reader;
// - a reader running on another thread while the storage is being written
//   (as in 'pull' mode, where the collector writes while replicas read)
//   ends up with exactly the events of the complete transactions on disk;
// - purging the oldest binlogs from another storage instance in purging mode
//   (as the 'purge_binlogs' operation does while 'fetch' / 'pull' keeps
//   running) leaves a storage that can be opened again at any moment and
//   holds exactly the remaining binlogs;
// - opening a storage in which one file was damaged afterwards (deleted,
//   truncated, extended, a byte changed, a JSON value replaced, an unexpected
//   file added, binlog index entries dropped, duplicated, swapped or added,
//   or the metadata of every binlog damaged) never crashes and fails only
//   with a regular exception; damage that breaks the storage structure is
//   rejected when streaming, damage the storage is designed to tolerate (a
//   changed byte in binlog data, extra data at the end of the last binlog) is
//   accepted with unchanged binlog records, and opening for queries skips
//   exactly the binlogs whose metadata cannot be read.
//
// The generated events are not real binlog events, but each one starts with
// a valid 19-byte common header (type code, event size), which is all the
// reading side relies on.
//
// Each test case may also run with an encrypted storage (a generated key
// encryption key from a test keyring and an AES-CTR data cipher). Then the
// files on disk are checked by size only and must differ from the plaintext,
// while the read-back property checks the decrypted content.
//
// Every property also has a variant ('...OnS3') that runs the same test
// cases on the S3 storage backend against an S3-compatible server, given by
// the environment variables PBS_TEST_S3_ENDPOINT (host:port),
// PBS_TEST_S3_ACCESS_KEY, PBS_TEST_S3_SECRET_KEY and PBS_TEST_S3_BUCKET; the
// variants are skipped when these are not set. The storage objects are then
// read and changed through the backend instead of the filesystem.

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <new>
#include <memory>
#include <numeric>
#include <source_location>
#include <optional>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
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

#include "binsrv/basic_storage_backend.hpp"
#include "binsrv/main_config.hpp"
#include "binsrv/null_logger.hpp"
#include "binsrv/storage.hpp"
#include "binsrv/storage_backend_factory.hpp"
#include "binsrv/storage_backend_type.hpp"
#include "binsrv/storage_config.hpp"
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

#include "util/byte_span.hpp"
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

struct encryption_settings {
  std::string kek_id;
  std::string data_cipher;
};

// the S3-compatible server used by the '...OnS3' properties
struct s3_server {
  std::string endpoint;
  std::string access_key;
  std::string secret_key;
  std::string bucket;
};

[[nodiscard]] std::optional<s3_server> s3_server_from_environment() {
  const auto get{[](const char *name) {
    const char *value{std::getenv(name)}; // NOLINT(concurrency-mt-unsafe)
    return value == nullptr ? std::string{} : std::string{value};
  }};
  s3_server result{.endpoint = get("PBS_TEST_S3_ENDPOINT"),
                   .access_key = get("PBS_TEST_S3_ACCESS_KEY"),
                   .secret_key = get("PBS_TEST_S3_SECRET_KEY"),
                   .bucket = get("PBS_TEST_S3_BUCKET")};
  if (result.endpoint.empty() || result.access_key.empty() ||
      result.secret_key.empty() || result.bucket.empty()) {
    return std::nullopt;
  }
  return result;
}

struct storage_settings {
  // set by the '...OnS3' properties, not generated
  std::optional<s3_server> s3;
  bool gtid_mode;
  std::optional<std::uint64_t> checkpoint_size;
  std::optional<encryption_settings> encryption;
  // block sizes used by the sender_context readers
  std::size_t tailing_reader_block_size;
  std::size_t fresh_reader_block_size;
};

std::ostream &operator<<(std::ostream &output,
                         const storage_settings &settings) {
  output << (settings.s3.has_value() ? "S3, " : "filesystem, ")
         << (settings.gtid_mode ? "gtid" : "position") << " mode, ";
  if (settings.checkpoint_size.has_value()) {
    output << "checkpoint_size " << *settings.checkpoint_size;
  } else {
    output << "no checkpoint_size";
  }
  if (settings.encryption.has_value()) {
    output << ", encrypted with " << settings.encryption->data_cipher
           << " (KEK '" << settings.encryption->kek_id << "')";
  }
  return output << ", reader block sizes " << settings.tailing_reader_block_size
                << " / " << settings.fresh_reader_block_size;
}

// the keys of the test keyring (the same as in the MTR suite's
// generate_keyring_data_file.inc): every supported key encryption key mode
constexpr std::string_view keyring_name{"keyring_data.json"};
constexpr std::string_view keyring_content{R"({"version": 1, "keys": [
  {"id": "alpha16", "cipher": "AES-128-ECB", "data_hex": "000102030405060708090A0B0C0D0E0F"},
  {"id": "alpha32", "cipher": "AES-256-ECB", "data_hex": "000102030405060708090A0B0C0D0E0F000102030405060708090A0B0C0D0E0F"},
  {"id": "beta24", "cipher": "AES-192-CBC", "data_hex": "101112131415161718191A1B1C1D1E1F1011121314151617"},
  {"id": "gamma16", "cipher": "AES-128-CTR", "data_hex": "202122232425262728292A2B2C2D2E2F"},
  {"id": "gamma32", "cipher": "AES-256-CTR", "data_hex": "202122232425262728292A2B2C2D2E2F202122232425262728292A2B2C2D2E2F"},
  {"id": "delta24", "cipher": "AES-192-GCM", "data_hex": "303132333435363738393A3B3C3D3E3F3031323334353637"}
]})"};
const std::vector<std::string> keyring_key_ids{"alpha16", "alpha32", "beta24",
                                               "gamma16", "gamma32", "delta24"};
// with a block-mode (ECB / CBC) key encryption key, the file key length must
// be a multiple of its 16-byte block, which rules out AES-192 data keys (the
// storage rejects such a configuration at startup)
const std::vector<std::string> block_mode_key_ids{"alpha16", "alpha32",
                                                  "beta24"};

// the storage section members that select where a storage lives, written
// by 'storage_location::config_members()'
using location_members = std::string;

// writes a configuration file for a storage at a location (given by its
// configuration members); the connection / replication source sections are
// required by the configuration schema but are not used by the storage
// (an encrypted storage uses the keyring file from 'directory')
[[nodiscard]] std::filesystem::path
write_config(const std::filesystem::path &directory,
             const location_members &location,
             const storage_settings &settings) {
  static std::atomic<std::uint64_t> config_counter{0ULL};
  std::ostringstream storage_section;
  storage_section << location;
  if (settings.checkpoint_size.has_value()) {
    storage_section << R"(, "checkpoint_size": ")" << *settings.checkpoint_size
                    << '"';
  }
  std::string keyring_section;
  if (settings.encryption.has_value()) {
    storage_section << R"(, "encryption": { "format": "generic", "kek_id": ")"
                    << settings.encryption->kek_id << R"(", "cipher": ")"
                    << settings.encryption->data_cipher << R"(" })";
    keyring_section = R"(
  "keyring": { "uri": "file://)" +
                      (directory / keyring_name).generic_string() + R"(" },)";
  }

  const auto config_path{
      directory / ("config_" + std::to_string(config_counter++) + ".json")};
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
  },)" << keyring_section
         << R"(
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
make_storage(const std::filesystem::path &config_path,
             binsrv::storage_construction_mode_type construction_mode =
                 binsrv::storage_construction_mode_type::streaming) {
  const binsrv::main_config config{config_path.string()};
  return std::make_shared<binsrv::storage>(
      std::make_shared<binsrv::null_logger>(), config,
      construction_mode);
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
// where a storage lives
// ---------------------------------------------------------------------------

using object_bytes = std::vector<std::byte>;

// the objects of a storage: files in a directory, or objects under a prefix
// in an S3 bucket
class storage_location {
public:
  storage_location() = default;
  storage_location(const storage_location &) = delete;
  storage_location &operator=(const storage_location &) = delete;
  storage_location(storage_location &&) = delete;
  storage_location &operator=(storage_location &&) = delete;
  virtual ~storage_location() = default;

  // the members of the configuration's "storage" section that select it
  [[nodiscard]] virtual std::string config_members() const = 0;
  // sorted
  [[nodiscard]] virtual std::vector<std::string> object_names() = 0;
  [[nodiscard]] virtual object_bytes read(std::string_view name) = 0;
  virtual void write(std::string_view name, const object_bytes &content) = 0;
  virtual void remove(std::string_view name) = 0;
  // an empty location next to this one, for copies of the storage
  [[nodiscard]] virtual std::unique_ptr<storage_location>
  make_sibling(std::string_view label) = 0;

  [[nodiscard]] bool exists(std::string_view name) {
    const auto names{object_names()};
    return std::ranges::find(names, name) != std::cend(names);
  }
  [[nodiscard]] std::uint64_t size(std::string_view name) {
    return std::size(read(name));
  }
  void clear() {
    for (const auto &name : object_names()) {
      remove(name);
    }
  }
  void copy_to(storage_location &target) {
    target.clear();
    for (const auto &name : object_names()) {
      target.write(name, read(name));
    }
  }
};

class directory_location final : public storage_location {
public:
  explicit directory_location(std::filesystem::path directory)
      : directory_{std::move(directory)} {
    std::filesystem::create_directories(directory_);
  }

  [[nodiscard]] std::string config_members() const override {
    return R"("backend": "file", "uri": "file://)" +
           directory_.generic_string() + '"';
  }
  [[nodiscard]] std::vector<std::string> object_names() override {
    std::vector<std::string> result;
    for (const auto &entry :
         std::filesystem::directory_iterator{directory_}) {
      result.push_back(entry.path().filename().string());
    }
    std::ranges::sort(result);
    return result;
  }
  [[nodiscard]] object_bytes read(std::string_view name) override {
    return read_file(directory_ / name);
  }
  void write(std::string_view name, const object_bytes &content) override {
    std::ofstream output{directory_ / name, std::ios::binary | std::ios::trunc};
    output.write(
        reinterpret_cast<const char *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            std::data(content)),
        static_cast<std::streamsize>(std::size(content)));
    if (!output) {
      throw std::runtime_error{"cannot write '" + std::string{name} + "'"};
    }
  }
  void remove(std::string_view name) override {
    std::filesystem::remove(directory_ / name);
  }
  [[nodiscard]] std::unique_ptr<storage_location>
  make_sibling(std::string_view label) override {
    auto sibling_directory{directory_.parent_path() /
                           (directory_.filename().string() + '-' +
                            std::string{label})};
    std::filesystem::remove_all(sibling_directory);
    return std::make_unique<directory_location>(std::move(sibling_directory));
  }

private:
  std::filesystem::path directory_;
};

// percent-encodes everything except the characters a URI never needs to
// encode, so that any access key or secret fits into the user info
[[nodiscard]] std::string percent_encode(std::string_view text) {
  static constexpr std::string_view digits{"0123456789ABCDEF"};
  std::string result;
  for (const char character : text) {
    const auto byte{static_cast<unsigned char>(character)};
    if ((byte >= 'A' && byte <= 'Z') || (byte >= 'a' && byte <= 'z') ||
        (byte >= '0' && byte <= '9') || byte == '-' || byte == '.' ||
        byte == '_' || byte == '~') {
      result += character;
    } else {
      result += '%';
      result += digits[byte >> 4U];
      result += digits[byte & 0x0FU];
    }
  }
  return result;
}

// objects under a prefix of an S3 bucket, read and written through the S3
// storage backend; the objects are removed when the location is destroyed
class s3_location final : public storage_location {
public:
  s3_location(s3_server server, std::string prefix,
              std::filesystem::path buffer_directory)
      : server_{std::move(server)}, prefix_{std::move(prefix)},
        buffer_directory_{std::move(buffer_directory)} {
    std::filesystem::create_directories(buffer_directory_);
    binsrv::storage_config config{};
    config.get<"backend">() = binsrv::storage_backend_type::s3;
    config.get<"uri">() = uri();
    config.get<"fs_buffer_directory">() = buffer_directory_.string();
    backend_ = binsrv::storage_backend_factory::create(config);
  }
  s3_location(const s3_location &) = delete;
  s3_location &operator=(const s3_location &) = delete;
  s3_location(s3_location &&) = delete;
  s3_location &operator=(s3_location &&) = delete;
  ~s3_location() override {
    try {
      clear();
    } catch (...) { // NOLINT(bugprone-empty-catch)
    }
  }

  [[nodiscard]] std::string config_members() const override {
    return R"("backend": "s3", "uri": ")" + uri() +
           R"(", "fs_buffer_directory": ")" + buffer_directory_.string() +
           '"';
  }
  [[nodiscard]] std::vector<std::string> object_names() override {
    std::vector<std::string> result;
    for (const auto &[name, size] : backend_->list_objects()) {
      result.push_back(name);
    }
    std::ranges::sort(result);
    return result;
  }
  [[nodiscard]] object_bytes read(std::string_view name) override {
    return backend_->get_object(name);
  }
  void write(std::string_view name, const object_bytes &content) override {
    backend_->put_object(name, util::const_byte_span{content});
  }
  void remove(std::string_view name) override {
    backend_->remove_object(name);
  }
  [[nodiscard]] std::unique_ptr<storage_location>
  make_sibling(std::string_view label) override {
    auto sibling{std::make_unique<s3_location>(
        server_, prefix_ + '-' + std::string{label},
        buffer_directory_.parent_path() /
            (buffer_directory_.filename().string() + '-' +
             std::string{label}))};
    sibling->clear();
    return sibling;
  }

private:
  s3_server server_;
  std::string prefix_;
  std::filesystem::path buffer_directory_;
  binsrv::basic_storage_backend_ptr backend_{};

  [[nodiscard]] std::string uri() const {
    return "http://" + percent_encode(server_.access_key) + ':' +
           percent_encode(server_.secret_key) + '@' + server_.endpoint + '/' +
           server_.bucket + '/' + prefix_;
  }
};

// ---------------------------------------------------------------------------
// operations
// ---------------------------------------------------------------------------

enum class operation_kind : std::uint8_t {
  write_event,
  discard_incomplete_transaction,
  flush,
  rotate,
  restart,
  crash,
  purge
};

struct operation {
  operation_kind kind;
  // only for write_event
  std::size_t event_size;
  bool at_transaction_boundary;
  std::uint32_t timestamp;
  // only for purge: how many of the oldest binlogs to purge (at most all but
  // the current one)
  std::size_t purge_count;
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
  case operation_kind::purge:
    return output << "purge(" << op.purge_count << ')';
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
         operation_kind::rotate, operation_kind::rotate,
         operation_kind::restart, operation_kind::crash,
         operation_kind::purge}))};
    static constexpr std::size_t max_purge_count{3U};
    operation result{.kind = kind,
                     .event_size = 0U,
                     .at_transaction_boundary = false,
                     .timestamp = 0U,
                     .purge_count = 0U};
    if (kind == operation_kind::purge) {
      result.purge_count = tc.draw(gs::integers<std::size_t>(
          {.min_value = 1U, .max_value = max_purge_count}));
    }
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
    storage_settings result{.s3 = std::nullopt,
                            .gtid_mode = tc.draw(gs::booleans()),
                            .checkpoint_size = std::nullopt,
                            .encryption = std::nullopt,
                            .tailing_reader_block_size = 0U,
                            .fresh_reader_block_size = 0U};
    if (tc.draw(gs::booleans())) {
      result.checkpoint_size = tc.draw(gs::integers<std::uint64_t>(
          {.min_value = min_checkpoint_size,
           .max_value = max_checkpoint_size}));
    }
    if (tc.draw(gs::booleans())) {
      const auto kek_id{tc.draw(gs::sampled_from(keyring_key_ids))};
      const bool block_mode_kek{std::ranges::find(block_mode_key_ids, kek_id) !=
                                std::cend(block_mode_key_ids)};
      const auto data_cipher{tc.draw(gs::sampled_from(
          block_mode_kek
              ? std::vector<std::string>{"AES-128-CTR", "AES-256-CTR"}
              : std::vector<std::string>{"AES-128-CTR", "AES-192-CTR",
                                         "AES-256-CTR"}))};
      result.encryption =
          encryption_settings{.kek_id = kek_id, .data_cipher = data_cipher};
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

  // removes the 'count' oldest binlogs (which are closed, so all their
  // transactions are on disk); their GTIDs stay part of every later
  // binlog's previous GTIDs
  void purge(std::size_t count);

  // GTIDs of the transactions in purged binlogs
  [[nodiscard]] const binsrv::gtids::gtid_set &
  get_purged_gtids() const noexcept {
    return purged_gtids_;
  }

private:
  binsrv::gtids::gtid_set purged_gtids_{};
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
                      const model_binlog &binlog, bool encrypted) {
  const auto magic_size{std::size(binsrv::events::magic_binlog_payload)};
  if (encrypted) {
    // the content cannot be compared, but the file must still end at a
    // transaction boundary, and it must not hold the plaintext
    std::vector<std::byte> plaintext(
        std::cbegin(binsrv::events::magic_binlog_payload),
        std::cend(binsrv::events::magic_binlog_payload));
    std::size_t transactions_on_disk{0U};
    while (std::size(plaintext) < std::size(file_content)) {
      require(transactions_on_disk < std::size(binlog.transactions),
              describe_binlog(binlog) +
                  " holds more data than its complete transactions (" +
                  std::to_string(std::size(file_content)) + " bytes)");
      for (const auto &event :
           binlog.transactions[transactions_on_disk].events) {
        plaintext.insert(std::end(plaintext), std::cbegin(event.bytes),
                         std::cend(event.bytes));
      }
      ++transactions_on_disk;
    }
    require(std::size(plaintext) == std::size(file_content),
            describe_binlog(binlog) + " ends in the middle of transaction " +
                std::to_string(transactions_on_disk) + " (" +
                std::to_string(std::size(file_content)) + " bytes)");
    require(plaintext != file_content,
            describe_binlog(binlog) + " is stored unencrypted");
    return transactions_on_disk;
  }
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

void storage_model::purge(std::size_t count) {
  assert(count < std::size(binlogs_));
  for (std::size_t index{0U}; index < count; ++index) {
    purged_gtids_ +=
        model_gtids(binlogs_[index], std::size(binlogs_[index].transactions));
  }
  binlogs_.erase(std::begin(binlogs_),
                 std::next(std::begin(binlogs_),
                           static_cast<std::ptrdiff_t>(count)));
}

void require_record_matches(const binsrv::binlog_record &record,
                            const model_binlog &binlog,
                            std::size_t transactions_on_disk,
                            std::uint64_t file_size,
                            const binsrv::gtids::gtid_set &earlier_gtids,
                            bool gtid_mode, bool encrypted) {
  const auto label{describe_binlog(binlog) + " record: "};
  require(record.encryption.has_value() == encrypted,
          label + (encrypted ? "has no encryption metadata"
                             : "has encryption metadata in an unencrypted "
                               "storage"));
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
require_disk_matches_model(storage_location &location,
                           const binsrv::binlog_record_container &records,
                           const storage_model &model,
                           const storage_settings &settings) {
  const bool encrypted{settings.encryption.has_value()};
  const auto &binlogs{model.get_binlogs()};
  require(std::size(records) == std::size(binlogs),
          std::to_string(std::size(records)) + " binlog records instead of " +
              std::to_string(std::size(binlogs)));

  binsrv::gtids::gtid_set earlier_gtids{model.get_purged_gtids()};
  disk_state result{.file_size = 0ULL,
                    .transactions_on_disk = 0U,
                    .transactions_on_disk_per_binlog = {}};
  for (std::size_t index{0U}; index < std::size(binlogs); ++index) {
    const auto &binlog{binlogs[index]};
    const auto file_content{location.read(binlog.name.str())};
    const auto transactions_on_disk{
        match_file_with_model(file_content, binlog, encrypted)};
    if (binlog.closed) {
      require(transactions_on_disk == std::size(binlog.transactions),
              describe_binlog(binlog) + " was closed with " +
                  std::to_string(std::size(binlog.transactions) -
                                 transactions_on_disk) +
                  " complete transaction(s) missing");
    }
    require_record_matches(records[index], binlog, transactions_on_disk,
                           std::size(file_content), earlier_gtids,
                           settings.gtid_mode, encrypted);
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
// damaged storage files
// ---------------------------------------------------------------------------

enum class damage_kind : std::uint8_t {
  delete_file,
  truncate_file,
  change_byte,
  append_garbage,
  replace_text,
  add_unexpected_file,
  index_drop_entry,
  index_duplicate_entry,
  index_swap_entries,
  index_add_entry,
  index_crlf_line_endings,
  damage_every_binlog_metadata
};

constexpr std::array damage_kind_names{
    std::string_view{"delete_file"},
    std::string_view{"truncate_file"},
    std::string_view{"change_byte"},
    std::string_view{"append_garbage"},
    std::string_view{"replace_text"},
    std::string_view{"add_unexpected_file"},
    std::string_view{"index_drop_entry"},
    std::string_view{"index_duplicate_entry"},
    std::string_view{"index_swap_entries"},
    std::string_view{"index_add_entry"},
    std::string_view{"index_crlf_line_endings"},
    std::string_view{"damage_every_binlog_metadata"}};

// the damage to apply; file, positions and replacement are chosen from these
// values modulo what the storage holds
struct damage {
  damage_kind kind;
  std::size_t target;
  std::size_t position;
  std::size_t other_position;
  std::uint8_t mask;
  std::string text;
};

std::ostream &operator<<(std::ostream &output, const damage &value) {
  return output << damage_kind_names.at(static_cast<std::size_t>(value.kind))
                << "(target " << value.target << ", position "
                << value.position << ", other position "
                << value.other_position << ", mask "
                << static_cast<unsigned>(value.mask) << ", text '"
                << value.text << "')";
}

[[nodiscard]] gs::Generator<damage> damages() {
  return gs::compose([](const hegel::TestCase &tc) {
    const auto position{[&tc] {
      return tc.draw(gs::integers<std::size_t>(
          {.min_value = 0U, .max_value = 1U << 20U}));
    }};
    damage result{
        .kind = static_cast<damage_kind>(tc.draw(gs::integers<std::size_t>(
            {.min_value = 0U,
             .max_value = std::size(damage_kind_names) - 1U}))),
        .target = position(),
        .position = position(),
        .other_position = position(),
        .mask = tc.draw(gs::integers<std::uint8_t>({.min_value = 1U, .max_value = 255U})),
        .text = {}};
    // JSON values of other types and out-of-range numbers, unexpected file
    // names, and binlog index entries
    result.text = tc.draw(gs::sampled_from<std::string>(
        {"", "null", "-1", "0", "1.5", "1e999", "18446744073709551616",
         "\"\"", "\"x\"", "[]", "{}", "true", "\"11111111-aaaa-1111-aaaa-"
         "111111111111:0\"", "\"2026-10-06T99:99:99\"", "binlog.000099",
         "binlog.000099.json", "notes.txt", "./binlog.000001",
         "./binlog.000099", "../binlog.000001", "binlog.index",
         "./binlog.index", "./", "./binlog.0000001", "./binlog.00000a"}));
    return result;
  });
}

[[nodiscard]] std::string read_text(storage_location &location,
                                    std::string_view name) {
  const auto bytes{location.read(name)};
  std::string result(std::size(bytes), '\0');
  std::ranges::transform(bytes, std::begin(result), [](std::byte value) {
    return static_cast<char>(std::to_integer<unsigned char>(value));
  });
  return result;
}

void write_text(storage_location &location, std::string_view name,
                std::string_view content) {
  object_bytes bytes(std::size(content));
  std::ranges::transform(content, std::begin(bytes), [](char value) {
    return static_cast<std::byte>(static_cast<unsigned char>(value));
  });
  location.write(name, bytes);
}

[[nodiscard]] std::vector<std::string> split_lines(std::string_view text) {
  std::vector<std::string> result;
  std::size_t start{0U};
  while (start < std::size(text)) {
    const auto end{text.find('\n', start)};
    const auto line{text.substr(start, end - start)};
    if (!line.empty()) {
      result.emplace_back(line);
    }
    if (end == std::string_view::npos) {
      break;
    }
    start = end + 1U;
  }
  return result;
}

[[nodiscard]] std::string join_lines(const std::vector<std::string> &lines,
                                     std::string_view ending = "\n") {
  std::string result;
  for (const auto &line : lines) {
    result += line;
    result += ending;
  }
  return result;
}

[[nodiscard]] bool same_records(const binsrv::binlog_record &first,
                                const binsrv::binlog_record &second) {
  return first.name == second.name && first.size == second.size &&
         first.previous_gtids == second.previous_gtids &&
         first.added_gtids == second.added_gtids &&
         first.timestamps.is_empty() == second.timestamps.is_empty() &&
         (first.timestamps.is_empty() ||
          (first.timestamps.get_min_timestamp() ==
               second.timestamps.get_min_timestamp() &&
           first.timestamps.get_max_timestamp() ==
               second.timestamps.get_max_timestamp())) &&
         first.last_sequence_number == second.last_sequence_number &&
         first.encryption.has_value() == second.encryption.has_value();
}

// what opening a storage led to
struct open_outcome {
  std::optional<binsrv::binlog_record_container> records;
  std::string error;
};

// opens a storage, turning a regular exception into a rejection; anything
// else (a crash, std::terminate, a non-standard exception or an allocation
// failure reported for corrupted data) fails the property
[[nodiscard]] open_outcome
open_storage(const std::filesystem::path &config_path,
             binsrv::storage_construction_mode_type construction_mode) {
  open_outcome result{};
  try {
    const auto storage{make_storage(config_path, construction_mode)};
    result.records = storage->get_binlog_records();
  } catch (const std::bad_alloc &e) {
    throw std::runtime_error{std::string{"rejected with std::bad_alloc: "} +
                             e.what()};
  } catch (const std::exception &e) {
    const std::string message{e.what()};
    if (message.find("bad_alloc") != std::string::npos) {
      throw std::runtime_error{"rejected with an allocation failure: " +
                               message};
    }
    result.error = message;
  } catch (...) {
    throw std::runtime_error{"rejected with a non-standard exception"};
  }
  return result;
}

// ---------------------------------------------------------------------------
// running a sequence of operations
// ---------------------------------------------------------------------------

enum class checked_properties : std::uint8_t {
  model_agreement,
  checkpoint_size_bound,
  read_back,
  concurrent_read_back,
  purge_reopenable,
  damaged_files
};

[[nodiscard]] bool reads_back(checked_properties properties) noexcept {
  return properties == checked_properties::read_back ||
         properties == checked_properties::concurrent_read_back;
}

// what a reader running on its own thread received; written only by that
// thread and read only after it has been joined
struct concurrent_read_result {
  event_list received;
  std::string error;
};

// reads through sender_context until a stop is requested, polling at the
// end of the data as a blocking COM_BINLOG_DUMP session does; after the stop
// request it makes one more pass up to the end of the data, so that
// everything written before the request is read
void read_concurrently(const std::stop_token &stop,
                       operations::sender_context &reader,
                       concurrent_read_result &result) {
  static constexpr std::size_t max_events{1'000'000U};
  static constexpr auto max_duration{std::chrono::seconds{60}};
  const auto deadline{std::chrono::steady_clock::now() + max_duration};
  bool final_pass{false};
  util::const_byte_span event{};
  try {
    while (true) {
      if (!reader.get_event(event)) {
        result.error = "sender_context::get_event() failed after " +
                       std::to_string(std::size(result.received)) +
                       " event(s)";
        return;
      }
      if (!event.empty()) {
        result.received.emplace_back(std::cbegin(event), std::cend(event));
        if (std::size(result.received) > max_events) {
          result.error = "the reader returned too many events";
          return;
        }
        continue;
      }
      // end of the data
      if (final_pass) {
        return;
      }
      if (stop.stop_requested()) {
        final_pass = true;
        continue;
      }
      if (std::chrono::steady_clock::now() > deadline) {
        result.error = "the reader did not finish in time";
        return;
      }
      std::this_thread::yield();
    }
  } catch (const std::exception &e) {
    result.error = std::string{"exception in the reader: "} + e.what();
  }
}

class storage_harness {
public:
  storage_harness(const storage_settings &settings,
                  checked_properties properties)
      : settings_{settings}, properties_{properties},
        location_{make_location()}, config_path_{prepare_config()} {
    storage_ = make_storage(config_path_);
    const auto status{storage_->open_binlog(model_.get_current_binlog().name)};
    require(status == binsrv::open_binlog_status::created,
            "the first binlog was not reported as created");
    restart_tailing_reader();
    if (properties_ == checked_properties::concurrent_read_back) {
      concurrent_reader_ = std::make_unique<operations::sender_context>(
          std::make_shared<binsrv::null_logger>(), storage_,
          settings_.tailing_reader_block_size);
      concurrent_thread_ = std::jthread{[this](const std::stop_token &stop) {
        read_concurrently(stop, *concurrent_reader_, concurrent_result_);
      }};
    }
    check();
  }

  // called after the last operation: for the concurrent property, stops the
  // reader and compares what it received with what is on disk
  void finish() {
    if (properties_ != checked_properties::concurrent_read_back) {
      return;
    }
    concurrent_thread_.request_stop();
    concurrent_thread_.join();
    require(concurrent_result_.error.empty(),
            "concurrent reader: " + concurrent_result_.error);
    const auto state{require_disk_matches_model(
        *location_, storage_->get_binlog_records(), model_,
        settings_)};
    require_same_events(concurrent_result_.received,
                        expected_events(model_, state), "concurrent reader");
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
      // a binlog streamed from a server always ends with a ROTATE / STOP
      // event, so closed binlogs are never empty; the reader relies on that
      // (an empty binlog in the middle reads as the end of the data), so the
      // read-back property only rotates binlogs that hold data
      if (reads_back(properties_) &&
          model_.get_current_binlog().transactions.empty()) {
        break;
      }
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
      // a running server never replaces its storage object while replicas
      // are reading, so restarts are not part of the concurrent property
      if (properties_ == checked_properties::concurrent_read_back) {
        break;
      }
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
    case operation_kind::purge:
      purge(op.purge_count);
      break;
    }
    check();
  }

  // damages one file (or every binlog metadata file) of a copy of the
  // storage as it is on disk now, and opens the copy for streaming and for
  // queries
  void check_damaged_copy(const damage &value) {
    const auto copy{location_->make_sibling("damaged")};
    location_->copy_to(*copy);
    const auto copy_config{
        write_config(root_.path(), copy->config_members(), settings_)};

    // the binlog records as the undamaged files describe them
    const auto reference_outcome{open_storage(
        copy_config, binsrv::storage_construction_mode_type::querying_only)};
    require(reference_outcome.records.has_value(),
            "the undamaged storage cannot be opened for queries: " +
                reference_outcome.error);
    const auto &reference{*reference_outcome.records};
    require(!reference.empty(), "the undamaged storage has no binlogs");

    const auto files{copy->object_names()};
    const std::string index_name{
        binsrv::storage_core::default_binlog_index_name};
    const std::string metadata_name{binsrv::storage_core::metadata_name};
    const auto is_binlog_metadata{[&metadata_name](const std::string &name) {
      return name != metadata_name && name.ends_with(".json");
    }};
    const auto last_binlog{reference.back().name.str()};

    // what the damage must lead to when opening for streaming
    enum class expectation : std::uint8_t { rejected, accepted, either };
    auto expected{expectation::either};
    // the binlogs whose metadata files were damaged
    std::vector<std::string> damaged_metadata;
    std::ostringstream description_stream;
    description_stream << value << ':';
    std::string description{description_stream.str()};

    const auto target_name{files[value.target % std::size(files)]};
    const bool target_is_data{target_name != index_name &&
                              target_name != metadata_name &&
                              !is_binlog_metadata(target_name)};
    const auto binlog_of_metadata{[](const std::string &name) {
      return name.substr(0U, std::size(name) - std::string_view{".json"}.size());
    }};

    switch (value.kind) {
    case damage_kind::delete_file:
      copy->remove(target_name);
      description += " deleting '" + target_name + "'";
      expected = expectation::rejected;
      if (is_binlog_metadata(target_name)) {
        damaged_metadata.push_back(binlog_of_metadata(target_name));
      }
      break;
    case damage_kind::truncate_file: {
      auto truncated{copy->read(target_name)};
      const auto size{std::size(truncated)};
      if (size == 0U) {
        return;
      }
      const auto new_size{value.position % size};
      truncated.resize(new_size);
      copy->write(target_name, truncated);
      description += " truncating '" + target_name + "' to " +
                     std::to_string(new_size) + " bytes";
      if (target_is_data) {
        // a binlog shorter than its metadata says cannot be recovered
        const auto record_it{std::ranges::find(
            reference, target_name,
            [](const binsrv::binlog_record &record) {
              return record.name.str();
            })};
        if (record_it != std::cend(reference) && new_size < record_it->size) {
          expected = expectation::rejected;
        }
      }
      if (is_binlog_metadata(target_name)) {
        damaged_metadata.push_back(binlog_of_metadata(target_name));
      }
      break;
    }
    case damage_kind::change_byte: {
      auto content{read_text(*copy, target_name)};
      if (content.empty()) {
        return;
      }
      auto &byte{content[value.position % std::size(content)]};
      byte = static_cast<char>(static_cast<unsigned char>(byte) ^ value.mask);
      write_text(*copy, target_name, content);
      description += " changing a byte of '" + target_name + "'";
      // the storage does not check binlog data (there is a TODO for it), so
      // a changed byte there leaves the records as they are
      if (target_is_data) {
        expected = expectation::accepted;
      }
      if (is_binlog_metadata(target_name)) {
        damaged_metadata.push_back(binlog_of_metadata(target_name));
      }
      break;
    }
    case damage_kind::append_garbage: {
      auto content{read_text(*copy, target_name)};
      content += value.text.empty() ? std::string{"garbage"} : value.text;
      write_text(*copy, target_name, content);
      description += " appending to '" + target_name + "'";
      if (target_is_data) {
        // extra data at the end of the last binlog is what an interrupted
        // write leaves behind and is truncated on startup; anywhere else the
        // sizes no longer match
        expected = target_name == last_binlog ? expectation::accepted
                                              : expectation::rejected;
      }
      if (is_binlog_metadata(target_name)) {
        damaged_metadata.push_back(binlog_of_metadata(target_name));
      }
      break;
    }
    case damage_kind::replace_text: {
      if (target_is_data) {
        return;
      }
      auto content{read_text(*copy, target_name)};
      const auto start{value.position % (std::size(content) + 1U)};
      const auto length{std::min(value.other_position % 24U,
                                 std::size(content) - start)};
      content.replace(start, length, value.text);
      write_text(*copy, target_name, content);
      description += " replacing '" + target_name + "' bytes " +
                     std::to_string(start) + ".." +
                     std::to_string(start + length) + " with '" + value.text +
                     "'";
      if (is_binlog_metadata(target_name)) {
        damaged_metadata.push_back(binlog_of_metadata(target_name));
      }
      break;
    }
    case damage_kind::add_unexpected_file: {
      auto name{value.text};
      if (name.empty() || name.find('/') != std::string::npos ||
          copy->exists(name)) {
        name = "unexpected.bin";
      }
      write_text(*copy, name, "unexpected");
      description += " adding '" + name + "'";
      expected = expectation::rejected;
      break;
    }
    case damage_kind::index_drop_entry:
    case damage_kind::index_duplicate_entry:
    case damage_kind::index_swap_entries:
    case damage_kind::index_add_entry:
    case damage_kind::index_crlf_line_endings: {
      auto lines{split_lines(read_text(*copy, index_name))};
      if (lines.empty()) {
        return;
      }
      const auto first{value.position % std::size(lines)};
      const auto second{value.other_position % std::size(lines)};
      const auto first_it{
          std::next(std::begin(lines), static_cast<std::ptrdiff_t>(first))};
      std::string ending{"\n"};
      switch (value.kind) {
      case damage_kind::index_drop_entry:
        lines.erase(first_it);
        description += " dropping binlog index entry " + std::to_string(first);
        expected = expectation::rejected;
        break;
      case damage_kind::index_duplicate_entry:
        lines.insert(first_it, *first_it);
        description +=
            " duplicating binlog index entry " + std::to_string(first);
        expected = expectation::rejected;
        break;
      case damage_kind::index_swap_entries:
        if (first == second) {
          return;
        }
        std::iter_swap(first_it, std::next(std::begin(lines),
                                           static_cast<std::ptrdiff_t>(second)));
        description += " swapping binlog index entries " +
                       std::to_string(first) + " and " + std::to_string(second);
        break;
      case damage_kind::index_add_entry:
        if (value.text.empty() ||
            std::ranges::find(lines, value.text) != std::cend(lines)) {
          return;
        }
        lines.insert(first_it, value.text);
        description += " adding binlog index entry '" + value.text + "'";
        expected = expectation::rejected;
        break;
      default:
        ending = "\r\n";
        description += " using CRLF line endings in the binlog index";
        break;
      }
      write_text(*copy, index_name, join_lines(lines, ending));
      break;
    }
    case damage_kind::damage_every_binlog_metadata:
      for (const auto &name : files) {
        if (is_binlog_metadata(name)) {
          auto content{copy->read(name)};
          content.resize(std::size(content) / 2U);
          copy->write(name, content);
          damaged_metadata.push_back(binlog_of_metadata(name));
        }
      }
      description += " truncating every binlog metadata file to half";
      expected = expectation::rejected;
      break;
    }

    // opening for queries: binlogs with readable metadata are returned as
    // they were, the others are skipped
    const auto queried{open_storage(
        copy_config, binsrv::storage_construction_mode_type::querying_only)};
    if (queried.records.has_value()) {
      for (const auto &record : *queried.records) {
        const auto name{record.name.str()};
        if (std::ranges::find(damaged_metadata, name) !=
            std::cend(damaged_metadata)) {
          continue;
        }
        const auto reference_it{std::ranges::find(
            reference, record.name, &binsrv::binlog_record::name)};
        require(reference_it != std::cend(reference),
                description + ": opening for queries returned an unknown "
                              "binlog '" + name + "'");
        if (value.kind != damage_kind::damage_every_binlog_metadata &&
            target_name != metadata_name) {
          require(same_records(record, *reference_it),
                  description + ": opening for queries returned a changed "
                                "record for '" + name + "'");
        }
      }
      if (value.kind == damage_kind::damage_every_binlog_metadata) {
        require(queried.records->empty(),
                description + ": opening for queries returned " +
                    std::to_string(std::size(*queried.records)) +
                    " binlog(s) with unreadable metadata");
      }
    }

    // opening for streaming
    const auto streamed{open_storage(
        copy_config, binsrv::storage_construction_mode_type::streaming)};
    if (expected == expectation::rejected) {
      require(!streamed.records.has_value(),
              description + ": the storage was opened for streaming");
    }
    if (expected == expectation::accepted) {
      require(streamed.records.has_value(),
              description + ": the storage was rejected: " + streamed.error);
      require(std::size(*streamed.records) == std::size(reference),
              description + ": opened with " +
                  std::to_string(std::size(*streamed.records)) +
                  " binlogs instead of " + std::to_string(std::size(reference)));
      for (std::size_t index{0U}; index < std::size(reference); ++index) {
        require(same_records((*streamed.records)[index], reference[index]),
                description + ": opened with a changed record for '" +
                    reference[index].name.str() + "'");
      }
      // extra data at the end of the last binlog is cut off
      require(copy->size(last_binlog) ==
                  reference.back().size,
              description + ": the last binlog was not truncated to " +
                  std::to_string(reference.back().size) + " bytes");
    }
  }

private:
  storage_settings settings_;
  checked_properties properties_;
  scratch_directory root_{};
  std::unique_ptr<storage_location> location_;
  std::filesystem::path config_path_;
  binsrv::storage_ptr storage_{};
  storage_model model_{};
  std::uint64_t event_counter_{0ULL};
  // a reader that is kept across operations and follows the storage as it
  // grows, as a connected replica does
  std::unique_ptr<operations::sender_context> tailing_reader_{};
  event_list tailing_received_{};
  // the reader of the concurrent property; declared in this order so that
  // the thread is stopped and joined before the reader and the result it
  // writes to are destroyed
  std::unique_ptr<operations::sender_context> concurrent_reader_{};
  concurrent_read_result concurrent_result_{};
  std::jthread concurrent_thread_{};

  void restart_tailing_reader() {
    tailing_reader_.reset();
    tailing_received_.clear();
    if (properties_ == checked_properties::read_back) {
      tailing_reader_ = std::make_unique<operations::sender_context>(
          std::make_shared<binsrv::null_logger>(), storage_,
          settings_.tailing_reader_block_size);
    }
  }

  [[nodiscard]] std::unique_ptr<storage_location> make_location() {
    if (settings_.s3.has_value()) {
      // the scratch directory name is unique, and so is the prefix
      return std::make_unique<s3_location>(
          *settings_.s3, root_.path().filename().string(),
          root_.path() / "s3-buffer");
    }
    return std::make_unique<directory_location>(root_.path() / "storage");
  }

  [[nodiscard]] std::filesystem::path prepare_config() {
    if (settings_.encryption.has_value()) {
      std::ofstream keyring{root_.path() / keyring_name};
      keyring << keyring_content;
    }
    return write_config(root_.path(), location_->config_members(), settings_);
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

  // purges the oldest binlogs through a separate storage instance in purging
  // mode, as a 'purge_binlogs' process does while this one keeps running;
  // only the purge property uses it
  void purge(std::size_t requested_count) {
    const auto &binlogs{model_.get_binlogs()};
    if (properties_ != checked_properties::purge_reopenable ||
        std::size(binlogs) < 2U) {
      return;
    }
    // the current (last) binlog cannot be purged
    const auto count{std::min(requested_count, std::size(binlogs) - 1U)};
    const auto target{binlogs[count - 1U].name};
    {
      const auto purger{make_storage(
          config_path_, binsrv::storage_construction_mode_type::purging)};
      const auto [purged, warning]{purger->purge_binlogs(target)};
      require(warning.empty(), "purge reported a warning: " + warning);
      require(std::size(purged) == count,
              "purge removed " + std::to_string(std::size(purged)) +
                  " binlog(s) instead of " + std::to_string(count));
    }
    model_.purge(count);
  }

  // a copy of the storage directory taken now is what a killed process
  // leaves behind: it must be accepted by a new storage, and resuming must
  // start right after what is on disk
  void crash() {
    const auto crash_location{location_->make_sibling("crash")};
    location_->copy_to(*crash_location);
    const auto crash_config{write_config(
        root_.path(), crash_location->config_members(), settings_)};
    {
      auto crashed{make_storage(crash_config)};
      const auto state{require_disk_matches_model(
          *crash_location, crashed->get_binlog_records(), model_, settings_)};
      reopen_current_binlog(*crashed, "crash");
      require(crashed->get_current_position() == state.file_size,
              "crash: resuming at position " +
                  std::to_string(crashed->get_current_position()) +
                  " instead of right after the data on disk (" +
                  std::to_string(state.file_size) + ")");
    }
    crash_location->clear();
  }

  void check() {
    if (properties_ == checked_properties::purge_reopenable) {
      // after a purge by another process, the running storage's own list of
      // binlogs is stale by design; what must hold at every moment is that
      // the storage on disk can be opened again (as after a restart or a
      // crash) and holds exactly the remaining binlogs
      crash();
      return;
    }
    const auto state{require_disk_matches_model(
        *location_, storage_->get_binlog_records(), model_,
        settings_)};
    const auto &current{model_.get_current_binlog()};
    const auto unflushed{
        unflushed_complete_size(current, state.transactions_on_disk)};

    if (properties_ == checked_properties::model_agreement ||
        properties_ == checked_properties::damaged_files) {
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

void run_storage_property(hegel::TestCase &tc, checked_properties properties,
                          const std::optional<s3_server> &s3 = std::nullopt) {
  auto settings{tc.draw("settings", storage_settings_generator())};
  settings.s3 = s3;
  const auto ops{tc.draw("ops", gs::vectors(operations()))};
  std::optional<damage> damage_value;
  if (properties == checked_properties::damaged_files) {
    damage_value = tc.draw("damage", damages());
  }
  storage_harness harness{settings, properties};
  for (const auto &op : ops) {
    harness.apply(op);
  }
  harness.finish();
  if (damage_value.has_value()) {
    harness.check_damaged_copy(*damage_value);
  }
}

// runs a property on the S3 storage backend, or skips it when no S3 server
// is configured
void run_s3_storage_property(checked_properties properties,
                             const std::source_location location =
                                 std::source_location::current()) {
  const auto server{s3_server_from_environment()};
  if (!server.has_value()) {
    BOOST_TEST_MESSAGE("skipped: PBS_TEST_S3_ENDPOINT, PBS_TEST_S3_ACCESS_KEY, "
                       "PBS_TEST_S3_SECRET_KEY and PBS_TEST_S3_BUCKET are not "
                       "set");
    return;
  }
  // every storage check goes through HTTP requests, so a test case takes
  // seconds instead of milliseconds: run fewer of them (20, or
  // PBS_TEST_S3_TEST_CASES), and do not fail on the slow generation that
  // follows
  static constexpr std::uint64_t default_s3_test_cases{20ULL};
  std::uint64_t s3_test_cases{default_s3_test_cases};
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  if (const char *value{std::getenv("PBS_TEST_S3_TEST_CASES")};
      value != nullptr) {
    s3_test_cases = std::stoull(value);
  }
  hegel::Settings settings{};
  settings.test_cases = s3_test_cases;
  settings.suppress_health_check = {hegel::HealthCheck::TooSlow};
  run_property(
      [properties, &server](hegel::TestCase &tc) {
        run_storage_property(tc, properties, server);
      },
      settings, location);
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

BOOST_AUTO_TEST_CASE(StoragePurgeKeepsStorageReopenable) {
  run_property([](hegel::TestCase &tc) {
    run_storage_property(tc, checked_properties::purge_reopenable);
  });
}

BOOST_AUTO_TEST_CASE(StorageConcurrentReadBackMatchesDisk) {
  run_property([](hegel::TestCase &tc) {
    run_storage_property(tc, checked_properties::concurrent_read_back);
  });
}

BOOST_AUTO_TEST_CASE(StorageOpeningOfDamagedFilesFailsCleanly) {
  run_property([](hegel::TestCase &tc) {
    run_storage_property(tc, checked_properties::damaged_files);
  });
}

BOOST_AUTO_TEST_CASE(StorageMatchesModelOnS3) {
  run_s3_storage_property(checked_properties::model_agreement);
}

BOOST_AUTO_TEST_CASE(StorageRespectsCheckpointSizeOnS3) {
  run_s3_storage_property(checked_properties::checkpoint_size_bound);
}

BOOST_AUTO_TEST_CASE(StorageReadBackMatchesDiskOnS3) {
  run_s3_storage_property(checked_properties::read_back);
}

BOOST_AUTO_TEST_CASE(StoragePurgeKeepsStorageReopenableOnS3) {
  run_s3_storage_property(checked_properties::purge_reopenable);
}

BOOST_AUTO_TEST_CASE(StorageConcurrentReadBackMatchesDiskOnS3) {
  run_s3_storage_property(checked_properties::concurrent_read_back);
}

BOOST_AUTO_TEST_CASE(StorageOpeningOfDamagedFilesFailsCleanlyOnS3) {
  run_s3_storage_property(checked_properties::damaged_files);
}
