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
// - opening a storage in which one file was damaged afterwards (deleted,
//   truncated, extended, a byte changed, a JSON value replaced, an unexpected
//   file added, binlog index entries dropped, duplicated, swapped or added,
//   or the metadata of every binlog damaged) never crashes and fails only
//   with a regular exception; damage that breaks the storage structure is
//   rejected when streaming, damage the storage is designed to tolerate (a
//   changed byte in binlog data, extra data at the end of the last binlog) is
//   accepted with unchanged binlog records, and opening for queries skips
//   exactly the binlogs whose metadata cannot be read;
// - opening a storage for queries (as 'list' and 'search_by_*' do, possibly
//   while 'fetch' / 'pull' keeps running) never changes any of its objects,
//   also with a configuration that adds encryption to a storage created
//   without it;
// - closing a binlog while an incomplete transaction is still buffered (as
//   the collector does when a reconnect's artificial ROTATE names another
//   binlog) drops that transaction completely, including its timestamps;
// - opening the storage for queries on another thread while it is being
//   written, rotated, restarted and checkpointed always succeeds and lists
//   the binlogs without a gap;
// - a writer killed, or hitting a storage error, at any backend call (the
//   backend is wrapped through the storage_backend_factory test seam) leaves
//   a storage that opens again, holds every binlog but the last completely
//   and the last one up to a transaction boundary, never holds data twice,
//   and resumes right after the data on disk;
// - a fault (a kill or a storage error) injected only while the stopped
//   storage is purged (stop -> purge_binlogs -> restart, the supported
//   retention procedure) leaves a storage that opens again and holds either
//   the binlogs from before the purge or those after it, every one matching
//   the model exactly, and resumes right after the data of the last one;
// - with an encrypted storage, a writer killed right after appending data
//   that its metadata does not cover yet, recovered by the next start (which
//   truncates that tail) and then writing different events, never encrypts
//   an offset twice with the same AES-CTR keystream (the XOR of the two
//   ciphertexts must not be the XOR of the two plaintexts).
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
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
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
#include "binsrv/events/common_header_flag_type.hpp"
#include "binsrv/events/common_header_view.hpp"
#include "binsrv/events/composite_binlog_name.hpp"
#include "binsrv/events/event_fwd.hpp"
#include "binsrv/events/protocol_traits_fwd.hpp"

#include "binsrv/gtids/common_types.hpp"
#include "binsrv/gtids/gtid.hpp"
#include "binsrv/gtids/gtid_set.hpp"
#include "binsrv/gtids/uuid.hpp"

#include "operations/event_generation_helpers.hpp"
#include "operations/sender_context.hpp"

#include "util/byte_range.hpp"
#include "util/byte_span.hpp"
#include "util/byte_span_fwd.hpp"
#include "util/byte_span_inserters.hpp"
#include "util/ctime_timestamp.hpp"
#include "util/semantic_version.hpp"
#include "util/ctime_timestamp_range.hpp"
#include "util/dynamic_byte_buffer_fwd.hpp"

namespace {

namespace gs = hegel::generators;

using property_testing::require;
using property_testing::run_property;

constexpr std::string_view binlog_base_name{"binlog"};
constexpr std::string_view transaction_uuid{
    "11111111-aaaa-1111-aaaa-111111111111"};

// the read-back properties start each binlog with a real FORMAT_DESCRIPTION,
// as a server does, because the reworked sender_context streams the binlog by
// fetching its FDE first
const util::semantic_version fde_server_version{8U, 4U, 8U};
constexpr std::uint32_t fde_server_id{42U};

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
    "port": 3307, "server_id": 142,
    "server_uuid": "00000000-0000-0000-0000-00000000008e",
    "read_timeout": 60, "write_timeout": 60,
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
  purge,
  // closing the binlog and opening the next one without discarding the
  // incomplete transaction first, as the collector does when the artificial
  // ROTATE of a reconnect names another binlog while the events re-sent at
  // the start of the old one (FORMAT_DESCRIPTION, PREVIOUS_GTIDS) are still
  // buffered
  close_mid_transaction
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
  case operation_kind::close_mid_transaction:
    return output << "close_mid_transaction";
  }
  return output;
}

constexpr std::size_t min_event_size{
    binsrv::events::common_header_view_base::size_in_bytes};
constexpr std::size_t max_event_size{512U};

[[nodiscard]] gs::Generator<operation>
operations(bool with_close_mid_transaction = false) {
  // writes are listed several times so that most sequences build up enough
  // data to exercise checkpointing
  std::vector<operation_kind> kinds{
      operation_kind::write_event, operation_kind::write_event,
      operation_kind::write_event, operation_kind::write_event,
      operation_kind::write_event, operation_kind::write_event,
      operation_kind::discard_incomplete_transaction, operation_kind::flush,
      operation_kind::rotate, operation_kind::rotate,
      operation_kind::restart, operation_kind::crash,
      operation_kind::purge};
  if (with_close_mid_transaction) {
    kinds.push_back(operation_kind::close_mid_transaction);
    kinds.push_back(operation_kind::close_mid_transaction);
  }
  return gs::compose([kinds](const hegel::TestCase &tc) {
    const auto kind{tc.draw(gs::sampled_from(kinds))};
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

// the stored events a reader must return are interleaved with the artificial
// ROTATE events the sender_context generates (a leading one, and one at every
// binlog boundary); those are not on disk, so drop them before comparing
void strip_artificial_rotates(event_list &events) {
  using header = binsrv::events::common_header_view_base;
  std::erase_if(events, [](const std::vector<std::byte> &bytes) {
    if (std::size(bytes) < header::size_in_bytes) {
      return false;
    }
    const binsrv::events::common_header_view view{
        util::const_byte_span{bytes}.subspan(0U, header::size_in_bytes)};
    return view.get_type_code() == binsrv::events::code_type::rotate &&
           view.get_flags().has_element(
               binsrv::events::common_header_flag_type::artificial);
  });
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
// fault injection through the storage_backend_factory test seam
// ---------------------------------------------------------------------------

// thrown by a backend call of a "crashed" process: the process is gone, so
// this and every later backend call fails (the storage destructor swallows
// it, as it swallows any error of its final flush)
class simulated_crash : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

// thrown once by the backend call chosen for an injected failure (an I/O
// error such as ENOSPC or a failed S3 request); the process keeps running
class injected_failure : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

enum class fault_kind : std::uint8_t { crash, failure };

// which backend call to hit and how; shared by every backend the factory
// creates while the plan is installed (the writer, a restarted writer, a
// purging instance)
struct fault_plan {
  // 1-based number of the mutating backend call to hit (put, resize,
  // remove, opening / writing / closing the binlog stream)
  std::size_t target_call{0U};
  fault_kind kind{fault_kind::crash};
  // for a crash: whether the chosen call still completes before the process
  // dies (otherwise it dies right before the call)
  bool after_call{false};
  // count only the calls that write binlog data to the stream (the crash
  // then always leaves data the metadata does not cover yet)
  bool only_stream_writes{false};

  // counting starts once armed, after the initial storage is set up
  bool armed{false};
  std::size_t calls{0U};
  bool triggered{false};
  bool dead{false};
  std::string triggered_at{};
  // takes the snapshot of what a killed process leaves behind
  std::function<void()> on_crash{};
};

std::ostream &operator<<(std::ostream &output, const fault_plan &plan) {
  return output << (plan.kind == fault_kind::crash ? "crash " : "failure ")
                << (plan.kind == fault_kind::crash && plan.after_call
                        ? "right after"
                        : "at")
                << " mutating backend call " << plan.target_call;
}

// forwards to the real backend, applying the plan to every mutating call
class faulty_backend final : public binsrv::basic_storage_backend {
public:
  faulty_backend(binsrv::basic_storage_backend_ptr inner, fault_plan &plan)
      : inner_{std::move(inner)}, plan_{&plan} {}

private:
  binsrv::basic_storage_backend_ptr inner_;
  fault_plan *plan_;

  void fail_if_dead() const {
    if (plan_->dead) {
      throw simulated_crash{"the process has crashed"};
    }
  }
  // true when the chosen call must die after it completes
  [[nodiscard]] bool before_mutation(const std::string &what) {
    fail_if_dead();
    if (!plan_->armed || plan_->triggered) {
      return false;
    }
    if (plan_->only_stream_writes &&
        !what.starts_with("write_data_to_stream")) {
      return false;
    }
    if (++plan_->calls != plan_->target_call) {
      return false;
    }
    plan_->triggered = true;
    plan_->triggered_at = what;
    if (plan_->kind == fault_kind::failure) {
      throw injected_failure{"injected backend failure at " + what};
    }
    if (plan_->after_call) {
      return true;
    }
    crash_now();
    return false;
  }
  void crash_now() {
    if (plan_->on_crash) {
      plan_->on_crash();
    }
    plan_->dead = true;
    throw simulated_crash{"crashed at " + plan_->triggered_at};
  }
  template <typename Call> void mutate(const std::string &what, Call call) {
    const bool die_after{before_mutation(what)};
    call();
    if (die_after) {
      crash_now();
    }
  }

  [[nodiscard]] binsrv::storage_object_name_container
  do_list_objects() override {
    fail_if_dead();
    return inner_->list_objects();
  }
  [[nodiscard]] util::dynamic_byte_buffer
  do_get_object(std::string_view name, const util::byte_range &range) override {
    fail_if_dead();
    return inner_->get_object(name, range);
  }
  void do_put_object(std::string_view name,
                     util::const_byte_span content) override {
    mutate("put_object(" + std::string{name} + ")",
           [&] { inner_->put_object(name, content); });
  }
  void do_resize_object(std::string_view name,
                        std::uint64_t new_size) override {
    mutate("resize_object(" + std::string{name} + ", " +
               std::to_string(new_size) + ")",
           [&] { inner_->resize_object(name, new_size); });
  }
  void do_remove_object(std::string_view name) override {
    // the wrapped backend's remove_object() includes its durability barrier
    mutate("remove_object(" + std::string{name} + ")",
           [&] { inner_->remove_object(name); });
  }
  void do_fsync() override { fail_if_dead(); }
  [[nodiscard]] std::uint64_t
  do_open_stream(std::string_view name,
                 binsrv::storage_backend_open_stream_mode mode) override {
    std::uint64_t result{0ULL};
    mutate("open_stream(" + std::string{name} + ")",
           [&] { result = inner_->open_stream(name, mode); });
    return result;
  }
  void do_write_data_to_stream(util::const_byte_span data) override {
    mutate("write_data_to_stream(" + std::to_string(std::size(data)) +
               " bytes)",
           [&] { inner_->write_data_to_stream(data); });
  }
  void do_close_stream() override {
    mutate("close_stream", [&] { inner_->close_stream(); });
  }
  [[nodiscard]] std::string do_get_description() const override {
    return inner_->get_description();
  }
  [[nodiscard]] std::string
  do_get_object_uri(std::string_view name) const override {
    return inner_->get_object_uri(name);
  }
};

// installs 'plan' for every backend the factory creates while it lives
class fault_injection_scope {
public:
  explicit fault_injection_scope(fault_plan &plan) {
    binsrv::storage_backend_factory::set_test_decorator(
        [&plan](binsrv::basic_storage_backend_ptr inner)
            -> binsrv::basic_storage_backend_ptr {
          return std::make_unique<faulty_backend>(std::move(inner), plan);
        });
  }
  fault_injection_scope(const fault_injection_scope &) = delete;
  fault_injection_scope &operator=(const fault_injection_scope &) = delete;
  fault_injection_scope(fault_injection_scope &&) = delete;
  fault_injection_scope &operator=(fault_injection_scope &&) = delete;
  ~fault_injection_scope() {
    binsrv::storage_backend_factory::set_test_decorator({});
  }
};

[[nodiscard]] gs::Generator<fault_plan> fault_plans() {
  return gs::compose([](const hegel::TestCase &tc) {
    static constexpr std::size_t max_target_call{80U};
    fault_plan result{};
    result.target_call = tc.draw(gs::integers<std::size_t>(
        {.min_value = 1U, .max_value = max_target_call}));
    result.kind = tc.draw(gs::booleans()) ? fault_kind::crash
                                          : fault_kind::failure;
    result.after_call =
        result.kind == fault_kind::crash && tc.draw(gs::booleans());
    return result;
  });
}

// a fault for the purge-only property: the purge of a stopped storage and
// the restart after it make only a handful of mutating backend calls (the
// binlog index, two removals per purged binlog, reopening the last binlog)
[[nodiscard]] gs::Generator<fault_plan> purge_fault_plans() {
  return gs::compose([](const hegel::TestCase &tc) {
    static constexpr std::size_t max_target_call{12U};
    fault_plan result{};
    result.target_call = tc.draw(gs::integers<std::size_t>(
        {.min_value = 1U, .max_value = max_target_call}));
    result.kind = tc.draw(gs::booleans()) ? fault_kind::crash
                                          : fault_kind::failure;
    result.after_call =
        result.kind == fault_kind::crash && tc.draw(gs::booleans());
    return result;
  });
}

// a fault for the start that follows a first fault: opening the storage
// (removing temporary objects and the leftovers of an interrupted rotation or
// purge, truncating the last binlog) and resuming in its last binlog make
// only a handful of mutating backend calls
[[nodiscard]] gs::Generator<fault_plan> restart_fault_plans() {
  return gs::compose([](const hegel::TestCase &tc) {
    static constexpr std::size_t max_target_call{6U};
    fault_plan result{};
    result.target_call = tc.draw(gs::integers<std::size_t>(
        {.min_value = 1U, .max_value = max_target_call}));
    result.kind = tc.draw(gs::booleans()) ? fault_kind::crash
                                          : fault_kind::failure;
    result.after_call =
        result.kind == fault_kind::crash && tc.draw(gs::booleans());
    return result;
  });
}

// ---------------------------------------------------------------------------
// what a storage has recorded
// ---------------------------------------------------------------------------

// a binlog the storage has recorded: listed in 'binlog.index', with a
// metadata file whose 'size' its data file holds at least, and that data up
// to that size. These are the two sources of truth the startup checks rely
// on (README, "Storage consistency checks"), read here from the objects
// themselves rather than through storage_core.
struct recorded_binlog {
  std::string name;
  std::uint64_t size;
  object_bytes data;
};
using recorded_binlog_list = std::vector<recorded_binlog>;

// the 'size' member of a binlog metadata file
[[nodiscard]] std::optional<std::uint64_t>
recorded_binlog_size(std::string_view metadata) {
  static constexpr std::string_view key{"\"size\""};
  auto position{metadata.find(key)};
  if (position == std::string_view::npos) {
    return std::nullopt;
  }
  position = metadata.find_first_not_of(" \t\r\n:", position + std::size(key));
  if (position == std::string_view::npos) {
    return std::nullopt;
  }
  std::uint64_t value{0ULL};
  const auto *const first{std::next(std::data(metadata),
                                    static_cast<std::ptrdiff_t>(position))};
  const auto *const last{std::data(metadata) + std::size(metadata)};
  const auto [end, error]{std::from_chars(first, last, value)};
  if (error != std::errc{} || end == first) {
    return std::nullopt;
  }
  return value;
}

[[nodiscard]] recorded_binlog_list
read_recorded_binlogs(storage_location &location) {
  recorded_binlog_list result;
  const auto names{location.object_names()};
  const auto present{[&names](std::string_view name) {
    return std::ranges::find(names, name) != std::cend(names);
  }};
  const std::string index_name{binsrv::storage_core::default_binlog_index_name};
  if (!present(index_name)) {
    return result;
  }
  for (const auto &line : split_lines(read_text(location, index_name))) {
    const auto name{std::filesystem::path{line}.filename().string()};
    const auto metadata_name{name + ".json"};
    if (!present(name) || !present(metadata_name)) {
      continue;
    }
    const auto size{recorded_binlog_size(read_text(location, metadata_name))};
    if (!size.has_value()) {
      continue;
    }
    auto data{location.read(name)};
    if (std::size(data) < *size) {
      continue;
    }
    data.resize(*size);
    result.push_back({.name = name, .size = *size, .data = std::move(data)});
  }
  return result;
}

// every binlog recorded before a start is still there after it, holding at
// least the recorded size, with the recorded data unchanged
void require_recorded_binlogs_kept(
    const recorded_binlog_list &recorded, storage_location &location,
    const binsrv::binlog_record_container &records, const std::string &label) {
  for (const auto &binlog : recorded) {
    const auto record_it{std::ranges::find_if(
        records, [&binlog](const binsrv::binlog_record &record) {
          return record.name.str() == binlog.name;
        })};
    require(record_it != std::cend(records),
            label + ": the start dropped '" + binlog.name +
                "', which binlog.index listed with " +
                std::to_string(binlog.size) + " recorded byte(s)");
    require(record_it->size >= binlog.size,
            label + ": the start left " + std::to_string(record_it->size) +
                " byte(s) of '" + binlog.name + "' instead of the " +
                std::to_string(binlog.size) + " recorded");
    const auto names{location.object_names()};
    require(std::ranges::find(names, binlog.name) != std::cend(names),
            label + ": the start removed the data file of '" + binlog.name +
                "'");
    const auto data{location.read(binlog.name)};
    require(std::size(data) >= binlog.size &&
                std::equal(std::cbegin(binlog.data), std::cend(binlog.data),
                           std::cbegin(data)),
            label + ": the start changed the recorded data of '" +
                binlog.name + "'");
  }
}

// ---------------------------------------------------------------------------
// running a sequence of operations
// ---------------------------------------------------------------------------

enum class checked_properties : std::uint8_t {
  model_agreement,
  checkpoint_size_bound,
  read_back,
  concurrent_read_back,
  damaged_files,
  query_read_only,
  close_mid_transaction,
  concurrent_queries,
  injected_fault,
  purge_fault,
  recorded_data_kept,
  restart_fault,
  query_keyring_error
};

// the properties whose writer is hit by an injected fault at a chosen backend
// call (and so neither crashes by operation nor purges while running)
[[nodiscard]] bool injects_faults(checked_properties properties) noexcept {
  return properties == checked_properties::injected_fault ||
         properties == checked_properties::recorded_data_kept ||
         properties == checked_properties::restart_fault;
}

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

// the first error any query thread of the concurrent-query property hit in
// this process: whether a query lands between a writer's directory read and
// its rename is a matter of timing, so the outcome does not repeat when Hegel
// replays a test case; the test case therefore reports it after the run
// instead of failing the property (which Hegel would report as flaky)
std::string first_concurrent_query_error{};

// what the query thread of the concurrent-query property saw; written only
// by that thread and read only after it has been joined
struct concurrent_query_result {
  std::size_t queries{0U};
  std::string error;
};

// opens the storage for queries again and again (as 'list' and
// 'search_by_*' do next to a running 'fetch' / 'pull') until stopped; every
// opening must succeed and list the binlogs without a gap: the first one
// (nothing is purged here) followed by each next one
void query_concurrently(const std::stop_token &stop,
                        const std::filesystem::path &config_path,
                        concurrent_query_result &result) {
  try {
    while (!stop.stop_requested()) {
      const auto storage{make_storage(
          config_path, binsrv::storage_construction_mode_type::querying_only)};
      const auto records{storage->get_binlog_records()};
      ++result.queries;
      binsrv::events::composite_binlog_name expected{binlog_base_name, 1U};
      for (const auto &record : records) {
        if (record.name != expected) {
          result.error = "query " + std::to_string(result.queries) +
                         " listed '" + record.name.str() + "' where '" +
                         expected.str() + "' was expected (" +
                         std::to_string(std::size(records)) + " record(s))";
          return;
        }
        expected = expected.next();
      }
      if (records.empty()) {
        result.error = "query " + std::to_string(result.queries) +
                       " listed no binlogs";
        return;
      }
      std::this_thread::yield();
    }
  } catch (const std::exception &e) {
    result.error = "query " + std::to_string(result.queries + 1U) +
                   " failed: " + e.what();
  }
}

class storage_harness {
public:
  // with 'early_plan', the fault plan is armed before the storage and its
  // first binlog are created, so that a fault can hit their creation too;
  // a fault there ends the start (see start_faulted())
  storage_harness(const storage_settings &settings,
                  checked_properties properties,
                  fault_plan *early_plan = nullptr)
      : settings_{settings}, properties_{properties},
        location_{make_location()}, config_path_{prepare_config()} {
    if (early_plan != nullptr) {
      arm(*early_plan);
    }
    try {
      storage_ = make_storage(config_path_);
      const auto status{
          storage_->open_binlog(model_.get_current_binlog().name)};
      require(status == binsrv::open_binlog_status::created,
              "the first binlog was not reported as created");
      write_first_binlog_event();
    } catch (const simulated_crash &) {
      start_faulted_ = true;
      return;
    } catch (const injected_failure &) {
      start_faulted_ = true;
      return;
    }
    restart_tailing_reader();
    if (properties_ == checked_properties::concurrent_read_back) {
      concurrent_reader_ = std::make_unique<operations::sender_context>(
          std::make_shared<binsrv::null_logger>(), storage_,
          settings_.tailing_reader_block_size, std::string_view{},
          binsrv::events::magic_binlog_offset,
          /*session_source_binlog_checksum=*/false);
      concurrent_thread_ = std::jthread{[this](const std::stop_token &stop) {
        read_concurrently(stop, *concurrent_reader_, concurrent_result_);
      }};
    }
    if (properties_ == checked_properties::concurrent_queries) {
      query_thread_ = std::jthread{[this](const std::stop_token &stop) {
        query_concurrently(stop, config_path_, query_result_);
      }};
    }
    check();
  }

  [[nodiscard]] bool start_faulted() const noexcept { return start_faulted_; }

  // called after the last operation: for the concurrent property, stops the
  // reader and compares what it received with what is on disk
  void finish() {
    if (properties_ == checked_properties::concurrent_queries) {
      query_thread_.request_stop();
      query_thread_.join();
      if (!query_result_.error.empty() &&
          first_concurrent_query_error.empty()) {
        first_concurrent_query_error = query_result_.error;
      }
      return;
    }
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
    strip_artificial_rotates(concurrent_result_.received);
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
      write_first_binlog_event();
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
      // with injected faults the crash happens at a chosen backend call
      if (!injects_faults(properties_) &&
          properties_ != checked_properties::purge_fault) {
        crash();
      }
      break;
    case operation_kind::purge:
      purge(op.purge_count);
      break;
    case operation_kind::close_mid_transaction:
      // whatever is buffered past the last transaction boundary is dropped
      // with the binlog it belongs to, as with 'rotate'
      storage_->close_binlog();
      model_.rotate();
      require(storage_->open_binlog(model_.get_current_binlog().name) ==
                  binsrv::open_binlog_status::created,
              "the next binlog was not reported as created");
      write_first_binlog_event();
      break;
    }
    // an injected failure in the final flush of a storage destructor (a
    // restart or a purge stops the writer) is swallowed by the destructor:
    // report it here, so that it is judged by what the next start finds like
    // any other failure, instead of comparing the reopened storage with a
    // model that assumed a clean stop
    if (armed_plan_ != nullptr && armed_plan_->triggered &&
        armed_plan_->kind == fault_kind::failure) {
      throw injected_failure{"swallowed by the storage destructor: " +
                             armed_plan_->triggered_at};
    }
    check();
  }

  // damages one file (or every binlog metadata file) of a copy of the
  // storage as it is on disk now, and opens the copy for streaming and for
  // queries
  // opens the storage for queries with 'query_settings' and checks that this
  // left every object exactly as it was ('querying_only' is the mode of
  // 'list' and 'search_by_*', which may run next to a writer and must not
  // race it on any object)
  void check_query_leaves_storage_unchanged(
      const storage_settings &query_settings) {
    if (query_settings.encryption.has_value()) {
      std::ofstream keyring{root_.path() / keyring_name};
      keyring << keyring_content;
    }
    const auto query_config{write_config(
        root_.path(), location_->config_members(), query_settings)};
    const auto before{snapshot()};
    const auto outcome{open_storage(
        query_config, binsrv::storage_construction_mode_type::querying_only)};
    require(outcome.records.has_value(),
            "the storage cannot be opened for queries: " + outcome.error);
    const auto after{snapshot()};
    std::string changes;
    for (const auto &[name, content] : before) {
      const auto found{after.find(name)};
      if (found == std::cend(after)) {
        changes += " '" + name + "' removed;";
      } else if (found->second != content) {
        changes += " '" + name + "' changed;";
      }
    }
    for (const auto &[name, content] : after) {
      if (!before.contains(name)) {
        changes += " '" + name + "' added;";
      }
    }
    require(changes.empty(),
            "opening the storage for queries changed it:" + changes);
  }

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

    // a binlog holding only the binlog magic looks exactly like the leftover
    // of an interrupted binlog creation, which the start removes (PBS-59):
    // losing its index entry, or the whole index when it is the only binlog,
    // loses nothing, so the storage may be accepted then
    const bool last_holds_only_magic{reference.back().size <=
                                     binsrv::events::magic_binlog_offset};

    switch (value.kind) {
    case damage_kind::delete_file:
      copy->remove(target_name);
      description += " deleting '" + target_name + "'";
      expected = target_name == index_name && std::size(reference) == 1U &&
                         last_holds_only_magic
                     ? expectation::either
                     : expectation::rejected;
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
        // dropping the first of several entries leaves what an interrupted
        // purge leaves (purge_binlogs commits the shorter index first), which
        // the start completes by removing that binlog (PBS-59)
        expected = (first == 0U && std::size(lines) > 1U) ||
                           (first + 1U == std::size(lines) &&
                            last_holds_only_magic)
                       ? expectation::either
                       : expectation::rejected;
        lines.erase(first_it);
        description += " dropping binlog index entry " + std::to_string(first);
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

    // the querying-only mode ignores inconsistencies and reports what is on
    // disk (README, "Storage consistency checks"): when the damage is
    // confined to the files of one binlog, or adds an object, opening for
    // queries still works and returns every other binlog (damage to the
    // binlog index or the storage metadata may change the whole answer)
    const bool damages_one_binlog{
        value.kind != damage_kind::damage_every_binlog_metadata &&
        value.kind != damage_kind::index_drop_entry &&
        value.kind != damage_kind::index_duplicate_entry &&
        value.kind != damage_kind::index_swap_entries &&
        value.kind != damage_kind::index_add_entry &&
        value.kind != damage_kind::index_crlf_line_endings &&
        (value.kind == damage_kind::add_unexpected_file ||
         (target_name != index_name && target_name != metadata_name))};
    if (damages_one_binlog) {
      const auto damaged_binlog{
          value.kind == damage_kind::add_unexpected_file
              ? std::string{}
              : (is_binlog_metadata(target_name)
                     ? binlog_of_metadata(target_name)
                     : target_name)};
      require(queried.records.has_value(),
              description + ": opening for queries failed: " + queried.error);
      for (const auto &record : reference) {
        const auto name{record.name.str()};
        if (name == damaged_binlog) {
          continue;
        }
        require(std::ranges::find(*queried.records, record.name,
                                  &binsrv::binlog_record::name) !=
                    std::cend(*queried.records),
                description + ": opening for queries left out '" + name +
                    "', whose files were not damaged");
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
  // what a process killed by an injected crash left behind
  std::unique_ptr<storage_location> fault_snapshot_{};
  // the plan installed by arm(), to notice a failure a destructor swallowed
  const fault_plan *armed_plan_{nullptr};
  // an early fault plan hit the creation of the storage or its first binlog
  bool start_faulted_{false};
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
  // the query thread of the concurrent-query property, likewise stopped and
  // joined before the result it writes to is destroyed
  concurrent_query_result query_result_{};
  std::jthread query_thread_{};

  void restart_tailing_reader() {
    tailing_reader_.reset();
    tailing_received_.clear();
    if (properties_ == checked_properties::read_back) {
      tailing_reader_ = std::make_unique<operations::sender_context>(
          std::make_shared<binsrv::null_logger>(), storage_,
          settings_.tailing_reader_block_size, std::string_view{},
          binsrv::events::magic_binlog_offset,
          /*session_source_binlog_checksum=*/false);
    }
  }

  [[nodiscard]] std::map<std::string, object_bytes> snapshot() {
    std::map<std::string, object_bytes> result;
    for (const auto &name : location_->object_names()) {
      result.emplace(name, location_->read(name));
    }
    return result;
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

  // writes one event to both the storage and the model with the GTID /
  // sequence number the collector would give it
  void store_event(std::vector<std::byte> bytes,
                   const util::ctime_timestamp &timestamp, bool at_boundary) {
    const auto gno{model_.get_current_gno()};
    // as in the collector, the sequence number comes with the first event
    // of a transaction (the GTID event) and the GTID applies to all of them
    const auto sequence_number{
        model_.is_first_event_of_transaction() ? gno : 0ULL};
    const auto transaction_gtid{
        settings_.gtid_mode
            ? binsrv::gtids::gtid{binsrv::gtids::uuid{transaction_uuid}, gno}
            : binsrv::gtids::gtid{}};
    // the model goes first: storage::write_event() buffers the event before
    // it may flush, so the event belongs to the storage's state even when an
    // injected fault interrupts that flush
    model_.write_event(bytes, timestamp, at_boundary);
    storage_->write_event(util::const_byte_span{bytes}, at_boundary,
                          transaction_gtid, timestamp, sequence_number);
  }

  void write_event(const operation &op) {
    store_event(next_event_bytes(op.event_size, op.timestamp),
                util::ctime_timestamp{static_cast<std::time_t>(op.timestamp)},
                op.at_transaction_boundary);
  }

  // the first event of every binlog is a real FORMAT_DESCRIPTION, as a server
  // sends it; the reworked sender_context fetches it to start streaming, so
  // the read-back properties need it on disk (the others keep their synthetic
  // events only)
  void write_first_binlog_event() {
    if (!reads_back(properties_)) {
      return;
    }
    binsrv::events::event_storage buffer;
    operations::generate_format_description_event_ex(
        buffer, fde_server_version, binsrv::events::magic_binlog_offset,
        fde_server_id, /*enable_checksum_algorithm=*/true,
        /*artificial=*/false);
    store_event({std::cbegin(buffer), std::cend(buffer)},
                util::ctime_timestamp{std::time_t{1}}, /*at_boundary=*/true);
  }

  void reopen_current_binlog(binsrv::storage &target, std::string_view label) {
    const auto status{target.open_binlog(model_.get_current_binlog().name)};
    require(status != binsrv::open_binlog_status::created,
            std::string{label} + ": the existing binlog was created anew");
  }

  // purges the oldest binlogs; only the injected-fault property uses it,
  // through the supported procedure (purging while the writer keeps running
  // is unsupported: PBS-75)
  void purge(std::size_t requested_count) {
    const auto &binlogs{model_.get_binlogs()};
    if (!injects_faults(properties_) || std::size(binlogs) < 2U) {
      return;
    }
    purge_stopped(requested_count);
  }

  // the retention procedure the README asks for: stop the writer, run
  // 'purge_binlogs', start the writer again
  void purge_stopped(std::size_t requested_count) {
    const auto &binlogs{model_.get_binlogs()};
    const auto count{std::min(requested_count, std::size(binlogs) - 1U)};
    const auto target{binlogs[count - 1U].name};
    storage_.reset();
    model_.drop_incomplete_transaction();
    std::string warning;
    {
      const auto purger{make_storage(
          config_path_, binsrv::storage_construction_mode_type::purging)};
      warning = purger->purge_binlogs(target).second;
    }
    model_.purge(count);
    if (!warning.empty()) {
      // the purge committed the new index but could not remove every file
      // (an injected failure inside the batch removal): judged like any
      // other failure, by what the next start finds
      throw injected_failure{"purge_binlogs reported a warning: " + warning};
    }
    storage_ = make_storage(config_path_);
    reopen_current_binlog(*storage_, "purge");
  }

public:
  // the purge-only fault property: stops the writer cleanly, then runs the
  // supported retention procedure (purge_binlogs on the stopped storage, then
  // a restart) with 'plan' armed for exactly those backend calls, so that
  // faults while writing or rotating (tickets 15/18) cannot mask it
  void purge_with_fault(std::size_t requested_count, fault_plan &plan) {
    while (std::size(model_.get_binlogs()) < 2U) {
      apply(operation{.kind = operation_kind::rotate,
                      .event_size = 0U,
                      .at_transaction_boundary = false,
                      .timestamp = 0U,
                      .purge_count = 0U});
    }
    const auto &binlogs{model_.get_binlogs()};
    const auto count{std::min(requested_count, std::size(binlogs) - 1U)};
    const auto target{binlogs[count - 1U].name};
    std::vector<std::string> before_names;
    for (const auto &binlog : binlogs) {
      before_names.push_back(binlog.name.str());
    }
    const std::vector<std::string> after_names(
        std::next(std::cbegin(before_names),
                  static_cast<std::ptrdiff_t>(count)),
        std::cend(before_names));

    tailing_reader_.reset();
    storage_.reset();
    model_.drop_incomplete_transaction();
    arm(plan);
    std::string warning;
    try {
      {
        const auto purger{make_storage(
            config_path_, binsrv::storage_construction_mode_type::purging)};
        warning = purger->purge_binlogs(target).second;
      }
      if (warning.empty()) {
        storage_ = make_storage(config_path_);
        reopen_current_binlog(*storage_, "restart after the purge");
      }
    } catch (const std::exception &) {
      if (!plan.triggered) {
        throw;
      }
    }
    storage_.reset();
    plan.armed = false;
    plan.dead = false;

    const bool crashed{plan.triggered && plan.kind == fault_kind::crash};
    storage_location &leftover{crashed ? *fault_snapshot_ : *location_};
    const auto config{crashed ? write_config(root_.path(),
                                             leftover.config_members(),
                                             settings_)
                              : config_path_};
    std::ostringstream label_stream;
    label_stream << "purging " << count << " of " << std::size(before_names)
                 << " binlog(s) up to '" << target.str() << "'";
    if (plan.triggered) {
      label_stream << (crashed ? ", crash " : ", failure ")
                   << (crashed && plan.after_call ? "right after " : "at ")
                   << plan.triggered_at;
    }
    if (!warning.empty()) {
      label_stream << " (purge_binlogs warning: " << warning << ')';
    }
    const auto label{label_stream.str()};

    binsrv::storage_ptr reopened;
    try {
      reopened = make_storage(config);
    } catch (const std::exception &e) {
      require(false,
              label + ": the storage cannot be opened again: " + e.what());
    }
    const auto records{reopened->get_binlog_records()};
    std::vector<std::string> names;
    for (const auto &record : records) {
      names.push_back(record.name.str());
    }
    const auto join{[](const std::vector<std::string> &values) {
      std::string result;
      for (const auto &value : values) {
        result += (result.empty() ? "" : ", ") + value;
      }
      return "[" + result + "]";
    }};
    require(names == before_names || names == after_names,
            label + ": the storage holds " + join(names) +
                " instead of the binlogs before the purge " +
                join(before_names) + " or after it " + join(after_names));
    if (names == after_names) {
      model_.purge(count);
    }
    static_cast<void>(require_disk_matches_model(leftover, records, model_,
                                                 settings_));
    const auto &last{records.back()};
    binsrv::open_binlog_status status{};
    try {
      status = reopened->open_binlog(last.name);
    } catch (const std::exception &e) {
      require(false, label + ": cannot resume in '" + last.name.str() +
                         "': " + e.what());
    }
    require(status != binsrv::open_binlog_status::created,
            label + ": resuming created '" + last.name.str() + "' anew");
    require(reopened->get_current_position() == last.size,
            label + ": resuming at position " +
                std::to_string(reopened->get_current_position()) +
                " instead of right after the data of '" + last.name.str() +
                "' (" + std::to_string(last.size) + ")");
  }

  // installs the crash snapshot for 'plan' and starts counting its calls; the
  // snapshot is a copy of 'source' (by default the writer's storage)
  void arm(fault_plan &plan, storage_location *source = nullptr) {
    storage_location &copied{source != nullptr ? *source : *location_};
    plan.on_crash = [this, &copied] {
      fault_snapshot_ = location_->make_sibling("fault");
      copied.copy_to(*fault_snapshot_);
    };
    plan.armed = true;
    armed_plan_ = &plan;
  }

  // opening an encrypted storage for queries with a configuration that cannot
  // decrypt it is a configuration error, not damage to the storage: it fails
  // with an error naming the keyring / KEK, as streaming does, instead of
  // skipping every encrypted binlog (ticket 35). The variants: 0 - the keyring
  // lacks the storage's KEK and the encryption section names another one it
  // holds, 1 - the keyring lacks the KEK the encryption section names, 2 - no
  // keyring and no encryption section
  void check_query_keyring_error(std::size_t variant) {
    tailing_reader_.reset();
    storage_.reset();
    require(settings_.encryption.has_value(),
            "the keyring property needs an encrypted storage");
    const auto &stored_kek{settings_.encryption->kek_id};

    const auto directory{root_.path() / "wrong-keyring"};
    std::filesystem::create_directories(directory);
    std::string keys;
    for (const auto &line : split_lines(keyring_content)) {
      if (line.find(R"({"id": ")") == std::string::npos ||
          line.find('"' + stored_kek + '"') != std::string::npos) {
        continue;
      }
      auto key{line};
      while (!key.empty() && (key.back() == ',' || key.back() == ' ')) {
        key.pop_back();
      }
      keys += (keys.empty() ? "" : ",\n") + key;
    }
    {
      std::ofstream keyring{directory / keyring_name};
      keyring << "{\"version\": 1, \"keys\": [\n" << keys << "\n]}";
    }

    auto query_settings{settings_};
    std::string label;
    switch (variant % 3U) {
    case 0U:
      query_settings.encryption =
          stored_kek == "gamma32"
              ? encryption_settings{.kek_id = "gamma16",
                                    .data_cipher = "AES-128-CTR"}
              : encryption_settings{.kek_id = "gamma32",
                                    .data_cipher = "AES-256-CTR"};
      label = "a keyring without '" + stored_kek +
              "' and an encryption section naming '" +
              query_settings.encryption->kek_id + "'";
      break;
    case 1U:
      label = "a keyring without '" + stored_kek +
              "', which the encryption section names";
      break;
    default:
      query_settings.encryption.reset();
      label = "no keyring and no encryption section";
      break;
    }
    const auto config{write_config(directory, location_->config_members(),
                                   query_settings)};
    const auto queried{open_storage(
        config, binsrv::storage_construction_mode_type::querying_only)};
    require(!queried.records.has_value(),
            "opening the encrypted storage for queries with " + label +
                " succeeded with " +
                std::to_string(std::size(*queried.records)) +
                " binlog(s) instead of failing");
    require(queried.error.find("keyring") != std::string::npos ||
                queried.error.find("KEK") != std::string::npos ||
                queried.error.find("encrypt") != std::string::npos,
            "opening the encrypted storage for queries with " + label +
                " failed without naming the keyring, the KEK or the "
                "encryption: " + queried.error);
  }

  // the restart after a fault keeps everything the storage had recorded
  // (README, "Storage consistency checks": binlog.index and the binlog
  // metadata are the sources of truth): the next start may remove the
  // leftovers of an interrupted operation and truncate data the metadata
  // does not cover, but never drops a recorded binlog or recorded data
  void check_recorded_data_after_fault(fault_plan &plan) {
    tailing_reader_.reset();
    storage_.reset();
    plan.armed = false;
    plan.dead = false;
    const bool crashed{plan.kind == fault_kind::crash};
    storage_location &leftover{crashed ? *fault_snapshot_ : *location_};
    const auto config{crashed ? write_config(root_.path(),
                                             leftover.config_members(),
                                             settings_)
                              : config_path_};
    const auto label{describe_fault(plan, "after a ")};
    const auto recorded{read_recorded_binlogs(leftover)};
    require_restart_keeps(recorded, leftover, config, label);
  }

  // a second fault, during the start that follows the first one (while it
  // removes leftovers, truncates the last binlog or resumes in it), still
  // leaves a storage that the start after it opens, with everything that
  // was recorded before the first restart
  void check_fault_during_restart(fault_plan &plan,
                                  const fault_plan &second) {
    tailing_reader_.reset();
    storage_.reset();
    plan.armed = false;
    plan.dead = false;
    const auto first_label{describe_fault(plan, "after a ")};

    // the storage the restart finds, copied so that a second crash can be
    // snapshotted from it
    const auto restarted{location_->make_sibling("restart")};
    (plan.kind == fault_kind::crash ? *fault_snapshot_ : *location_)
        .copy_to(*restarted);
    const auto restart_config{write_config(
        root_.path(), restarted->config_members(), settings_)};
    const auto recorded{read_recorded_binlogs(*restarted)};

    plan.target_call = second.target_call;
    plan.kind = second.kind;
    plan.after_call = second.after_call;
    plan.calls = 0U;
    plan.triggered = false;
    plan.triggered_at.clear();
    arm(plan, restarted.get());
    try {
      const auto storage{make_storage(restart_config)};
      const auto records{storage->get_binlog_records()};
      if (!records.empty()) {
        static_cast<void>(storage->open_binlog(records.back().name));
      }
    } catch (const std::exception &e) {
      if (!plan.triggered) {
        require(false, first_label + ": the restart failed without a "
                                     "second fault: " + e.what());
      }
    }
    plan.armed = false;
    plan.dead = false;

    const bool second_crashed{plan.triggered &&
                              plan.kind == fault_kind::crash};
    storage_location &leftover{second_crashed ? *fault_snapshot_
                                              : *restarted};
    const auto config{second_crashed
                          ? write_config(root_.path(),
                                         leftover.config_members(), settings_)
                          : restart_config};
    const auto label{
        first_label +
        (plan.triggered ? describe_fault(plan, ", then a ") +
                              " during the restart"
                        : std::string{", then a restart without a fault"})};
    require_restart_keeps(recorded, leftover, config, label);
  }

  // what the next start finds after the fault: a crashed process leaves the
  // snapshot taken at the crash; a process that hit a failure stops (the
  // collector does not handle storage errors) and its storage destructor
  // runs the final flush. Either way the storage must open, hold every
  // binlog but the last completely and the last one up to a transaction
  // boundary, with records that describe exactly that, and resume right
  // after the data of the last binlog.
  void check_after_fault(fault_plan &plan) {
    tailing_reader_.reset();
    storage_.reset();
    plan.armed = false;
    plan.dead = false;
    const bool crashed{plan.kind == fault_kind::crash};
    storage_location &leftover{crashed ? *fault_snapshot_ : *location_};
    const auto config{crashed ? write_config(root_.path(),
                                             leftover.config_members(),
                                             settings_)
                              : config_path_};
    std::ostringstream label_stream;
    label_stream << (crashed ? "after a crash " : "after a failure ")
                 << (crashed && plan.after_call ? "right after " : "at ")
                 << plan.triggered_at;
    const auto label{label_stream.str()};

    binsrv::storage_ptr reopened;
    try {
      reopened = make_storage(config);
    } catch (const std::exception &e) {
      require(false,
              label + ": the storage cannot be opened again: " + e.what());
    }
    const auto records{reopened->get_binlog_records()};
    require_resumable_prefix(leftover, records, label);

    const auto &last{records.back()};
    binsrv::open_binlog_status status{};
    try {
      status = reopened->open_binlog(last.name);
    } catch (const std::exception &e) {
      require(false, label + ": cannot resume in '" + last.name.str() +
                         "': " + e.what());
    }
    require(status != binsrv::open_binlog_status::created,
            label + ": resuming created '" + last.name.str() + "' anew");
    require(reopened->get_current_position() == last.size,
            label + ": resuming at position " +
                std::to_string(reopened->get_current_position()) +
                " instead of right after the data of '" + last.name.str() +
                "' (" + std::to_string(last.size) + ")");
  }

private:
  [[nodiscard]] static std::string describe_fault(const fault_plan &plan,
                                                  std::string_view prefix) {
    const bool crashed{plan.kind == fault_kind::crash};
    return std::string{prefix} + (crashed ? "crash " : "failure ") +
           (crashed && plan.after_call ? "right after " : "at ") +
           plan.triggered_at;
  }

  // opens what a fault left behind ('location', through 'config') and
  // resumes in its last binlog, requiring that every binlog in 'recorded'
  // is still there with its recorded data, after opening and after resuming
  void require_restart_keeps(const recorded_binlog_list &recorded,
                             storage_location &location,
                             const std::filesystem::path &config,
                             const std::string &label) {
    binsrv::storage_ptr reopened;
    try {
      reopened = make_storage(config);
    } catch (const std::exception &e) {
      require(false,
              label + ": the storage cannot be opened again: " + e.what());
    }
    const auto records{reopened->get_binlog_records()};
    require_recorded_binlogs_kept(recorded, location, records, label);
    if (records.empty()) {
      return;
    }
    try {
      static_cast<void>(reopened->open_binlog(records.back().name));
    } catch (const std::exception &e) {
      require(false, label + ": cannot resume in '" +
                         records.back().name.str() + "': " + e.what());
    }
    require_recorded_binlogs_kept(recorded, location,
                                  reopened->get_binlog_records(),
                                  label + " (after resuming)");
  }

  // the stored binlogs are consecutive binlogs of the model (a purge stopped
  // by the fault may already have removed some of the oldest); every one
  // but the last holds all of its transactions, the last a prefix of them,
  // and every record matches its file
  void require_resumable_prefix(storage_location &location,
                                const binsrv::binlog_record_container &records,
                                const std::string &label) {
    const auto &binlogs{model_.get_binlogs()};
    require(!records.empty(), label + ": no binlogs left");
    const auto first_it{std::ranges::find(binlogs, records.front().name,
                                          &model_binlog::name)};
    require(first_it != std::cend(binlogs),
            label + ": unexpected first binlog '" +
                records.front().name.str() + "'");
    const auto first_index{
        static_cast<std::size_t>(std::distance(std::cbegin(binlogs), first_it))};
    require(first_index + std::size(records) <= std::size(binlogs),
            label + ": " + std::to_string(std::size(records)) +
                " binlog record(s) from '" + records.front().name.str() +
                "' but the model has only " +
                std::to_string(std::size(binlogs) - first_index));

    const bool encrypted{settings_.encryption.has_value()};
    binsrv::gtids::gtid_set earlier_gtids{model_.get_purged_gtids()};
    for (std::size_t index{0U}; index < first_index; ++index) {
      earlier_gtids += model_gtids(binlogs[index],
                                   std::size(binlogs[index].transactions));
    }
    for (std::size_t index{0U}; index < std::size(records); ++index) {
      const auto &binlog{binlogs[first_index + index]};
      const auto file_content{location.read(binlog.name.str())};
      const auto transactions_on_disk{
          match_file_with_model(file_content, binlog, encrypted)};
      if (index + 1U < std::size(records)) {
        require(transactions_on_disk == std::size(binlog.transactions),
                label + ": " + describe_binlog(binlog) + " lost " +
                    std::to_string(std::size(binlog.transactions) -
                                   transactions_on_disk) +
                    " complete transaction(s) although a later binlog "
                    "exists");
      }
      require_record_matches(records[index], binlog, transactions_on_disk,
                             std::size(file_content), earlier_gtids,
                             settings_.gtid_mode, encrypted);
      earlier_gtids += model_gtids(binlog, transactions_on_disk);
    }
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
    const auto state{require_disk_matches_model(
        *location_, storage_->get_binlog_records(), model_,
        settings_)};
    const auto &current{model_.get_current_binlog()};
    const auto unflushed{
        unflushed_complete_size(current, state.transactions_on_disk)};

    if (properties_ == checked_properties::model_agreement ||
        properties_ == checked_properties::damaged_files ||
        properties_ == checked_properties::close_mid_transaction ||
        injects_faults(properties_) ||
        properties_ == checked_properties::purge_fault) {
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
      // the stream also carries one artificial ROTATE per binlog plus the
      // leading one, which are stripped before the comparison
      const auto slack{std::size(model_.get_binlogs()) + 1U};

      read_until_end(*tailing_reader_, tailing_received_,
                     std::size(expected) + slack, "tailing reader");
      auto tailing{tailing_received_};
      strip_artificial_rotates(tailing);
      require_same_events(tailing, expected, "tailing reader");

      operations::sender_context fresh_reader{
          std::make_shared<binsrv::null_logger>(), storage_,
          settings_.fresh_reader_block_size, std::string_view{},
          binsrv::events::magic_binlog_offset,
          /*session_source_binlog_checksum=*/false};
      event_list fresh_received;
      read_until_end(fresh_reader, fresh_received,
                     std::size(expected) + slack, "fresh reader");
      strip_artificial_rotates(fresh_received);
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
  const auto ops{tc.draw(
      "ops", gs::vectors(operations(
                 properties == checked_properties::close_mid_transaction)))};
  std::optional<damage> damage_value;
  if (properties == checked_properties::damaged_files) {
    damage_value = tc.draw("damage", damages());
  }
  // the configuration of a query run: the same storage, encrypted in the
  // configuration also when the storage was created without encryption
  // (only adding encryption is accepted for an existing storage)
  std::optional<storage_settings> query_settings;
  if (properties == checked_properties::query_read_only) {
    query_settings = settings;
    if (!settings.encryption.has_value() &&
        tc.draw("query_adds_encryption", gs::booleans())) {
      query_settings->encryption =
          encryption_settings{.kek_id = "gamma32", .data_cipher = "AES-256-CTR"};
    }
  }
  if (properties == checked_properties::purge_fault) {
    static constexpr std::size_t max_purge_count{3U};
    const auto purge_count{tc.draw(
        "purge_count", gs::integers<std::size_t>(
                           {.min_value = 1U, .max_value = max_purge_count}))};
    auto plan{tc.draw("fault", purge_fault_plans())};
    const fault_injection_scope scope{plan};
    storage_harness harness{settings, properties};
    for (const auto &op : ops) {
      harness.apply(op);
    }
    harness.purge_with_fault(purge_count, plan);
    return;
  }
  if (properties == checked_properties::injected_fault) {
    auto plan{tc.draw("fault", fault_plans())};
    const fault_injection_scope scope{plan};
    storage_harness harness{settings, properties};
    harness.arm(plan);
    try {
      for (const auto &op : ops) {
        harness.apply(op);
      }
    } catch (const simulated_crash &) {
      harness.check_after_fault(plan);
      return;
    } catch (const injected_failure &) {
      harness.check_after_fault(plan);
      return;
    }
    plan.armed = false;
    return;
  }
  if (properties == checked_properties::recorded_data_kept ||
      properties == checked_properties::restart_fault) {
    auto plan{tc.draw("fault", fault_plans())};
    std::optional<fault_plan> second;
    if (properties == checked_properties::restart_fault) {
      second = tc.draw("restart_fault", restart_fault_plans());
    }
    // whether the fault may also hit the creation of the storage and its
    // first binlog (the interrupted-first-binlog recovery)
    const bool fault_from_start{tc.draw("fault_from_start", gs::booleans())};
    const fault_injection_scope scope{plan};
    storage_harness harness{settings, properties,
                            fault_from_start ? &plan : nullptr};
    if (!fault_from_start) {
      harness.arm(plan);
    }
    if (!harness.start_faulted()) {
      try {
        for (const auto &op : ops) {
          harness.apply(op);
        }
      } catch (const simulated_crash &) {
      } catch (const injected_failure &) {
      }
    }
    if (!plan.triggered) {
      plan.armed = false;
      return;
    }
    if (second.has_value()) {
      harness.check_fault_during_restart(plan, *second);
    } else {
      harness.check_recorded_data_after_fault(plan);
    }
    return;
  }
  std::optional<std::size_t> keyring_variant;
  if (properties == checked_properties::query_keyring_error) {
    if (!settings.encryption.has_value()) {
      settings.encryption =
          encryption_settings{.kek_id = "gamma32", .data_cipher = "AES-256-CTR"};
    }
    keyring_variant = tc.draw(
        "keyring_variant",
        gs::integers<std::size_t>({.min_value = 0U, .max_value = 2U}));
  }
  storage_harness harness{settings, properties};
  for (const auto &op : ops) {
    harness.apply(op);
  }
  harness.finish();
  if (keyring_variant.has_value()) {
    harness.check_query_keyring_error(*keyring_variant);
  }
  if (damage_value.has_value()) {
    harness.check_damaged_copy(*damage_value);
  }
  if (query_settings.has_value()) {
    harness.check_query_leaves_storage_unchanged(*query_settings);
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


// ---------------------------------------------------------------------------
// keystream reuse after crash recovery
// ---------------------------------------------------------------------------

// one event of the keystream property, optionally followed by a flush
struct keystream_write {
  std::size_t event_size;
  bool at_transaction_boundary;
  bool flush_after;
};

std::ostream &operator<<(std::ostream &output, const keystream_write &value) {
  return output << "write(" << value.event_size << " bytes"
                << (value.at_transaction_boundary ? ", end of transaction" : "")
                << (value.flush_after ? ", flush" : "") << ')';
}

[[nodiscard]] gs::Generator<keystream_write> keystream_writes() {
  return gs::compose([](const hegel::TestCase &tc) {
    return keystream_write{
        .event_size = tc.draw(gs::integers<std::size_t>(
            {.min_value = min_event_size, .max_value = max_event_size})),
        .at_transaction_boundary = tc.draw(gs::booleans()),
        .flush_after = tc.draw(gs::booleans())};
  });
}

// the bytes of an event: a valid common header and a pattern that depends on
// 'seed', so that the two lives of the storage write different bytes
[[nodiscard]] std::vector<std::byte> keystream_event_bytes(std::size_t size,
                                                           std::uint64_t seed) {
  using header = binsrv::events::common_header_view_base;
  static constexpr std::uint64_t byte_multiplier{29ULL};
  std::vector<std::byte> result(size);
  for (std::size_t index{0U}; index < size; ++index) {
    result[index] =
        static_cast<std::byte>((seed + index * byte_multiplier) & 0xFFULL);
  }
  assert(size >= header::size_in_bytes);
  const auto write_field{[&result](std::size_t offset, auto value) {
    util::byte_span field{std::next(std::data(result),
                                    static_cast<std::ptrdiff_t>(offset)),
                          sizeof value};
    util::insert_fixed_int_to_byte_span(field, value);
  }};
  write_field(header::type_code_offset,
              static_cast<std::uint8_t>(binsrv::events::code_type::query));
  write_field(header::event_size_offset, static_cast<std::uint32_t>(size));
  return result;
}

// writes the events to the storage and appends them to 'plaintext' (the
// binlog file is the magic followed by every event in order, as nothing is
// discarded)
void write_keystream_events(binsrv::storage &target,
                            const std::vector<keystream_write> &writes,
                            std::uint64_t seed,
                            std::vector<std::byte> &plaintext) {
  static constexpr std::uint64_t event_seed_step{97ULL};
  for (const auto &write : writes) {
    const auto bytes{keystream_event_bytes(write.event_size, seed)};
    seed += event_seed_step;
    plaintext.insert(std::end(plaintext), std::cbegin(bytes), std::cend(bytes));
    target.write_event(util::const_byte_span{bytes},
                       write.at_transaction_boundary, binsrv::gtids::gtid{},
                       util::ctime_timestamp{std::time_t{1}}, 0ULL);
    if (write.flush_after) {
      target.flush_event_buffer();
    }
  }
  target.flush_event_buffer();
}

// AES-CTR turns the same key, IV and offset into the same keystream byte,
// so no offset of a binlog may be encrypted twice for different plaintext:
// the XOR of the two ciphertexts would be the XOR of the two plaintexts.
// A writer killed right after appending data its metadata does not cover
// yet leaves a tail that the next start truncates (storage_core recovery);
// whatever is written next lands at the same offsets of the same file,
// whose key and IV come from the same binlog metadata.
void run_keystream_property(hegel::TestCase &tc) {
  const auto kek_id{tc.draw("kek_id", gs::sampled_from(keyring_key_ids))};
  const bool block_mode_kek{std::ranges::find(block_mode_key_ids, kek_id) !=
                            std::cend(block_mode_key_ids)};
  const auto data_cipher{tc.draw(
      "data_cipher",
      gs::sampled_from(block_mode_kek
                           ? std::vector<std::string>{"AES-128-CTR",
                                                      "AES-256-CTR"}
                           : std::vector<std::string>{"AES-128-CTR",
                                                      "AES-192-CTR",
                                                      "AES-256-CTR"}))};
  static constexpr std::uint64_t min_checkpoint_size{64ULL};
  static constexpr std::uint64_t max_checkpoint_size{4096ULL};
  std::optional<std::uint64_t> checkpoint_size{};
  if (tc.draw("with_checkpoint_size", gs::booleans())) {
    checkpoint_size = tc.draw(
        "checkpoint_size",
        gs::integers<std::uint64_t>({.min_value = min_checkpoint_size,
                                     .max_value = max_checkpoint_size}));
  }
  static constexpr std::size_t max_writes{30U};
  const auto first_life{tc.draw(
      "first_life",
      gs::vectors(keystream_writes(), {.min_size = 1U, .max_size = max_writes}))};
  static constexpr std::size_t max_crash_write{20U};
  const auto crash_write{tc.draw(
      "crash_after_stream_write",
      gs::integers<std::size_t>({.min_value = 1U, .max_value = max_crash_write}))};
  const auto second_life{tc.draw(
      "second_life",
      gs::vectors(keystream_writes(), {.min_size = 1U, .max_size = max_writes}))};

  const storage_settings settings{
      .s3 = std::nullopt,
      .gtid_mode = false,
      .checkpoint_size = checkpoint_size,
      .encryption =
          encryption_settings{.kek_id = kek_id, .data_cipher = data_cipher},
      .tailing_reader_block_size = 0U,
      .fresh_reader_block_size = 0U};
  const scratch_directory root{};
  {
    std::ofstream keyring{root.path() / keyring_name};
    keyring << keyring_content;
  }
  directory_location location{root.path() / "storage"};
  const auto config{
      write_config(root.path(), location.config_members(), settings)};
  const binsrv::events::composite_binlog_name name{binlog_base_name, 1U};

  fault_plan plan{};
  plan.kind = fault_kind::crash;
  plan.after_call = true;
  plan.only_stream_writes = true;
  plan.target_call = crash_write;
  std::unique_ptr<storage_location> snapshot{};
  plan.on_crash = [&location, &snapshot] {
    snapshot = location.make_sibling("crash");
    location.copy_to(*snapshot);
  };
  const fault_injection_scope scope{plan};

  // the first life, killed right after its chosen data write
  std::vector<std::byte> first_plaintext{
      std::cbegin(binsrv::events::magic_binlog_payload),
      std::cend(binsrv::events::magic_binlog_payload)};
  {
    const auto writer{make_storage(config)};
    require(writer->open_binlog(name) == binsrv::open_binlog_status::created,
            "the binlog was not created");
    plan.armed = true;
    try {
      write_keystream_events(*writer, first_life, 1ULL, first_plaintext);
    } catch (const simulated_crash &) {
    }
  }
  plan.armed = false;
  plan.dead = false;
  if (!snapshot) {
    // the first life wrote less often than the chosen crash point
    return;
  }
  const auto first_ciphertext{snapshot->read(name.str())};

  // the second life: the next start recovers the copy the crash left behind
  // and appends different events
  const auto crash_config{
      write_config(root.path(), snapshot->config_members(), settings)};
  std::uint64_t resumed_at{0ULL};
  std::vector<std::byte> second_plaintext;
  {
    const auto writer{make_storage(crash_config)};
    require(writer->open_binlog(name) != binsrv::open_binlog_status::created,
            "the recovered binlog was created anew");
    resumed_at = writer->get_current_position();
    require(resumed_at <= std::size(first_ciphertext) &&
                resumed_at <= std::size(first_plaintext),
            "resuming at " + std::to_string(resumed_at) +
                ", past the data of the first life");
    second_plaintext.assign(
        std::cbegin(first_plaintext),
        std::next(std::cbegin(first_plaintext),
                  static_cast<std::ptrdiff_t>(resumed_at)));
    static constexpr std::uint64_t second_life_seed{0x5AULL};
    write_keystream_events(*writer, second_life, second_life_seed,
                           second_plaintext);
  }
  const auto second_ciphertext{snapshot->read(name.str())};

  // sanity: the data really is encrypted, and recovery kept the prefix
  require(std::size(first_ciphertext) <= std::size(first_plaintext),
          "the first life left more bytes than it wrote");
  require(!std::equal(std::cbegin(first_ciphertext),
                      std::cend(first_ciphertext),
                      std::cbegin(first_plaintext)),
          "the binlog is stored unencrypted");
  require(std::size(second_ciphertext) >= resumed_at &&
              std::equal(std::cbegin(first_ciphertext),
                         std::next(std::cbegin(first_ciphertext),
                                   static_cast<std::ptrdiff_t>(resumed_at)),
                         std::cbegin(second_ciphertext)),
          "recovery changed the binlog before " + std::to_string(resumed_at));

  // offsets written in both lives with different plaintext, and how many of
  // them were encrypted with the same keystream byte; with independent
  // keystreams that happens for about 1 in 256 of them
  const auto overlap_end{std::min(std::size(first_ciphertext),
                                  std::size(second_ciphertext))};
  std::size_t differing{0U};
  std::size_t same_keystream{0U};
  for (auto offset{static_cast<std::size_t>(resumed_at)}; offset < overlap_end;
       ++offset) {
    if (first_plaintext[offset] == second_plaintext[offset]) {
      continue;
    }
    ++differing;
    if ((first_ciphertext[offset] ^ first_plaintext[offset]) ==
        (second_ciphertext[offset] ^ second_plaintext[offset])) {
      ++same_keystream;
    }
  }
  static constexpr std::size_t min_reused{8U};
  static constexpr std::size_t chance_divisor{16U};
  require(same_keystream < min_reused ||
              same_keystream * chance_divisor <= differing,
          "the keystream of bytes " + std::to_string(resumed_at) + ".." +
              std::to_string(overlap_end) + " (" +
              std::to_string(std::size(first_ciphertext) - resumed_at) +
              " byte(s) truncated by recovery) was reused for different "
              "plaintext: " +
              std::to_string(same_keystream) + " of " +
              std::to_string(differing) +
              " differing byte(s) encrypted with the same keystream");
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

BOOST_AUTO_TEST_CASE(StorageQueryingLeavesItUnchanged) {
  run_property([](hegel::TestCase &tc) {
    run_storage_property(tc, checked_properties::query_read_only);
  });
}

// opening an encrypted storage for queries with a configuration that cannot
// decrypt it (the keyring lacks the KEK the binlog metadata names, or there
// is no keyring) fails with an error naming the keyring / KEK / encryption,
// instead of skipping every encrypted binlog (ticket 35)
BOOST_AUTO_TEST_CASE(StorageQueryingReportsKeyringErrors) {
  run_property([](hegel::TestCase &tc) {
    run_storage_property(tc, checked_properties::query_keyring_error);
  });
}

// a binlog closed while an incomplete transaction is still buffered (the
// collector does this when a reconnect's artificial ROTATE names another
// binlog) leaves no trace of that transaction: neither its bytes nor its
// timestamps or sequence number end up in any binlog or record
BOOST_AUTO_TEST_CASE(StorageClosingMidTransactionDropsIt) {
  run_property([](hegel::TestCase &tc) {
    run_storage_property(tc, checked_properties::close_mid_transaction);
  });
}

BOOST_AUTO_TEST_CASE(StorageConcurrentQueriesSucceed) {
  first_concurrent_query_error.clear();
  run_property([](hegel::TestCase &tc) {
    run_storage_property(tc, checked_properties::concurrent_queries);
  });
  if (!first_concurrent_query_error.empty()) {
    BOOST_FAIL("a query next to the writer failed: " +
               first_concurrent_query_error);
  }
}

// a writer that is killed, or hits a storage error, at any backend call -
// while writing, checkpointing, rotating, restarting or purging (stopped,
// as the README asks) - leaves a storage that the next start opens and
// resumes: every binlog but the last complete, the last one up to a
// transaction boundary, no data written twice, records matching the files
BOOST_AUTO_TEST_CASE(StorageSurvivesAFaultAtAnyBackendCall) {
  run_property([](hegel::TestCase &tc) {
    run_storage_property(tc, checked_properties::injected_fault);
  });
}

// a writer stopped cleanly and purged as the README asks (stop ->
// purge_binlogs -> restart), with a kill or a storage error injected only
// during that purge and restart, leaves a storage that opens again and holds
// either the binlogs from before the purge or those after it (COVERAGE_GAPS
// row 5 / lead N3: storage_core.cpp purge_binlogs commits the index first,
// so a fault in the removals leaves files the next start refuses)
BOOST_AUTO_TEST_CASE(StoragePurgeSurvivesAFault) {
  run_property([](hegel::TestCase &tc) {
    run_storage_property(tc, checked_properties::purge_fault);
  });
}

// the start after a kill or a storage error at any backend call never drops
// a binlog that binlog.index lists, nor data its metadata records, although
// it may remove leftovers of the interrupted operation and truncate data the
// metadata does not cover (the startup recovery of PBS-59)
BOOST_AUTO_TEST_CASE(StorageRestartKeepsRecordedData) {
  run_property([](hegel::TestCase &tc) {
    run_storage_property(tc, checked_properties::recorded_data_kept);
  });
}

// a second kill or storage error, during the start that recovers from the
// first one, still leaves a storage that the next start opens with every
// binlog and all the data recorded before the first fault
BOOST_AUTO_TEST_CASE(StorageRestartSurvivesAFault) {
  run_property([](hegel::TestCase &tc) {
    run_storage_property(tc, checked_properties::restart_fault);
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

BOOST_AUTO_TEST_CASE(StorageConcurrentReadBackMatchesDiskOnS3) {
  run_s3_storage_property(checked_properties::concurrent_read_back);
}

BOOST_AUTO_TEST_CASE(StorageOpeningOfDamagedFilesFailsCleanlyOnS3) {
  run_s3_storage_property(checked_properties::damaged_files);
}

BOOST_AUTO_TEST_CASE(StorageQueryingLeavesItUnchangedOnS3) {
  run_s3_storage_property(checked_properties::query_read_only);
}

BOOST_AUTO_TEST_CASE(StorageSurvivesAFaultAtAnyBackendCallOnS3) {
  run_s3_storage_property(checked_properties::injected_fault);
}

BOOST_AUTO_TEST_CASE(StoragePurgeSurvivesAFaultOnS3) {
  run_s3_storage_property(checked_properties::purge_fault);
}

BOOST_AUTO_TEST_CASE(StorageRestartKeepsRecordedDataOnS3) {
  run_s3_storage_property(checked_properties::recorded_data_kept);
}

BOOST_AUTO_TEST_CASE(StorageRestartSurvivesAFaultOnS3) {
  run_s3_storage_property(checked_properties::restart_fault);
}

// no offset of an encrypted binlog is encrypted twice with the same AES-CTR
// key, IV and counter for different plaintext, also when a crash left data
// that the next start truncates and writes over (catalog property
// ctr-keystream-not-reused-across-truncation)
BOOST_AUTO_TEST_CASE(StorageNeverReusesKeystreamAfterCrashRecovery) {
  run_property([](hegel::TestCase &tc) { run_keystream_property(tc); });
}
