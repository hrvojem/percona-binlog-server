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

// Property-based tests for the replication source handshake of the 'pull'
// mode listener (minimysql::network_service, PBS-90), built with Hegel
// (https://hegel.dev).
//
// Each test case starts the real listener in this process, as 'pull' does
// (an io_context running on its own thread, a streaming storage holding one
// binlog, a replication_source configuration with a generated server_id /
// server_uuid), on 127.0.0.1 and a free port. A minimal MySQL client written
// here connects over TCP, completes caching_sha2_password and sends a
// generated sequence of commands: statements (the handshake statements a
// replica sends, other known statements, and near-misses of them),
// COM_PING, COM_REGISTER_SLAVE and COM_BINLOG_DUMP (non-blocking). Every
// reply is checked against a simple model of the session:
// - SELECT @source_binlog_checksum / @master_binlog_checksum return the value
//   set by the last checksum SET of the session, NULL before any;
// - COM_BINLOG_DUMP is refused with ERR 1236 exactly when no checksum SET
//   came earlier in the session, and otherwise streams the stored binlog
//   followed by EOF;
// - statements are matched exactly or by the documented prefixes (the
//   variable name followed by ' ' or '=', or 'SET NAMES '): every known
//   statement gets OK or a resultset, and a near-miss (an extra suffix, a
//   prefix without its delimiter, a different letter case) gets an error;
// - SELECT @@GLOBAL.SERVER_ID / SERVER_UUID return the configured values and
//   SELECT @@GLOBAL.GTID_MODE is ON exactly for a storage in GTID mode;
// - a client disconnecting at any point is logged as "connection closed by
//   peer" at info level, never as an error.

#include <sys/socket.h>
#include <sys/time.h>

#include <netinet/in.h>

#include <arpa/inet.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/buffer.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <boost/system/system_error.hpp>

#include <boost/crc.hpp>

// needed for 'digest_storage'
#include <boost/container/static_vector.hpp> // IWYU pragma: keep
// needed for binsrv::gtids::gtid_set_storage
#include <boost/container/small_vector.hpp> // IWYU pragma: keep

#define BOOST_TEST_MODULE ReplicationSourcePropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "heartbeat_test_helpers.hpp"
#include "property_test_helpers.hpp"

// the protocol codec headers produce conversion warnings, as in
// connection_context.cpp
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"
#pragma GCC diagnostic ignored "-Wconversion"

#include "mysqlrouter/classic_protocol_codec_base.h"
#include "mysqlrouter/classic_protocol_codec_frame.h"
#include "mysqlrouter/classic_protocol_codec_message.h"
#include "mysqlrouter/classic_protocol_constants.h"
#include "mysqlrouter/classic_protocol_frame.h"
#include "mysqlrouter/classic_protocol_message.h"

#include "mysql/harness/stdx/flags.h"

#pragma GCC diagnostic pop

#include "binsrv/basic_logger.hpp"
#include "binsrv/log_severity.hpp"
#include "binsrv/main_config.hpp"
#include "binsrv/null_logger.hpp"
#include "binsrv/replication_source_config.hpp"
#include "binsrv/storage.hpp"

#include "binsrv/events/composite_binlog_name.hpp"
#include "binsrv/events/code_type.hpp"
#include "binsrv/events/common_header_flag_type.hpp"
#include "binsrv/events/event.hpp"
#include "binsrv/events/event_fwd.hpp"
#include "binsrv/events/generic_body.hpp"
#include "binsrv/events/common_header_view.hpp"
#include "binsrv/events/generic_post_header.hpp"
#include "binsrv/events/gtid_log_flag_type.hpp"
#include "binsrv/events/gtid_log_post_header.hpp"
#include "binsrv/events/protocol_traits_fwd.hpp"

#include "binsrv/gtids/gtid.hpp"
#include "binsrv/gtids/gtid_set.hpp"
#include "binsrv/gtids/tag.hpp"
#include "binsrv/gtids/uuid.hpp"

#include "minimysql/connection_context.hpp"
#include "minimysql/connection_context_fwd.hpp"
#include "minimysql/network_io_operations_fwd.hpp"
#include "minimysql/network_service.hpp"

#include "operations/event_generation_helpers.hpp"

#include "opensslpp/digest_context.hpp"
#include "opensslpp/encryption_padding.hpp"
#include "opensslpp/evp_pkey.hpp"
#include "opensslpp/digest_context_fwd.hpp"

#include "util/byte_span.hpp"
#include "util/ctime_timestamp.hpp"
#include "util/semantic_version.hpp"

namespace {

namespace gs = hegel::generators;
namespace cp = classic_protocol;

using property_testing::require;
using property_testing::run_property;

using minimysql::network_buffer_type;

constexpr std::string_view auth_method{"caching_sha2_password"};
constexpr std::string_view replica_user{"rpl"};
constexpr std::string_view replica_password{"rpl_password"};
constexpr std::size_t frame_header_size{4U};
constexpr std::uint32_t max_packet_payload{0xFFFFFFU};
constexpr std::uint16_t er_source_fatal_error_reading_binlog{1236U};

// ---------------------------------------------------------------------------
// the listener
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
           ("pbs_replication_source_property_" + std::to_string(ticks) + "_" +
            std::to_string(counter++));
  }
};

struct source_settings {
  bool gtid_mode;
  std::uint32_t server_id;
  std::string server_uuid;
  // replication_source.read_timeout, in seconds
  std::uint32_t read_timeout{60U};
};

std::ostream &operator<<(std::ostream &output, const source_settings &value) {
  return output << (value.gtid_mode ? "gtid" : "position")
                << " mode storage, server_id " << value.server_id
                << ", server_uuid " << value.server_uuid;
}

[[nodiscard]] gs::Generator<source_settings> source_settings_generator() {
  return gs::compose([](const hegel::TestCase &tc) {
    return source_settings{
        .gtid_mode = tc.draw(gs::booleans()),
        .server_id = tc.draw(gs::integers<std::uint32_t>()),
        // canonical 8-4-4-4-12 form, both letter cases
        .server_uuid = tc.draw(gs::from_regex(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
            "[0-9a-fA-F]{12}",
            true))};
  });
}

// a port that nothing listens on right now (another process may still take
// it before the listener binds it, which is retried)
[[nodiscard]] std::uint16_t pick_free_port() {
  boost::asio::io_context context;
  const boost::asio::ip::tcp::acceptor acceptor{
      context, boost::asio::ip::tcp::endpoint{
                   boost::asio::ip::make_address_v4("127.0.0.1"), 0U}};
  return acceptor.local_endpoint().port();
}

// an RSA key for the listener's full authentication, generated once per
// process (generating one takes tens of milliseconds)
[[nodiscard]] const std::string &test_rsa_private_key_pem() {
  static constexpr std::size_t key_bits{2048U};
  static const std::string pem{opensslpp::evp_pkey::export_private_pem(
      opensslpp::evp_pkey::generate(
          opensslpp::evp_pkey::get_rsa_algorithm_name(), key_bits))};
  return pem;
}

// a frame with a raw payload, for the caching_sha2_password exchange after the
// client greeting (single-byte requests and the encrypted password)
[[nodiscard]] network_buffer_type raw_client_frame(std::uint8_t sequence_number,
                                                   std::string_view payload) {
  network_buffer_type result;
  const auto size{std::size(payload)};
  result.push_back(static_cast<char>(size & 0xFFU));
  result.push_back(static_cast<char>((size >> 8U) & 0xFFU));
  result.push_back(static_cast<char>((size >> 16U) & 0xFFU));
  result.push_back(static_cast<char>(sequence_number));
  result.append(payload);
  return result;
}

// the replication source of 'pull': a streaming storage holding one binlog
// (a FORMAT_DESCRIPTION event, so that a dump has something to stream) and
// the listener serving it, with its io_context running on its own thread as
// in pull_operation.cpp
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

// the UUID of the transactions of a GTID-mode test storage
const binsrv::gtids::uuid stored_uuid{"5a5a5a5a-1111-2222-3333-444455556666"};

// a GTID-mode storage for COM_BINLOG_DUMP_GTID: binlogs holding the
// transactions stored_uuid:first_gno, :first_gno+1, ... in order (a GTID_LOG
// and an XID event each), 'transactions_per_binlog' of them in each binlog;
// the GTIDs below first_gno count as purged (the first binlog's
// PREVIOUS_GTIDS_LOG names them, no binlog holds them)
struct gtid_layout {
  binsrv::gtids::gno_t first_gno;
  std::vector<std::size_t> transactions_per_binlog;
};

std::ostream &operator<<(std::ostream &output, const gtid_layout &value) {
  output << "GTIDs from " << value.first_gno << " in binlogs of";
  for (const auto count : value.transactions_per_binlog) {
    output << " " << count;
  }
  return output << " transaction(s)";
}

void put_little_endian(std::vector<std::byte> &bytes, std::size_t offset,
                       std::uint64_t value, std::size_t size) {
  for (std::size_t index{0U}; index < size; ++index) {
    bytes[offset + index] =
        static_cast<std::byte>((value >> (8U * index)) & 0xFFU);
  }
}

// an XID event (common header, the 8-byte XID, a CRC32 footer), which ends a
// transaction
[[nodiscard]] std::vector<std::byte>
make_xid_event(std::uint64_t offset, std::uint32_t event_server_id,
               std::uint64_t xid) {
  using header = binsrv::events::common_header_view_base;
  static constexpr std::size_t xid_size{8U};
  static constexpr std::size_t footer_size{4U};
  static constexpr std::size_t size{header::size_in_bytes + xid_size +
                                    footer_size};
  std::vector<std::byte> result(size);
  put_little_endian(result, header::timestamp_offset, 1U, 4U);
  put_little_endian(result, header::type_code_offset,
                    static_cast<std::uint8_t>(binsrv::events::code_type::xid),
                    1U);
  put_little_endian(result, header::server_id_offset, event_server_id, 4U);
  put_little_endian(result, header::event_size_offset, size, 4U);
  put_little_endian(result, header::next_event_position_offset, offset + size,
                    4U);
  put_little_endian(result, header::flags_offset, 0U, 2U);
  put_little_endian(result, header::size_in_bytes, xid, xid_size);
  boost::crc_32_type crc;
  crc.process_bytes(std::data(result), size - footer_size);
  put_little_endian(result, size - footer_size, crc.checksum(), footer_size);
  return result;
}

// the GTID_LOG event of 'gtid', its transaction_length covering it and the
// XID event that follows
[[nodiscard]] std::vector<std::byte>
make_gtid_log_event(std::uint64_t offset, std::uint32_t event_server_id,
                    binsrv::gtids::gno_t gno, std::size_t xid_size) {
  const auto commit_time{std::chrono::high_resolution_clock::time_point{
      std::chrono::seconds{1'700'000'000}}};
  const binsrv::events::generic_post_header<binsrv::events::code_type::gtid_log>
      post_header{binsrv::events::gtid_log_flag_set{},
                  stored_uuid,
                  gno,
                  binsrv::events::gtid_log_post_header::known_logical_ts_code,
                  binsrv::events::seq_no_t{0ULL},
                  binsrv::events::seq_no_t{1ULL}};
  const auto encode{[&](std::uint64_t transaction_length) {
    binsrv::events::event_storage buffer;
    [[maybe_unused]] const auto event{
        binsrv::events::event::create_event<
            binsrv::events::code_type::gtid_log>(
            static_cast<std::uint32_t>(offset),
            util::ctime_timestamp{std::time_t{1}}, event_server_id,
            binsrv::events::common_header_flag_set{}, post_header,
            binsrv::events::generic_body<binsrv::events::code_type::gtid_log>{
                commit_time, commit_time, transaction_length,
                util::semantic_version{8U, 4U, 8U},
                util::semantic_version{8U, 4U, 8U}, std::nullopt},
            /*include_checksum=*/true, buffer)};
    return std::vector<std::byte>{std::cbegin(buffer), std::cend(buffer)};
  }};
  // the length is a packed integer, so its own size can change the event's
  const auto first{encode(xid_size)};
  auto result{encode(std::size(first) + xid_size)};
  if (std::size(result) != std::size(first)) {
    result = encode(std::size(result) + xid_size);
  }
  return result;
}

// the server_id of the stored binlog's FORMAT_DESCRIPTION, which the
// listener's heartbeats carry
constexpr std::uint32_t stored_fde_server_id{42U};

class replication_source {
public:
  // a storage holding one binlog with only its preamble, or, given a
  // 'layout' (with settings.gtid_mode), the GTID transactions it describes;
  // the listener logs to 'logger' if given (otherwise nowhere)
  explicit replication_source(const source_settings &settings,
                              const gtid_layout *layout = nullptr,
                              binsrv::basic_logger_ptr logger = {}) {
    if (logger != nullptr) {
      logger_ = std::move(logger);
    }
    std::filesystem::create_directories(root_.path() / "storage");
    const auto config_path{write_config(settings)};
    config_ = std::make_unique<binsrv::main_config>(config_path.string());
    storage_ = std::make_shared<binsrv::storage>(
        std::make_shared<binsrv::null_logger>(), *config_,
        binsrv::storage_construction_mode_type::streaming);
    if (layout == nullptr) {
      store_binlog();
    } else {
      store_gtid_binlogs(*layout);
    }

    source_config_.get<"server_id">() = settings.server_id;
    source_config_.get<"server_uuid">() = settings.server_uuid;
    source_config_.get<"read_timeout">() = settings.read_timeout;
    source_config_.get<"write_timeout">() = 60U;
    source_config_.get<"authentication">().get<"user">() = replica_user;
    source_config_.get<"authentication">().get<"password">() =
        replica_password;
    source_config_.get<"authentication">().get<"plugin">() = auth_method;
    // full caching_sha2_password authentication over an unencrypted
    // connection needs the listener's RSA key (PBS-33 / PBS-34): every login
    // takes the full path, as the listener has no authentication cache yet
    const auto key_path{root_.path() / "auth_rsa_private_key.pem"};
    {
      std::ofstream key_file{key_path};
      key_file << test_rsa_private_key_pem();
    }
    source_config_.get<"auth_rsa_private_key">() = key_path.string();

    static constexpr int max_bind_attempts{20};
    for (int attempt{1}; !service_.has_value(); ++attempt) {
      source_config_.get<"port">() = pick_free_port();
      source_config_.validate();
      try {
        service_.emplace(logger_, io_context_, storage_, source_config_);
      } catch (const boost::system::system_error &) {
        if (attempt == max_bind_attempts) {
          throw;
        }
      }
    }
    thread_ = std::jthread{[this] { io_context_.run(); }};
  }
  replication_source(const replication_source &) = delete;
  replication_source &operator=(const replication_source &) = delete;
  replication_source(replication_source &&) = delete;
  replication_source &operator=(replication_source &&) = delete;
  // as in pull_operation.cpp: the io_context is stopped, then the service is
  // destroyed before the io_context
  ~replication_source() {
    io_context_.stop();
    if (thread_.joinable()) {
      thread_.join();
    }
    service_.reset();
  }

  [[nodiscard]] std::uint16_t port() const {
    return source_config_.get<"port">();
  }

private:
  scratch_directory root_{};
  std::unique_ptr<binsrv::main_config> config_{};
  binsrv::storage_ptr storage_{};
  binsrv::replication_source_config source_config_{};
  // owned here, and declared before the io_context, as pull_operation.cpp
  // keeps its logger alive: destroying the io_context destroys the listener
  // coroutines, whose scope tracers still log on the way out
  binsrv::basic_logger_ptr logger_{std::make_shared<binsrv::null_logger>()};
  boost::asio::io_context io_context_{};
  std::optional<minimysql::network_service> service_{};
  std::jthread thread_{};

  // the configuration of the storage (the connection / replication sections
  // are required by the configuration schema, the replication source section
  // is not used: the listener gets 'source_config_')
  [[nodiscard]] std::filesystem::path
  write_config(const source_settings &settings) const {
    const auto config_path{root_.path() / "config.json"};
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
    "require_secure_transport": false,
    "authentication": {
      "user": "rpl", "password": "password", "plugin": "caching_sha2_password"
    }
  },
  "storage": { "backend": "file", "uri": "file://)"
           << (root_.path() / "storage").generic_string() << R"(" }
})";
    config.close();
    if (!config) {
      throw std::runtime_error{"cannot write the configuration file"};
    }
    return config_path;
  }

  void store_binlog() {
    static constexpr std::uint32_t fde_server_id{stored_fde_server_id};
    const binsrv::events::composite_binlog_name name{"binlog", 1U};
    require(storage_->open_binlog(name) == binsrv::open_binlog_status::created,
            "the binlog of the test storage was not created");
    binsrv::events::event_storage buffer;
    operations::generate_format_description_event_ex(
        buffer, util::semantic_version{8U, 4U, 8U},
        binsrv::events::magic_binlog_offset, fde_server_id,
        /*enable_checksum_algorithm=*/true, /*artificial=*/false);
    storage_->write_event(util::const_byte_span{buffer},
                          /*at_transaction_boundary=*/true,
                          binsrv::gtids::gtid{},
                          util::ctime_timestamp{std::time_t{1}}, 0ULL);
    const auto previous_gtids{make_previous_gtids_log(
        binsrv::events::magic_binlog_offset + std::size(buffer),
        fde_server_id)};
    storage_->write_event(util::const_byte_span{previous_gtids},
                          /*at_transaction_boundary=*/true,
                          binsrv::gtids::gtid{},
                          util::ctime_timestamp{std::time_t{1}}, 0ULL);
    storage_->flush_event_buffer();
  }

  void store_gtid_binlogs(const gtid_layout &layout) {
    static constexpr std::uint32_t fde_server_id{stored_fde_server_id};
    const auto at{[](std::uint64_t seconds) {
      return util::ctime_timestamp{static_cast<std::time_t>(seconds)};
    }};
    binsrv::gtids::gtid_set previous;
    for (binsrv::gtids::gno_t gno{1U}; gno < layout.first_gno; ++gno) {
      previous.add(stored_uuid, binsrv::gtids::tag{}, gno);
    }
    auto gno{layout.first_gno};
    std::uint64_t xid{1U};
    for (std::size_t index{0U};
         index < std::size(layout.transactions_per_binlog); ++index) {
      if (index != 0U) {
        storage_->close_binlog();
      }
      const binsrv::events::composite_binlog_name name{
          "binlog", static_cast<std::uint32_t>(index + 1U)};
      require(storage_->open_binlog(name) ==
                  binsrv::open_binlog_status::created,
              "a binlog of the test storage was not created");
      std::uint64_t position{binsrv::events::magic_binlog_offset};
      const auto store{[&](const std::vector<std::byte> &bytes,
                           bool at_boundary, const binsrv::gtids::gtid &gtid) {
        storage_->write_event(util::const_byte_span{bytes}, at_boundary, gtid,
                              at(1U), 0ULL);
        position += std::size(bytes);
      }};
      binsrv::events::event_storage buffer;
      operations::generate_format_description_event_ex(
          buffer, util::semantic_version{8U, 4U, 8U},
          binsrv::events::magic_binlog_offset, fde_server_id,
          /*enable_checksum_algorithm=*/true, /*artificial=*/false);
      store({std::cbegin(buffer), std::cend(buffer)}, false,
            binsrv::gtids::gtid{});
      store(make_previous_gtids_log(position, fde_server_id, previous), true,
            binsrv::gtids::gtid{});
      for (std::size_t count{0U};
           count < layout.transactions_per_binlog[index]; ++count, ++gno) {
        const binsrv::gtids::gtid gtid{stored_uuid, gno};
        const auto xid_size{std::size(make_xid_event(0U, fde_server_id, 0U))};
        store(make_gtid_log_event(position, fde_server_id, gno, xid_size),
              false, gtid);
        store(make_xid_event(position, fde_server_id, xid++), true, gtid);
        previous.add(gtid);
      }
      storage_->flush_event_buffer();
    }
  }
};

// ---------------------------------------------------------------------------
// the client
// ---------------------------------------------------------------------------

// what a MySQL client sends for caching_sha2_password (fast authentication):
// SHA256(password) XOR SHA256(SHA256(SHA256(password)) . scramble)
[[nodiscard]] std::string client_scramble(std::string_view password,
                                          std::string_view challenge) {
  const std::string digest{"SHA256"};
  const auto hashed{opensslpp::digest_context::calculate(
      digest, util::as_const_byte_span(password))};
  const auto double_hashed{
      opensslpp::digest_context::calculate(digest, hashed)};
  opensslpp::digest_context salted{digest};
  salted.update(double_hashed);
  salted.update(util::as_const_byte_span(challenge));
  opensslpp::digest_storage salted_hash(salted.get_digest_size_in_bytes());
  salted.finalize(salted_hash);
  std::string result(std::size(hashed), '\0');
  for (std::size_t index{0U}; index < std::size(hashed); ++index) {
    result[index] = static_cast<char>(
        std::to_integer<std::uint8_t>(hashed[index] ^ salted_hash[index]));
  }
  return result;
}

template <typename Message>
[[nodiscard]] network_buffer_type
encode_client_frame(std::uint8_t sequence_number, const Message &message,
                    const minimysql::capability_bitset &capabilities) {
  network_buffer_type result;
  const auto encode_result{cp::encode<cp::frame::Frame<Message>>(
      {sequence_number, message}, capabilities,
      boost::asio::dynamic_buffer(result))};
  if (!encode_result) {
    throw std::runtime_error{"the test client cannot encode a message"};
  }
  return result;
}

// the server's reply to one command, as the client sees it
enum class reply_kind : std::uint8_t {
  ok,
  error,
  resultset,
  // binlog events followed by EOF (the reply to a COM_BINLOG_DUMP)
  event_stream,
  // the server closed the connection without a reply
  closed,
  // anything else
  malformed
};

[[nodiscard]] std::string_view to_string(reply_kind kind) {
  switch (kind) {
  case reply_kind::ok:
    return "OK";
  case reply_kind::error:
    return "ERR";
  case reply_kind::resultset:
    return "resultset";
  case reply_kind::event_stream:
    return "event stream";
  case reply_kind::closed:
    return "connection closed";
  case reply_kind::malformed:
    return "malformed reply";
  }
  return "?";
}

struct reply {
  reply_kind kind{reply_kind::malformed};
  std::uint16_t error_code{0U};
  // the values of the first row of a resultset (std::nullopt for NULL)
  std::vector<std::optional<std::string>> first_row{};
  std::size_t rows{0U};
  std::size_t events{0U};
  std::string detail{};
};

std::ostream &operator<<(std::ostream &output, const reply &value) {
  output << to_string(value.kind);
  if (value.kind == reply_kind::error) {
    output << ' ' << value.error_code;
  }
  if (value.kind == reply_kind::resultset) {
    output << " (" << value.rows << " row(s)";
    if (!value.first_row.empty()) {
      output << ", first value "
             << (value.first_row.front().has_value()
                     ? "'" + *value.first_row.front() + "'"
                     : std::string{"NULL"});
    }
    output << ')';
  }
  if (value.kind == reply_kind::event_stream) {
    output << " (" << value.events << " event(s))";
  }
  if (!value.detail.empty()) {
    output << ": " << value.detail;
  }
  return output;
}

[[nodiscard]] std::uint64_t read_fixed_int(std::string_view bytes,
                                           std::size_t offset,
                                           std::size_t size) {
  std::uint64_t result{0ULL};
  for (std::size_t index{0U}; index < size; ++index) {
    result |= static_cast<std::uint64_t>(
                  static_cast<std::uint8_t>(bytes[offset + index]))
              << (8U * index);
  }
  return result;
}

// a length-encoded integer at 'offset' (advanced past it); std::nullopt for
// the NULL marker 0xFB
[[nodiscard]] std::optional<std::uint64_t>
read_lenenc_int(std::string_view bytes, std::size_t &offset) {
  require(offset < std::size(bytes), "a length-encoded integer is missing");
  const auto first{static_cast<std::uint8_t>(bytes[offset])};
  ++offset;
  std::size_t size{0U};
  switch (first) {
  case 0xFBU:
    return std::nullopt;
  case 0xFCU:
    size = 2U;
    break;
  case 0xFDU:
    size = 3U;
    break;
  case 0xFEU:
    size = 8U;
    break;
  default:
    return first;
  }
  require(offset + size <= std::size(bytes),
          "a length-encoded integer is cut short");
  const auto result{read_fixed_int(bytes, offset, size)};
  offset += size;
  return result;
}

[[nodiscard]] bool is_eof_packet(std::string_view payload) {
  static constexpr std::size_t max_eof_size{9U};
  return !payload.empty() && static_cast<std::uint8_t>(payload[0]) == 0xFEU &&
         std::size(payload) < max_eof_size;
}

// a blocking TCP connection to the listener speaking the client side of the
// MySQL protocol; every read gives up after a timeout, so that a listener
// that stops answering fails the property instead of hanging it
// how a login went: the path the listener took (none when it refused the
// client greeting outright) and whether it ended with OK
enum class auth_path : std::uint8_t { none, fast, full };

struct auth_outcome {
  auth_path path;
  bool ok;
  std::string detail;
};

class mysql_client {
public:
  explicit mysql_client(std::uint16_t port)
      : socket_{::socket(AF_INET, SOCK_STREAM, 0)} {
    if (socket_ < 0) {
      throw std::system_error{errno, std::generic_category(), "socket()"};
    }
    static constexpr int timeout_seconds{20};
    timeval timeout{};
    timeout.tv_sec = timeout_seconds;
    ::setsockopt(socket_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);
    ::setsockopt(socket_, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof timeout);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    if (::connect(socket_, reinterpret_cast<const sockaddr *>(&address),
                  sizeof address) != 0) {
      const int error{errno};
      ::close(socket_);
      throw std::system_error{error, std::generic_category(), "connect()"};
    }
  }
  mysql_client(const mysql_client &) = delete;
  mysql_client &operator=(const mysql_client &) = delete;
  mysql_client(mysql_client &&) = delete;
  mysql_client &operator=(mysql_client &&) = delete;
  ~mysql_client() {
    if (socket_ >= 0) {
      ::close(socket_);
    }
  }

  // closes the connection: an orderly close (FIN, the listener reads EOF) or
  // an abortive one (SO_LINGER 0: RST, the listener gets ECONNRESET)
  void disconnect(bool abortive) {
    if (abortive) {
      const linger no_linger{.l_onoff = 1, .l_linger = 0};
      ::setsockopt(socket_, SOL_SOCKET, SO_LINGER, &no_linger,
                   sizeof no_linger);
    }
    ::close(socket_);
    socket_ = -1;
  }

  [[nodiscard]] const minimysql::capability_bitset &
  capabilities() const noexcept {
    return capabilities_;
  }

  // reads one packet: its whole frame (header included), or std::nullopt if
  // the server closed the connection before it
  [[nodiscard]] std::optional<network_buffer_type> read_frame() {
    network_buffer_type frame(frame_header_size, '\0');
    if (!read_exactly(std::data(frame), frame_header_size)) {
      return std::nullopt;
    }
    const auto length{read_fixed_int(frame, 0U, 3U)};
    frame.resize(frame_header_size + length);
    require(read_exactly(std::next(std::data(frame),
                                   static_cast<std::ptrdiff_t>(frame_header_size)),
                         length),
            "the server closed the connection in the middle of a packet");
    return frame;
  }

  void write_frame(const network_buffer_type &frame) {
    std::size_t sent{0U};
    while (sent < std::size(frame)) {
      const auto result{::send(socket_, std::next(std::data(frame),
                                                  static_cast<std::ptrdiff_t>(sent)),
                               std::size(frame) - sent, MSG_NOSIGNAL)};
      if (result <= 0) {
        throw std::runtime_error{"cannot send a packet to the listener"};
      }
      sent += static_cast<std::size_t>(result);
    }
  }

  // server greeting, client greeting with the caching_sha2_password scramble,
  // then either the fast path (a user the listener has authenticated before:
  // "fast authentication succeeded" and OK) or the full authentication over
  // an unencrypted connection, as a MySQL client with --get-server-public-key
  // does it: "perform full authentication", a request for the server's RSA
  // public key, the password XOR the scramble encrypted with that key
  // (RSA-OAEP), and OK
  // how try_authenticate() damages the RSA-encrypted password of a full
  // authentication
  enum class password_garbling : std::uint8_t {
    none,
    // random bytes of the key's size: RSA-OAEP decryption fails
    random_bytes,
    // half of the encrypted password
    truncated,
    // a correct encryption of the password without its terminating NUL
    not_terminated
  };

  [[nodiscard]] auth_outcome
  try_authenticate(std::string_view user, std::string_view password,
                   password_garbling garbling = password_garbling::none) {
    const auto greeting{read_frame()};
    require(greeting.has_value(), "no server greeting");
    // the greeting can be decoded with any capabilities that include
    // protocol_41
    minimysql::capability_bitset decode_capabilities{};
    decode_capabilities.set(cp::capabilities::pos::protocol_41);
    auto buffer{boost::asio::buffer(*greeting)};
    const auto decoded{
        cp::decode<cp::frame::Frame<cp::message::server::Greeting>>(
            buffer, decode_capabilities)};
    require(decoded.has_value(), "the server greeting cannot be decoded");
    const auto &server_greeting{decoded.value().second.payload()};
    capabilities_ = server_greeting.capabilities();
    std::string challenge{server_greeting.auth_method_data()};
    if (!challenge.empty() && challenge.back() == '\0') {
      challenge.pop_back();
    }

    const cp::message::client::Greeting client_greeting{
        capabilities_,
        max_packet_payload,
        0U,
        std::string{user},
        client_scramble(password, challenge),
        "",
        std::string{auth_method},
        ""};
    write_frame(encode_client_frame(1U, client_greeting, capabilities_));

    // "fast authentication succeeded" (0x01 0x03) and OK for a user the
    // listener has authenticated before (PBS-10 part 3), otherwise "perform
    // full authentication" (0x01 0x04)
    const auto full_auth{read_frame()};
    if (!full_auth.has_value()) {
      return {.path = auth_path::none, .ok = false,
              .detail = "the connection closed after the client greeting"};
    }
    if (std::size(*full_auth) > frame_header_size &&
        (*full_auth)[frame_header_size] == '\xFF') {
      // refused without a second exchange (a cached user with a wrong
      // scramble)
      return {.path = auth_path::none, .ok = false, .detail = "ERR"};
    }
    require(std::size(*full_auth) == frame_header_size + 2U &&
                (*full_auth)[frame_header_size] == '\x01' &&
                ((*full_auth)[frame_header_size + 1U] == '\x03' ||
                 (*full_auth)[frame_header_size + 1U] == '\x04'),
            "neither ERR, 'fast authentication succeeded' nor 'perform full "
            "authentication' after the client greeting");
    if ((*full_auth)[frame_header_size + 1U] == '\x03') {
      const auto fast_ok{read_frame()};
      return {.path = auth_path::fast,
              .ok = fast_ok.has_value() &&
                    std::size(*fast_ok) > frame_header_size &&
                    (*fast_ok)[frame_header_size] == '\0',
              .detail = "after 'fast authentication succeeded'"};
    }
    // request the public key (0x02); the reply is 0x01 followed by its PEM
    write_frame(raw_client_frame(3U, std::string_view{"\x02", 1U}));
    const auto public_key{read_frame()};
    require(public_key.has_value() &&
                std::size(*public_key) > frame_header_size + 1U &&
                (*public_key)[frame_header_size] == '\x01',
            "no public key after the public key request");
    const auto key{opensslpp::evp_pkey::import_public_pem(
        std::string_view{*public_key}.substr(frame_header_size + 1U))};
    // the null-terminated password, XOR the scramble (repeated)
    std::string plain{password};
    if (garbling != password_garbling::not_terminated) {
      plain.push_back('\0');
    }
    for (std::size_t index{0U}; index < std::size(plain); ++index) {
      plain[index] = static_cast<char>(
          plain[index] ^ challenge[index % std::size(challenge)]);
    }
    auto encrypted{opensslpp::evp_key_encrypt(
        util::as_const_byte_span(plain), key,
        opensslpp::encryption_padding::pkcs1_oaep)};
    if (garbling == password_garbling::random_bytes) {
      for (std::size_t index{0U}; index < std::size(encrypted); ++index) {
        encrypted[index] = static_cast<std::byte>((index * 37U + 11U) & 0xFFU);
      }
    } else if (garbling == password_garbling::truncated) {
      encrypted.resize(std::size(encrypted) / 2U);
    }
    write_frame(raw_client_frame(
        5U, std::string_view{reinterpret_cast<const char *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                                 std::data(encrypted)),
                             std::size(encrypted)}));
    const auto ok{read_frame()};
    return {.path = auth_path::full,
            .ok = ok.has_value() && std::size(*ok) > frame_header_size &&
                  (*ok)[frame_header_size] == '\0',
            .detail = "after the encrypted password"};
  }

  void authenticate(std::string_view user, std::string_view password) {
    const auto outcome{try_authenticate(user, password)};
    require(outcome.ok, "authentication failed: " + outcome.detail);
  }

  // reads the reply to a statement, COM_PING or COM_REGISTER_SLAVE
  [[nodiscard]] reply read_reply() {
    reply result{};
    const auto first{read_frame()};
    if (!first.has_value()) {
      result.kind = reply_kind::closed;
      return result;
    }
    const std::string_view payload{
        std::string_view{*first}.substr(frame_header_size)};
    if (payload.empty()) {
      result.detail = "an empty packet";
      return result;
    }
    const auto header{static_cast<std::uint8_t>(payload[0])};
    if (header == 0x00U) {
      result.kind = reply_kind::ok;
      return result;
    }
    if (header == 0xFFU) {
      return error_reply(payload);
    }
    if (is_eof_packet(payload)) {
      result.detail = "an EOF packet";
      return result;
    }
    // a text resultset: column count, column definitions, EOF (the listener
    // does not offer CLIENT_DEPRECATE_EOF), rows, EOF
    std::size_t offset{0U};
    const auto columns{read_lenenc_int(payload, offset)};
    require(columns.has_value() && *columns > 0U,
            "a resultset with a bad column count");
    for (std::uint64_t column{0U}; column < *columns; ++column) {
      require(read_frame().has_value(),
              "the connection closed inside the column definitions");
    }
    const auto separator{read_frame()};
    require(separator.has_value() &&
                is_eof_packet(std::string_view{*separator}.substr(
                    frame_header_size)),
            "no EOF after the column definitions");
    result.kind = reply_kind::resultset;
    while (true) {
      const auto row{read_frame()};
      require(row.has_value(), "the connection closed inside the rows");
      const std::string_view row_payload{
          std::string_view{*row}.substr(frame_header_size)};
      if (is_eof_packet(row_payload)) {
        break;
      }
      if (result.rows == 0U) {
        std::size_t row_offset{0U};
        for (std::uint64_t column{0U}; column < *columns; ++column) {
          const auto length{read_lenenc_int(row_payload, row_offset)};
          if (!length.has_value()) {
            result.first_row.emplace_back(std::nullopt);
            continue;
          }
          require(row_offset + *length <= std::size(row_payload),
                  "a row value is cut short");
          result.first_row.emplace_back(
              std::string{row_payload.substr(row_offset, *length)});
          row_offset += *length;
        }
      }
      ++result.rows;
    }
    return result;
  }

  // reads the reply to a COM_BINLOG_DUMP: ERR, or binlog events (each an OK
  // byte and the event) up to EOF
  [[nodiscard]] reply read_dump_reply() {
    reply result{};
    while (true) {
      const auto frame{read_frame()};
      if (!frame.has_value()) {
        if (result.events == 0U) {
          result.kind = reply_kind::closed;
        } else {
          result.detail = "the connection closed after " +
                          std::to_string(result.events) +
                          " event(s) without EOF";
        }
        return result;
      }
      const std::string_view payload{
          std::string_view{*frame}.substr(frame_header_size)};
      if (payload.empty()) {
        result.detail = "an empty packet";
        return result;
      }
      const auto header{static_cast<std::uint8_t>(payload[0])};
      if (header == 0xFFU) {
        if (result.events != 0U) {
          result.detail = "an error after " + std::to_string(result.events) +
                          " event(s)";
          return result;
        }
        return error_reply(payload);
      }
      if (is_eof_packet(payload)) {
        result.kind = reply_kind::event_stream;
        return result;
      }
      if (header != 0x00U) {
        result.detail = "a packet that is neither an event nor EOF";
        return result;
      }
      ++result.events;
    }
  }

  // whether a packet (or the end of the connection) arrives within
  // 'timeout'
  [[nodiscard]] bool wait_readable(std::chrono::milliseconds timeout) {
    pollfd descriptor{.fd = socket_, .events = POLLIN, .revents = 0};
    return ::poll(&descriptor, 1U, static_cast<int>(timeout.count())) > 0;
  }

  // true when the server has closed the connection (nothing more arrives)
  [[nodiscard]] bool closed_by_server() {
    char byte{};
    return ::recv(socket_, &byte, 1U, 0) == 0;
  }

private:
  int socket_;
  minimysql::capability_bitset capabilities_{};

  [[nodiscard]] bool read_exactly(char *destination, std::size_t size) {
    std::size_t received{0U};
    while (received < size) {
      const auto result{::recv(socket_, std::next(destination,
                                                  static_cast<std::ptrdiff_t>(received)),
                               size - received, 0)};
      if (result == 0) {
        require(received == 0U,
                "the server closed the connection in the middle of a read");
        return false;
      }
      if (result < 0) {
        throw std::runtime_error{"no reply from the listener (" +
                                 std::string{std::strerror(errno)} + ")"};
      }
      received += static_cast<std::size_t>(result);
    }
    return true;
  }

  [[nodiscard]] static reply error_reply(std::string_view payload) {
    reply result{};
    result.kind = reply_kind::error;
    static constexpr std::size_t min_error_size{3U};
    if (std::size(payload) >= min_error_size) {
      result.error_code =
          static_cast<std::uint16_t>(read_fixed_int(payload, 1U, 2U));
    }
    return result;
  }
};

// ---------------------------------------------------------------------------
// statements and the model of how they are matched
// ---------------------------------------------------------------------------

// the checksum statements and the value each one leaves in
// @source_binlog_checksum (PBS always streams CRC32 events, so the
// '@@global.binlog_checksum' forms mean CRC32)
const std::vector<std::pair<std::string, std::string>> checksum_sets{
    {"SET @source_binlog_checksum = 'NONE', @master_binlog_checksum = 'NONE'",
     "NONE"},
    {"SET @master_binlog_checksum = 'NONE', @source_binlog_checksum = 'NONE'",
     "NONE"},
    {"SET @source_binlog_checksum = 'CRC32', @master_binlog_checksum = "
     "'CRC32'",
     "CRC32"},
    {"SET @master_binlog_checksum = 'CRC32', @source_binlog_checksum = "
     "'CRC32'",
     "CRC32"},
    {"SET @master_binlog_checksum = @@global.binlog_checksum, "
     "@source_binlog_checksum = @@global.binlog_checksum",
     "CRC32"},
    {"SET @source_binlog_checksum = @@global.binlog_checksum, "
     "@master_binlog_checksum = @@global.binlog_checksum",
     "CRC32"}};

constexpr std::string_view select_source_checksum{
    "SELECT @source_binlog_checksum"};
constexpr std::string_view select_master_checksum{
    "SELECT @master_binlog_checksum"};
constexpr std::string_view select_server_id{"SELECT @@GLOBAL.SERVER_ID"};
constexpr std::string_view select_server_uuid{"SELECT @@GLOBAL.SERVER_UUID"};
constexpr std::string_view select_gtid_mode{"SELECT @@GLOBAL.GTID_MODE"};

// every statement the listener knows exactly
[[nodiscard]] std::vector<std::string> exact_statements() {
  std::vector<std::string> result{"select * from tbl",
                                  "select @@version_comment limit 1",
                                  "SELECT VERSION()",
                                  "SELECT UNIX_TIMESTAMP()",
                                  std::string{select_server_id},
                                  std::string{select_server_uuid},
                                  std::string{select_source_checksum},
                                  std::string{select_master_checksum},
                                  std::string{select_gtid_mode}};
  for (const auto &[statement, value] : checksum_sets) {
    result.push_back(statement);
  }
  return result;
}

// the statements matched by prefix: a user variable assignment, matched by
// the variable name followed by ' ' or '=' (a longer name is another
// variable), and SET NAMES followed by a space
const std::vector<std::string> variable_prefixes{
    "SET @source_heartbeat_period", "SET @master_heartbeat_period",
    "SET @slave_uuid", "SET @replica_uuid"};
constexpr std::string_view set_names_prefix{"SET NAMES "};

enum class expected_reply : std::uint8_t { ok, resultset, error };

// the documented matching rules, written independently of the listener
[[nodiscard]] expected_reply classify(std::string_view statement) {
  for (const auto &known : exact_statements()) {
    if (statement == known) {
      return known.starts_with("SET ") ? expected_reply::ok
                                       : expected_reply::resultset;
    }
  }
  if (statement.starts_with(set_names_prefix)) {
    return expected_reply::ok;
  }
  for (const auto &prefix : variable_prefixes) {
    if (statement.starts_with(prefix) && std::size(statement) > std::size(prefix) &&
        (statement[std::size(prefix)] == ' ' ||
         statement[std::size(prefix)] == '=')) {
      return expected_reply::ok;
    }
  }
  return expected_reply::error;
}

[[nodiscard]] std::optional<std::string>
checksum_set_value(std::string_view statement) {
  for (const auto &[known, value] : checksum_sets) {
    if (statement == known) {
      return value;
    }
  }
  return std::nullopt;
}

[[nodiscard]] gs::Generator<std::string> checksum_statements() {
  std::vector<std::string> statements{std::string{select_source_checksum},
                                      std::string{select_master_checksum}};
  for (const auto &[statement, value] : checksum_sets) {
    statements.push_back(statement);
  }
  return gs::sampled_from(statements);
}

// statements matched by prefix, with the payloads a replica sends
[[nodiscard]] gs::Generator<std::string> prefixed_statements() {
  return gs::compose([](const hegel::TestCase &tc) {
    const auto form{tc.draw(gs::integers<int>({.min_value = 0, .max_value = 4}))};
    switch (form) {
    case 0:
    case 1:
      return std::string{form == 0 ? "SET @source_heartbeat_period"
                                   : "SET @master_heartbeat_period"} +
             tc.draw(gs::sampled_from(std::vector<std::string>{" = ", "=",
                                                               " ="})) +
             std::to_string(tc.draw(gs::integers<std::uint64_t>()));
    case 2:
    case 3:
      return std::string{form == 2 ? "SET @slave_uuid" : "SET @replica_uuid"} +
             " = '" +
             tc.draw(gs::from_regex(
                 "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                 true)) +
             "'";
    default:
      return std::string{set_names_prefix} +
             tc.draw(gs::sampled_from(std::vector<std::string>{
                 "utf8mb4", "latin1", "binary", "utf8mb4 COLLATE utf8mb4_bin"}));
    }
  });
}

// near-misses of the known statements: an exact statement with something
// appended, a prefix without its delimiter (or followed by a longer
// variable name), and a known statement with a letter in the other case
[[nodiscard]] gs::Generator<std::string> near_miss_statements() {
  return gs::compose([](const hegel::TestCase &tc) {
    const auto exact{exact_statements()};
    const auto form{tc.draw(gs::integers<int>({.min_value = 0, .max_value = 2}))};
    if (form == 0) {
      return tc.draw(gs::sampled_from(exact)) +
             tc.draw(gs::sampled_from(std::vector<std::string>{
                 " ", ";", "x", "1", ",", "\n", " LIMIT 1", " "}));
    }
    if (form == 1) {
      const auto prefix{tc.draw(gs::sampled_from(std::vector<std::string>{
          variable_prefixes[0], variable_prefixes[1], variable_prefixes[2],
          variable_prefixes[3], "SET NAMES"}))};
      return prefix + tc.draw(gs::sampled_from(std::vector<std::string>{
                          "", "x = 1", "_x = 1", "s = 1", "0", "\tutf8mb4",
                          "utf8mb4"}));
    }
    // flip the case of one letter of a known or prefixed statement
    auto statement{tc.draw(gs::one_of(
        {gs::sampled_from(exact), prefixed_statements()}))};
    std::vector<std::size_t> letters;
    for (std::size_t index{0U}; index < std::size(statement); ++index) {
      if (std::isalpha(static_cast<unsigned char>(statement[index])) != 0) {
        letters.push_back(index);
      }
    }
    const auto chosen{letters[tc.draw(gs::integers<std::size_t>(
        {.min_value = 0U, .max_value = std::size(letters) - 1U}))]};
    auto &letter{statement[chosen]};
    const auto byte{static_cast<unsigned char>(letter)};
    letter = static_cast<char>(std::isupper(byte) != 0 ? std::tolower(byte)
                                                       : std::toupper(byte));
    return statement;
  });
}

// ---------------------------------------------------------------------------
// commands and sessions
// ---------------------------------------------------------------------------

enum class command_kind : std::uint8_t {
  query,
  ping,
  register_replica,
  binlog_dump,
  reset_connection,
  change_user
};

struct command {
  command_kind kind;
  std::string statement;
  // COM_BINLOG_DUMP only: the binlog and the position the client asks for
  // (an empty name: the beginning of the storage)
  std::string dump_binlog{};
  std::uint64_t dump_position{binsrv::events::magic_binlog_offset};
  // a blocking dump waits at the end of the data (and gets heartbeats)
  // instead of ending with EOF
  bool dump_blocking{false};
};

std::ostream &operator<<(std::ostream &output, const command &value) {
  switch (value.kind) {
  case command_kind::query:
    return output << "query '" << value.statement << "'";
  case command_kind::ping:
    return output << "COM_PING";
  case command_kind::register_replica:
    return output << "COM_REGISTER_SLAVE";
  case command_kind::reset_connection:
    return output << "COM_RESET_CONNECTION";
  case command_kind::change_user:
    return output << "COM_CHANGE_USER";
  case command_kind::binlog_dump:
    output << "COM_BINLOG_DUMP";
    if (!value.dump_binlog.empty() ||
        value.dump_position != binsrv::events::magic_binlog_offset) {
      output << " '" << value.dump_binlog << "':" << value.dump_position;
    }
    return output;
  }
  return output;
}

[[nodiscard]] gs::Generator<command>
commands(gs::Generator<std::string> statements) {
  return gs::compose([statements](const hegel::TestCase &tc) {
    const auto kind{tc.draw(gs::sampled_from(
        {command_kind::query, command_kind::query, command_kind::query,
         command_kind::query, command_kind::ping,
         command_kind::register_replica, command_kind::reset_connection,
         command_kind::change_user, command_kind::binlog_dump}))};
    command result{.kind = kind, .statement = {}};
    if (kind == command_kind::query) {
      result.statement = tc.draw(statements);
    }
    return result;
  });
}

[[nodiscard]] network_buffer_type
encode_command(const command &value,
               const minimysql::capability_bitset &capabilities) {
  switch (value.kind) {
  case command_kind::query:
    return encode_client_frame(
        0U, cp::message::client::Query{value.statement}, capabilities);
  case command_kind::ping:
    return encode_client_frame(0U, cp::message::client::Ping{}, capabilities);
  case command_kind::register_replica: {
    static constexpr std::uint32_t replica_server_id{2U};
    static constexpr std::uint16_t replica_port{3306U};
    return encode_client_frame(
        0U,
        cp::message::client::RegisterReplica{replica_server_id, "replica",
                                             "", "", replica_port, 0U, 0U},
        capabilities);
  }
  case command_kind::reset_connection:
    return encode_client_frame(0U, cp::message::client::ResetConnection{},
                               capabilities);
  case command_kind::change_user: {
    // the same user again, with a made-up scramble
    static constexpr std::uint16_t utf8mb4_collation{255U};
    return encode_client_frame(
        0U,
        cp::message::client::ChangeUser{
            std::string{replica_user}, std::string(32U, 'x'), "",
            utf8mb4_collation, std::string{auth_method}, ""},
        capabilities);
  }
  case command_kind::binlog_dump: {
    // non-blocking unless asked otherwise: the stream ends with EOF once
    // the stored binlog has been sent
    using dump = cp::message::client::BinlogDump;
    stdx::flags<dump::Flags> flags{};
    if (!value.dump_blocking) {
      flags |= dump::Flags::non_blocking;
    }
    static constexpr std::uint32_t replica_server_id{2U};
    return encode_client_frame(
        0U,
        dump{flags, replica_server_id, value.dump_binlog,
             static_cast<std::uint32_t>(value.dump_position)},
        capabilities);
  }
  }
  throw std::logic_error{"unknown command kind"};
}

struct exchange {
  command sent;
  reply received;
};

std::string describe(const std::vector<exchange> &transcript,
                     std::size_t failing_index) {
  std::ostringstream output;
  for (std::size_t index{0U}; index <= failing_index; ++index) {
    output << "\n  " << (index + 1U) << ". " << transcript[index].sent
           << " -> " << transcript[index].received;
  }
  return output.str();
}

// whether 'received' resets the session state as MySQL does: COM_RESET_CONNECTION
// answered with OK clears the user variables (the checksum declaration
// among them); refused with ERR, it changes nothing
[[nodiscard]] bool resets_session(const command &sent, const reply &received) {
  return sent.kind == command_kind::reset_connection &&
         received.kind == reply_kind::ok;
}

// connects to a new listener, authenticates and sends the commands up to and
// including the first COM_BINLOG_DUMP (the session ends with it), or up to a
// COM_CHANGE_USER that is not refused (the model does not follow the
// re-authentication it starts)
[[nodiscard]] std::vector<exchange> run_session(const source_settings &settings,
                                                const std::vector<command> &cmds) {
  const replication_source source{settings};
  mysql_client client{source.port()};
  client.authenticate(replica_user, replica_password);
  std::vector<exchange> transcript;
  for (const auto &sent : cmds) {
    client.write_frame(encode_command(sent, client.capabilities()));
    if (sent.kind == command_kind::binlog_dump) {
      auto received{client.read_dump_reply()};
      if (received.kind == reply_kind::event_stream ||
          received.kind == reply_kind::error) {
        // either way the listener ends the session
        if (!client.closed_by_server()) {
          received.kind = reply_kind::malformed;
          received.detail = "the session went on after the dump";
        }
      }
      transcript.push_back({.sent = sent, .received = received});
      break;
    }
    transcript.push_back({.sent = sent, .received = client.read_reply()});
    if (sent.kind == command_kind::change_user &&
        transcript.back().received.kind != reply_kind::error) {
      break;
    }
  }
  return transcript;
}

// the reply every command must get in any case: OK for COM_PING and
// COM_REGISTER_SLAVE, and the reply class of the statement's matching rule
void require_reply_class(const std::vector<exchange> &transcript,
                         std::size_t index) {
  const auto &[sent, received] = transcript[index];
  if (sent.kind == command_kind::ping ||
      sent.kind == command_kind::register_replica) {
    require(received.kind == reply_kind::ok,
            "not answered with OK:" + describe(transcript, index));
  }
  // MySQL answers COM_RESET_CONNECTION with OK; refusing it with ERR (as an
  // unsupported command) is acceptable, ending the session is not
  if (sent.kind == command_kind::reset_connection) {
    require(received.kind == reply_kind::ok ||
                received.kind == reply_kind::error,
            "neither OK nor ERR:" + describe(transcript, index));
  }
  // COM_CHANGE_USER starts a re-authentication in MySQL; refused with ERR or
  // not, the session must not just end
  if (sent.kind == command_kind::change_user) {
    require(received.kind != reply_kind::closed,
            "the session ended:" + describe(transcript, index));
  }
  if (sent.kind == command_kind::query) {
    switch (classify(sent.statement)) {
    case expected_reply::ok:
      require(received.kind == reply_kind::ok,
              "a known statement was not answered with OK:" +
                  describe(transcript, index));
      break;
    case expected_reply::resultset:
      require(received.kind == reply_kind::resultset && received.rows >= 1U &&
                  !received.first_row.empty(),
              "a known statement was not answered with a resultset:" +
                  describe(transcript, index));
      break;
    case expected_reply::error:
      require(received.kind == reply_kind::error,
              "a statement that is neither known nor documented by prefix "
              "was not answered with an error:" +
                  describe(transcript, index));
      break;
    }
  }
}

} // anonymous namespace

// SELECT @source_binlog_checksum / @master_binlog_checksum always return the
// value set by the last checksum SET of the session, NULL before any
BOOST_AUTO_TEST_CASE(ReplicationSourceChecksumVariableFollowsLastSet) {
  run_property([](hegel::TestCase &tc) {
    const auto settings{tc.draw("settings", source_settings_generator())};
    const auto cmds{tc.draw(
        "commands",
        gs::vectors(commands(gs::one_of(
                        {checksum_statements(), checksum_statements(),
                         prefixed_statements(), near_miss_statements()})),
                    {.max_size = 20U}))};
    const auto transcript{run_session(settings, cmds)};
    std::optional<std::string> model{};
    for (std::size_t index{0U}; index < std::size(transcript); ++index) {
      const auto &[sent, received] = transcript[index];
      if (resets_session(sent, received)) {
        model.reset();
      }
      if (sent.kind != command_kind::query) {
        continue;
      }
      if (sent.statement == select_source_checksum ||
          sent.statement == select_master_checksum) {
        require(received.kind == reply_kind::resultset &&
                    received.rows == 1U &&
                    std::size(received.first_row) == 1U &&
                    received.first_row.front() == model,
                "expected " + (model.has_value() ? "'" + *model + "'"
                                                 : std::string{"NULL"}) +
                    ":" + describe(transcript, index));
      }
      if (const auto value{checksum_set_value(sent.statement)};
          value.has_value()) {
        require(received.kind == reply_kind::ok,
                "a checksum SET was not answered with OK:" +
                    describe(transcript, index));
        model = value;
      }
    }
  });
}

// COM_BINLOG_DUMP is refused with ERR 1236 exactly when no checksum SET came
// earlier in the session, and otherwise streams the stored binlog up to EOF;
// either way the session ends
BOOST_AUTO_TEST_CASE(ReplicationSourceDumpRequiresChecksumDeclaration) {
  run_property([](hegel::TestCase &tc) {
    const auto settings{tc.draw("settings", source_settings_generator())};
    auto cmds{tc.draw(
        "commands",
        gs::vectors(commands(gs::one_of(
                        {checksum_statements(), prefixed_statements(),
                         near_miss_statements()})),
                    {.max_size = 12U}))};
    // every session ends with a dump
    cmds.push_back({.kind = command_kind::binlog_dump, .statement = {}});
    const auto transcript{run_session(settings, cmds)};
    bool declared{false};
    for (std::size_t index{0U}; index < std::size(transcript); ++index) {
      const auto &[sent, received] = transcript[index];
      if (sent.kind == command_kind::binlog_dump) {
        require(index + 1U == std::size(transcript),
                "the transcript goes on after a dump");
        if (declared) {
          require(received.kind == reply_kind::event_stream &&
                      received.events >= 1U,
                  "the dump did not stream the binlog although the checksum "
                  "was declared:" +
                      describe(transcript, index));
        } else {
          require(received.kind == reply_kind::error &&
                      received.error_code ==
                          er_source_fatal_error_reading_binlog,
                  "the dump was not refused with ERR 1236 although no "
                  "checksum was declared:" +
                      describe(transcript, index));
        }
        return;
      }
      if (sent.kind == command_kind::query &&
          checksum_set_value(sent.statement).has_value()) {
        declared = true;
      }
      if (resets_session(sent, received)) {
        declared = false;
      }
    }
    // a COM_CHANGE_USER that was not refused ends the transcript early
    require(!transcript.empty() &&
                transcript.back().sent.kind == command_kind::change_user,
            "the session ended before the dump");
  });
}

// a COM_BINLOG_DUMP from any position - also one that is not an event
// boundary, or past the end of the binlog - ends that session only: the
// listener keeps serving new connections (ticket 39: a position inside an
// event made a noexcept indexed_event_block accessor throw, which ended the
// whole process)
BOOST_AUTO_TEST_CASE(ReplicationSourceSurvivesAnyDumpPosition) {
  run_property([](hegel::TestCase &tc) {
    const auto settings{tc.draw("settings", source_settings_generator())};
    // the stored binlog (FORMAT_DESCRIPTION and PREVIOUS_GTIDS_LOG) is
    // shorter than this, so positions past its end are drawn too
    static constexpr std::uint64_t max_position{512U};
    const command dump{
        .kind = command_kind::binlog_dump,
        .statement = {},
        .dump_binlog = tc.draw(
            "binlog", gs::sampled_from(std::vector<std::string>{
                          "", "binlog.000001"})),
        .dump_position = tc.draw(
            "position", gs::integers<std::uint64_t>(
                            {.min_value = 0U, .max_value = max_position}))};

    const replication_source source{settings};
    {
      mysql_client client{source.port()};
      client.authenticate(replica_user, replica_password);
      client.write_frame(encode_command(
          {.kind = command_kind::query,
           .statement = "SET @source_binlog_checksum = 'NONE', "
                        "@master_binlog_checksum = 'NONE'"},
          client.capabilities()));
      require(client.read_reply().kind == reply_kind::ok,
              "the checksum SET was not answered with OK");
      client.write_frame(encode_command(dump, client.capabilities()));
      // ERR, an event stream or a closed connection: any of them ends the
      // session; only the listener's survival is checked here
      static_cast<void>(client.read_dump_reply());
    }

    std::ostringstream label;
    label << dump;
    mysql_client next{source.port()};
    next.authenticate(replica_user, replica_password);
    next.write_frame(encode_command({.kind = command_kind::ping, .statement = {}},
                                    next.capabilities()));
    require(next.read_reply().kind == reply_kind::ok,
            "after " + label.str() +
                " a new connection was not answered with OK to COM_PING");
  });
}

// the caching_sha2_password authentication cache (PBS-10 part 3), checked
// against a model over any sequence of logins: the configured user is
// cached after its first successful full authentication; a cached user is
// only ever offered the fast path, which succeeds exactly with the right
// password and otherwise is refused at once (no fallback to the full
// authentication, as MySQL Server does); everyone else goes through the full
// authentication, which succeeds exactly with the right user and password
BOOST_AUTO_TEST_CASE(ReplicationSourceAuthenticationCacheFollowsModel) {
  run_property([](hegel::TestCase &tc) {
    const auto settings{tc.draw("settings", source_settings_generator())};
    struct login {
      bool right_user;
      bool right_password;
    };
    const auto logins{tc.draw(
        "logins",
        gs::vectors(gs::compose([](const hegel::TestCase &inner) {
                      return login{.right_user = inner.draw(gs::booleans()),
                                   .right_password =
                                       inner.draw(gs::booleans())};
                    }),
                    {.min_size = 1U, .max_size = 8U}))};

    const replication_source source{settings};
    bool cached{false};
    std::string history;
    const auto attempt{[&](const login &value) {
      history += std::string{history.empty() ? "" : ", "} +
                 (value.right_user ? "user" : "other user") + "/" +
                 (value.right_password ? "password" : "wrong password");
      const bool fast{cached && value.right_user};
      const bool expected_ok{value.right_user && value.right_password};
      mysql_client client{source.port()};
      const auto outcome{client.try_authenticate(
          value.right_user ? replica_user : std::string_view{"intruder"},
          value.right_password ? replica_password
                               : std::string_view{"not the password"})};
      const auto expected_path{fast ? (expected_ok ? auth_path::fast
                                                   : auth_path::none)
                                    : auth_path::full};
      require(outcome.path == expected_path,
              "logins [" + history + "]: the last one took the " +
                  (outcome.path == auth_path::fast   ? "fast path"
                   : outcome.path == auth_path::full ? "full path"
                                                     : "refusal") +
                  " instead of the " +
                  (expected_path == auth_path::fast   ? "fast path"
                   : expected_path == auth_path::full ? "full path"
                                                      : "refusal") +
                  (cached ? " (the user is cached)" : " (nothing cached)"));
      require(outcome.ok == expected_ok,
              "logins [" + history + "]: the last one " +
                  (outcome.ok ? "succeeded" : "failed") + " (" +
                  outcome.detail + ")");
      if (!fast && expected_ok) {
        cached = true;
      }
    }};
    for (const auto &value : logins) {
      attempt(value);
    }
    // whatever happened before, the right user and password still get in
    attempt(login{.right_user = true, .right_password = true});
  });
}

// statements are matched exactly or by the documented prefixes only: every
// known statement gets OK or a resultset, a near-miss gets an error
BOOST_AUTO_TEST_CASE(ReplicationSourceMatchesStatementsExactlyOrByPrefix) {
  run_property([](hegel::TestCase &tc) {
    const auto settings{tc.draw("settings", source_settings_generator())};
    const auto exact{exact_statements()};
    const auto cmds{tc.draw(
        "commands",
        gs::vectors(commands(gs::one_of({gs::sampled_from(exact),
                                         prefixed_statements(),
                                         near_miss_statements(),
                                         near_miss_statements()})),
                    {.min_size = 1U, .max_size = 20U}))};
    const auto transcript{run_session(settings, cmds)};
    for (std::size_t index{0U}; index < std::size(transcript); ++index) {
      if (transcript[index].sent.kind != command_kind::binlog_dump) {
        require_reply_class(transcript, index);
      }
    }
  });
}

// SELECT @@GLOBAL.SERVER_ID / SERVER_UUID return the configured values and
// GTID_MODE is ON exactly for a storage in GTID mode
BOOST_AUTO_TEST_CASE(ReplicationSourceReportsConfiguredIdentity) {
  run_property([](hegel::TestCase &tc) {
    const auto settings{tc.draw("settings", source_settings_generator())};
    const auto cmds{tc.draw(
        "commands",
        gs::vectors(commands(gs::one_of(
                        {gs::sampled_from(std::vector<std::string>{
                             std::string{select_server_id},
                             std::string{select_server_uuid},
                             std::string{select_gtid_mode}}),
                         checksum_statements(), prefixed_statements()})),
                    {.min_size = 1U, .max_size = 12U}))};
    const auto transcript{run_session(settings, cmds)};
    for (std::size_t index{0U}; index < std::size(transcript); ++index) {
      const auto &[sent, received] = transcript[index];
      if (sent.kind != command_kind::query) {
        continue;
      }
      std::optional<std::string> expected{};
      if (sent.statement == select_server_id) {
        expected = std::to_string(settings.server_id);
      } else if (sent.statement == select_server_uuid) {
        expected = settings.server_uuid;
      } else if (sent.statement == select_gtid_mode) {
        expected = settings.gtid_mode ? "ON" : "OFF";
      } else {
        continue;
      }
      require(received.kind == reply_kind::resultset && received.rows == 1U &&
                  std::size(received.first_row) == 1U &&
                  received.first_row.front() == expected,
              "expected '" + *expected + "':" + describe(transcript, index));
    }
  });
}


// the statement by which a replica asks for heartbeats (PBS-30), in the
// spellings a replica or a hand-written client may use: any letter case and
// spacing, leading zeros, the legacy and the modern variable (one or both)
struct heartbeat_period_statement {
  std::string text;
  // whether the listener has to accept it (OK); one without a value is a
  // syntax error and leaves heartbeats off
  bool accepted;
  // the period the listener has to use after it: 0 (off) for an explicit 0
  // and for a value that does not fit a signed 64-bit count of nanoseconds
  std::chrono::nanoseconds period;
};

std::ostream &operator<<(std::ostream &output,
                         const heartbeat_period_statement &value) {
  return output << "'" << value.text << "' ("
                << (value.accepted ? "accepted" : "refused") << ", period "
                << value.period.count() << " ns)";
}

[[nodiscard]] gs::Generator<heartbeat_period_statement>
heartbeat_period_statements() {
  return gs::compose([](const hegel::TestCase &tc) {
    const auto variable{[&tc] {
      std::string name{tc.draw(gs::sampled_from(
                           std::vector<std::string>{"master", "source"})) +
                       "_heartbeat_period"};
      if (tc.draw(gs::booleans())) {
        std::ranges::transform(name, std::begin(name), [](char character) {
          return static_cast<char>(
              std::toupper(static_cast<unsigned char>(character)));
        });
      }
      return "@" + name;
    }};
    const auto spacing{
        gs::sampled_from(std::vector<std::string>{"", " ", "  "})};
    std::string text{
        tc.draw(gs::sampled_from(std::vector<std::string>{"SET", "set", "Set"})) +
        tc.draw(gs::sampled_from(std::vector<std::string>{" ", "  ", "\t"})) +
        variable()};
    static constexpr int no_value_odds{10};
    if (tc.draw(gs::integers<int>({.min_value = 1, .max_value = no_value_odds})) ==
        1) {
      if (tc.draw(gs::booleans())) {
        text += tc.draw(spacing) + "=";
      }
      return heartbeat_period_statement{
          .text = text, .accepted = false,
          .period = std::chrono::nanoseconds::zero()};
    }
    static constexpr std::int64_t min_period_ms{60};
    static constexpr std::int64_t max_period_ms{150};
    std::string digits;
    std::chrono::nanoseconds period{};
    switch (tc.draw(gs::integers<int>({.min_value = 0, .max_value = 5}))) {
    case 0:
      digits = "0";
      break;
    case 1:
      // more than a signed 64-bit count of nanoseconds holds
      digits = "99999999999999999999";
      break;
    default:
      period = std::chrono::milliseconds{tc.draw(gs::integers<std::int64_t>(
          {.min_value = min_period_ms, .max_value = max_period_ms}))};
      digits = std::to_string(period.count());
      break;
    }
    digits.insert(0U,
                  tc.draw(gs::integers<std::size_t>(
                      {.min_value = 0U, .max_value = 3U})),
                  '0');
    const auto value_part{[&tc, &spacing, &digits] {
      return tc.draw(spacing) + "=" + tc.draw(spacing) + digits;
    }};
    text += value_part();
    if (tc.draw(gs::booleans())) {
      text += ", " + variable() + value_part();
    }
    return heartbeat_period_statement{
        .text = text, .accepted = true, .period = period};
  });
}

// a blocking dump that has sent everything stored gets a heartbeat about
// every period the client asked for (PBS-30), each reporting where the
// client has got to; a non-blocking dump, or one whose period is 0, gets
// none. Timing-based: the bounds are loose (the count, and the average
// spacing), and the property runs a fixed, small number of cases (about one
// second each)
BOOST_AUTO_TEST_CASE(ReplicationSourceHeartbeatsFollowTheRequestedPeriod) {
  hegel::Settings settings{};
  static constexpr std::uint64_t test_cases{25U};
  settings.test_cases = test_cases;
  settings.suppress_health_check = {hegel::HealthCheck::TooSlow};
  run_property(
      [](hegel::TestCase &tc) {
        const auto source_values{
            tc.draw("settings", source_settings_generator())};
        const auto statement{
            tc.draw("statement", heartbeat_period_statements())};
        const bool blocking{tc.draw("blocking", gs::booleans())};

        const replication_source source{source_values};
        mysql_client client{source.port()};
        client.authenticate(replica_user, replica_password);
        client.write_frame(encode_command(
            {.kind = command_kind::query,
             .statement = "SET @source_binlog_checksum = 'NONE', "
                          "@master_binlog_checksum = 'NONE'"},
            client.capabilities()));
        require(client.read_reply().kind == reply_kind::ok,
                "the checksum SET was not answered with OK");
        client.write_frame(encode_command(
            {.kind = command_kind::query, .statement = statement.text},
            client.capabilities()));
        const auto period_reply{client.read_reply()};
        std::ostringstream label;
        label << statement << (blocking ? ", blocking" : ", non-blocking")
              << " dump";
        require(period_reply.kind == (statement.accepted ? reply_kind::ok
                                                         : reply_kind::error),
                label.str() + ": answered with " +
                    std::string{to_string(period_reply.kind)});
        client.write_frame(encode_command({.kind = command_kind::binlog_dump,
                                           .statement = {},
                                           .dump_blocking = blocking},
                                          client.capabilities()));

        // every event packet until EOF, or for 'window' of a blocking dump
        using clock = std::chrono::steady_clock;
        static constexpr auto window{std::chrono::milliseconds{900}};
        const auto deadline{clock::now() + window};
        std::vector<std::pair<clock::time_point, std::vector<std::byte>>>
            received;
        bool ended{false};
        while (true) {
          const auto remaining{
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  deadline - clock::now())};
          if (remaining.count() <= 0 || !client.wait_readable(remaining)) {
            break;
          }
          const auto frame{client.read_frame()};
          require(frame.has_value(),
                  label.str() + ": the listener closed the connection");
          const std::string_view payload{
              std::string_view{*frame}.substr(frame_header_size)};
          if (is_eof_packet(payload)) {
            ended = true;
            break;
          }
          require(!payload.empty() && payload.front() == '\0',
                  label.str() + ": a packet that is not an event");
          const auto event{payload.substr(1U)};
          received.emplace_back(
              clock::now(),
              std::vector<std::byte>{
                  reinterpret_cast<const std::byte *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                      std::data(event)),
                  reinterpret_cast<const std::byte *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                      std::data(event) + std::size(event))});
        }
        require(ended == !blocking,
                label.str() + (blocking ? ": ended with EOF"
                                        : ": did not end with EOF"));

        // the heartbeats, each checked against the coordinates the client
        // had reached
        heartbeat_testing::replica_coordinates coordinates;
        std::vector<clock::time_point> heartbeats;
        std::optional<clock::time_point> last_stored;
        for (const auto &[moment, event] : received) {
          const bool is_heartbeat{
              std::size(event) >
                  binsrv::events::common_header_view_base::size_in_bytes &&
              static_cast<std::uint8_t>(event[binsrv::events::
                                                  common_header_view_base::
                                                      type_code_offset]) ==
                  static_cast<std::uint8_t>(
                      binsrv::events::code_type::heartbeat_log_v2)};
          if (!is_heartbeat) {
            require(heartbeats.empty(),
                    label.str() + ": a stored event after a heartbeat");
            heartbeat_testing::follow(coordinates, event,
                                      /*session_checksum=*/false,
                                      /*format_description_checksum=*/true);
            last_stored = moment;
            continue;
          }
          const auto error{heartbeat_testing::heartbeat_error(
              event, coordinates, stored_fde_server_id,
              /*with_checksum=*/true)};
          require(error.empty(), label.str() + ": " + error);
          heartbeats.push_back(moment);
        }

        const auto period{statement.period};
        if (!blocking || period == std::chrono::nanoseconds::zero()) {
          require(heartbeats.empty(),
                  label.str() + ": " + std::to_string(std::size(heartbeats)) +
                      " heartbeat(s) where none are due");
          return;
        }
        require(last_stored.has_value(),
                label.str() + ": no stored event was streamed");
        const auto expected{window / period};
        const auto lower{std::max<std::int64_t>(1, expected / 3)};
        const auto upper{expected + 2};
        require(std::ssize(heartbeats) >= lower &&
                    std::ssize(heartbeats) <= upper,
                label.str() + ": " + std::to_string(std::size(heartbeats)) +
                    " heartbeat(s) in " + std::to_string(window.count()) +
                    " ms, expected " + std::to_string(lower) + " to " +
                    std::to_string(upper));
        // single gaps are measured when the client reads the packets, which
        // TCP may deliver late and then together (a small packet held back
        // until the previous one is acknowledged), so only their average is
        // compared with the period
        if (std::size(heartbeats) >= 2U) {
          const auto average{(heartbeats.back() - heartbeats.front()) /
                             (std::ssize(heartbeats) - 1)};
          require(average >= period / 2,
                  label.str() + ": heartbeats every " +
                      std::to_string(std::chrono::duration_cast<
                                         std::chrono::milliseconds>(average)
                                         .count()) +
                      " ms on average");
        }
      },
      settings);
}

// COM_BINLOG_DUMP_GTID (PBS-29 part 2) as a MySQL source answers it: a
// replica that lacks a GTID the source has purged gets ERR 1236; any other
// replica gets exactly the stored transactions whose GTIDs it does not have,
// in stored order (the source starts at the first binlog it needs and skips
// the transactions the replica has), whatever else its set holds (GTIDs of
// other servers)
BOOST_AUTO_TEST_CASE(ReplicationSourceDumpGtidSendsTheMissingTransactions) {
  run_property([](hegel::TestCase &tc) {
    auto source_values{tc.draw("settings", source_settings_generator())};
    source_values.gtid_mode = true;
    const gtid_layout layout{
        .first_gno = tc.draw(
            "first_gno", gs::integers<binsrv::gtids::gno_t>(
                             {.min_value = 1U, .max_value = 4U})),
        .transactions_per_binlog = tc.draw(
            "transactions_per_binlog",
            gs::vectors(gs::integers<std::size_t>(
                            {.min_value = 1U, .max_value = 4U}),
                        {.min_size = 1U, .max_size = 4U}))};
    binsrv::gtids::gno_t last_gno{layout.first_gno - 1U};
    for (const auto count : layout.transactions_per_binlog) {
      last_gno += count;
    }

    // the replica's executed GTIDs: everything up to a point (the usual
    // case), any subset, or a prefix plus GTIDs of another server
    binsrv::gtids::gtid_set replica;
    const auto shape{tc.draw(
        "replica_shape",
        gs::sampled_from(std::vector<std::string>{"prefix", "subset",
                                                  "prefix_and_foreign"}))};
    if (shape == "subset") {
      for (binsrv::gtids::gno_t gno{1U}; gno <= last_gno; ++gno) {
        if (tc.draw(gs::booleans())) {
          replica.add(stored_uuid, binsrv::gtids::tag{}, gno);
        }
      }
    } else {
      const auto executed{tc.draw(
          "executed_up_to", gs::integers<binsrv::gtids::gno_t>(
                                {.min_value = 0U, .max_value = last_gno}))};
      for (binsrv::gtids::gno_t gno{1U}; gno <= executed; ++gno) {
        replica.add(stored_uuid, binsrv::gtids::tag{}, gno);
      }
      if (shape == "prefix_and_foreign") {
        const binsrv::gtids::uuid foreign{
            "77777777-8888-9999-aaaa-bbbbccccdddd"};
        for (binsrv::gtids::gno_t gno{1U}; gno <= 3U; ++gno) {
          replica.add(foreign, binsrv::gtids::tag{}, gno);
        }
      }
    }

    bool lacks_purged{false};
    for (binsrv::gtids::gno_t gno{1U}; gno < layout.first_gno; ++gno) {
      lacks_purged =
          lacks_purged || !replica.contains(binsrv::gtids::gtid{stored_uuid, gno});
    }
    std::vector<binsrv::gtids::gno_t> expected;
    for (auto gno{layout.first_gno}; gno <= last_gno; ++gno) {
      if (!replica.contains(binsrv::gtids::gtid{stored_uuid, gno})) {
        expected.push_back(gno);
      }
    }

    std::ostringstream label;
    label << layout << ", replica has '" << replica << "'";

    const replication_source source{source_values, &layout};
    mysql_client client{source.port()};
    client.authenticate(replica_user, replica_password);
    client.write_frame(encode_command(
        {.kind = command_kind::query,
         .statement = "SET @source_binlog_checksum = 'NONE', "
                      "@master_binlog_checksum = 'NONE'"},
        client.capabilities()));
    require(client.read_reply().kind == reply_kind::ok,
            "the checksum SET was not answered with OK");

    std::vector<std::byte> encoded(replica.calculate_encoded_size());
    util::byte_span encoded_span{encoded};
    replica.encode_to(encoded_span);
    using dump_gtid = cp::message::client::BinlogDumpGtid;
    stdx::flags<dump_gtid::Flags> flags{};
    flags |= dump_gtid::Flags::non_blocking;
    static constexpr std::uint32_t replica_server_id{2U};
    client.write_frame(encode_client_frame(
        0U,
        dump_gtid{flags, replica_server_id, "",
                  binsrv::events::magic_binlog_offset,
                  std::string{reinterpret_cast<const char *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                                  std::data(encoded)),
                              std::size(encoded)}},
        client.capabilities()));

    // the transactions streamed (by the GNO of their GTID_LOG events) up to
    // EOF, or the error
    using header = binsrv::events::common_header_view_base;
    // GTID_LOG post header: flags (1 byte), UUID (16 bytes), GNO (8 bytes)
    static constexpr std::size_t gno_offset{header::size_in_bytes + 1U + 16U};
    static constexpr auto reply_timeout{std::chrono::seconds{10}};
    std::vector<binsrv::gtids::gno_t> received;
    std::optional<std::uint16_t> error_code;
    std::string ending{"no reply within " +
                       std::to_string(reply_timeout.count()) + " s"};
    while (client.wait_readable(reply_timeout)) {
      const auto frame{client.read_frame()};
      if (!frame.has_value()) {
        ending = "the connection closed";
        break;
      }
      const std::string_view payload{
          std::string_view{*frame}.substr(frame_header_size)};
      if (is_eof_packet(payload)) {
        ending = "EOF";
        break;
      }
      if (!payload.empty() && static_cast<std::uint8_t>(payload[0]) == 0xFFU) {
        error_code = static_cast<std::uint16_t>(read_fixed_int(payload, 1U, 2U));
        ending = "ERR " + std::to_string(*error_code);
        break;
      }
      const auto event{payload.substr(1U)};
      if (std::size(event) >= gno_offset + 8U &&
          static_cast<std::uint8_t>(event[header::type_code_offset]) ==
              static_cast<std::uint8_t>(binsrv::events::code_type::gtid_log)) {
        received.push_back(read_fixed_int(event, gno_offset, 8U));
      }
    }

    const auto list{[](const std::vector<binsrv::gtids::gno_t> &gnos) {
      std::string result;
      for (const auto gno : gnos) {
        result += (result.empty() ? "" : ",") + std::to_string(gno);
      }
      return "[" + result + "]";
    }};
    if (lacks_purged) {
      require(error_code == er_source_fatal_error_reading_binlog,
              label.str() + ": a replica lacking purged GTIDs was answered "
                            "with " + ending + " instead of ERR 1236");
      return;
    }
    require(ending == "EOF",
            label.str() + ": the dump ended with " + ending + " after GNOs " +
                list(received) + ", expected GNOs " + list(expected));
    require(received == expected,
            label.str() + ": streamed GNOs " + list(received) +
                ", expected " + list(expected));
  });
}

namespace {

// a logger that keeps every line the listener logs at info level or above
class capturing_logger final : public binsrv::basic_logger {
public:
  capturing_logger() noexcept : basic_logger{binsrv::log_severity::info} {}

  [[nodiscard]] std::vector<std::string> lines() const {
    const std::scoped_lock lock{lines_mutex_};
    return lines_;
  }

private:
  mutable std::mutex lines_mutex_;
  std::vector<std::string> lines_;

  void do_log(std::string_view message) override {
    const std::scoped_lock lock{lines_mutex_};
    lines_.emplace_back(message);
  }
};

// where in a session the client disconnects
enum class disconnect_point : std::uint8_t {
  after_server_greeting,
  inside_client_greeting,
  after_authentication,
  inside_command,
  // not a disconnect: the client stays connected and silent until the
  // listener's read_timeout ends the session
  silent_after_authentication
};

[[nodiscard]] std::string_view to_string(disconnect_point point) {
  switch (point) {
  case disconnect_point::after_server_greeting:
    return "after the server greeting";
  case disconnect_point::inside_client_greeting:
    return "inside the client greeting";
  case disconnect_point::after_authentication:
    return "after authentication";
  case disconnect_point::inside_command:
    return "inside a command";
  case disconnect_point::silent_after_authentication:
    return "silent after authentication";
  }
  return "?";
}

} // anonymous namespace

// the line network_service::handle_exception logs for a session ending
// with any error (or std::nullopt while none)
[[nodiscard]] std::optional<std::string>
session_end_line(const capturing_logger &logger) {
  for (const auto &line : logger.lines()) {
    if (line.find("net    : session ") != std::string::npos ||
        line.find("caught in session ") != std::string::npos) {
      return line;
    }
  }
  return std::nullopt;
}

// waits up to 'limit' for the session's end to be logged; returns the whole
// log, indented, for messages
std::string wait_for_session_end(const capturing_logger &logger,
                                 std::chrono::seconds limit) {
  const auto deadline{std::chrono::steady_clock::now() + limit};
  while (!session_end_line(logger).has_value() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }
  std::string log;
  for (const auto &line : logger.lines()) {
    log += "\n  " + line;
  }
  return log;
}

// a client that goes away at any point of a session (an orderly close or a
// reset) ends only that session, which the listener reports as "connection
// closed by peer" at info level, never at error level: a replica or
// mysqlbinlog disconnecting is routine (ticket 42, PBS-115: fails today for an
// orderly close, which network_service::handle_exception looks for as
// boost::asio::error::eof in the system category, while Asio reports it in
// asio.misc; the reset is ECONNRESET in the system category and passes); a
// client that stays silent past read_timeout is reported as a read timeout,
// also at info level
BOOST_AUTO_TEST_CASE(ReplicationSourceLogsDisconnectsAsRoutine) {
  run_property([](hegel::TestCase &tc) {
    auto settings{tc.draw("settings", source_settings_generator())};
    const auto point{static_cast<disconnect_point>(tc.draw(
        "point", gs::integers<std::uint8_t>(
                     {.min_value = 0U,
                      .max_value = static_cast<std::uint8_t>(
                          disconnect_point::silent_after_authentication)})))};
    const bool silent{point == disconnect_point::silent_after_authentication};
    const auto abortive{!silent && tc.draw("abortive", gs::booleans())};
    const std::string label{
        std::string{"a client "} + std::string{to_string(point)} +
        (silent ? "" : (abortive ? ", disconnecting with a reset"
                                 : ", disconnecting with a FIN"))};
    // the shortest allowed timeout for the silent client; long enough for
    // the authentication otherwise
    if (silent) {
      settings.read_timeout = 1U;
    }

    const auto logger{std::make_shared<capturing_logger>()};
    const replication_source source{settings, nullptr, logger};
    mysql_client client{source.port()};
    switch (point) {
    case disconnect_point::after_server_greeting:
      require(client.read_frame().has_value(), "no server greeting");
      break;
    case disconnect_point::inside_client_greeting:
      require(client.read_frame().has_value(), "no server greeting");
      // a frame header announcing 64 bytes, then only 3 of them
      client.write_frame(network_buffer_type{"\x40\x00\x00\x01xyz", 7U});
      break;
    case disconnect_point::after_authentication:
    case disconnect_point::silent_after_authentication:
      client.authenticate(replica_user, replica_password);
      break;
    case disconnect_point::inside_command:
      client.authenticate(replica_user, replica_password);
      // COM_QUERY announcing 32 bytes, then only "SEL"
      client.write_frame(network_buffer_type{"\x20\x00\x00\x00\x03SEL", 8U});
      break;
    }
    if (!silent) {
      client.disconnect(abortive);
    }

    const auto log{wait_for_session_end(*logger, std::chrono::seconds{10})};
    const auto end{session_end_line(*logger)};
    require(end.has_value(),
            label + ": the listener logged no end of the session:" + log);
    for (const auto &line : logger->lines()) {
      require(line.find("[  error]") == std::string::npos &&
                  line.find("[  fatal]") == std::string::npos,
              label + " was logged as an error:" + log);
    }
    const std::string_view expected{silent ? "frame header read timeout"
                                           : "connection closed by peer"};
    require(end->find("[   info]") != std::string::npos &&
                end->find(expected) != std::string::npos,
            label + " was not logged as '" + std::string{expected} +
                "' at info level:" + log);
  });
}

// a COM_BINLOG_DUMP the sender cannot serve (a binlog name that is not
// stored, or not a binlog name at all) ends that session with the reason in
// the log ("exception caught in session ..."), and the listener goes on
// accepting connections
BOOST_AUTO_TEST_CASE(ReplicationSourceLogsAFailedDumpAndKeepsListening) {
  const source_settings settings{.gtid_mode = false,
                                 .server_id = 142U,
                                 .server_uuid =
                                     "00000000-0000-0000-0000-00000000008e"};
  for (const std::string binlog : {"binlog.000099", "not-a-binlog-name"}) {
    const auto logger{std::make_shared<capturing_logger>()};
    const replication_source source{settings, nullptr, logger};
    const std::string label{"a dump of '" + binlog + "'"};
    {
      mysql_client client{source.port()};
      client.authenticate(replica_user, replica_password);
      client.write_frame(encode_command(
          {.kind = command_kind::query,
           .statement = "SET @source_binlog_checksum = 'NONE', "
                        "@master_binlog_checksum = 'NONE'"},
          client.capabilities()));
      BOOST_REQUIRE(client.read_reply().kind == reply_kind::ok);
      client.write_frame(encode_command({.kind = command_kind::binlog_dump,
                                         .statement = {},
                                         .dump_binlog = binlog},
                                        client.capabilities()));
      static_cast<void>(client.read_dump_reply());
    }
    const auto log{wait_for_session_end(*logger, std::chrono::seconds{10})};
    const auto end{session_end_line(*logger)};
    BOOST_TEST_INFO(label << ", listener log:" << log);
    BOOST_REQUIRE(end.has_value());
    BOOST_TEST_INFO(label << ", listener log:" << log);
    BOOST_CHECK(end->find("exception caught in session ") !=
                std::string::npos);

    mysql_client next{source.port()};
    next.authenticate(replica_user, replica_password);
    next.write_frame(encode_command(
        {.kind = command_kind::ping, .statement = {}}, next.capabilities()));
    BOOST_TEST_INFO(label);
    BOOST_CHECK(next.read_reply().kind == reply_kind::ok);
  }
}

// COM_RESET_CONNECTION and COM_CHANGE_USER, which a connection pool or the
// mysql client may send, get a reply (MySQL: OK, and a re-authentication;
// refused with ERR as unsupported is acceptable) and do not end the session:
// a COM_PING after a refused one is still answered with OK
BOOST_AUTO_TEST_CASE(ReplicationSourceAnswersResetConnectionAndChangeUser) {
  const source_settings settings{.gtid_mode = false,
                                 .server_id = 142U,
                                 .server_uuid =
                                     "00000000-0000-0000-0000-00000000008e"};
  for (const auto kind :
       {command_kind::reset_connection, command_kind::change_user}) {
    const replication_source source{settings};
    mysql_client client{source.port()};
    client.authenticate(replica_user, replica_password);
    const command sent{.kind = kind, .statement = {}};
    client.write_frame(encode_command(sent, client.capabilities()));
    const auto received{client.read_reply()};
    std::ostringstream label;
    label << sent << " -> " << received;
    BOOST_TEST_MESSAGE(label.str());
    BOOST_TEST_INFO(label.str());
    BOOST_CHECK(received.kind != reply_kind::closed &&
                received.kind != reply_kind::malformed);
    if (received.kind == reply_kind::error ||
        kind == command_kind::reset_connection) {
      client.write_frame(encode_command(
          {.kind = command_kind::ping, .statement = {}}, client.capabilities()));
      BOOST_TEST_INFO(label.str() << ", then COM_PING");
      BOOST_CHECK(client.read_reply().kind == reply_kind::ok);
    }
  }
}

// a full authentication whose RSA-encrypted password is garbled (random
// bytes of the key's size, which do not decrypt; half of a correct
// encryption; a correct encryption of the password without its terminating
// NUL) is refused like a wrong password, the listener keeps serving, and the
// failed attempt does not fill the authentication cache: the next login with
// the right password still takes the full path and succeeds
BOOST_AUTO_TEST_CASE(ReplicationSourceRefusesGarbledEncryptedPasswords) {
  const source_settings settings{.gtid_mode = false,
                                 .server_id = 142U,
                                 .server_uuid =
                                     "00000000-0000-0000-0000-00000000008e"};
  using garbling = mysql_client::password_garbling;
  for (const auto kind : {garbling::random_bytes, garbling::truncated,
                          garbling::not_terminated}) {
    const replication_source source{settings};
    BOOST_TEST_INFO("garbling " << static_cast<int>(kind));
    {
      mysql_client client{source.port()};
      const auto outcome{
          client.try_authenticate(replica_user, replica_password, kind)};
      BOOST_CHECK(outcome.path == auth_path::full);
      BOOST_CHECK(!outcome.ok);
    }
    mysql_client next{source.port()};
    const auto outcome{next.try_authenticate(replica_user, replica_password)};
    BOOST_TEST_INFO("garbling " << static_cast<int>(kind));
    BOOST_CHECK(outcome.path == auth_path::full);
    BOOST_TEST_INFO("garbling " << static_cast<int>(kind));
    BOOST_CHECK(outcome.ok);
  }
}
