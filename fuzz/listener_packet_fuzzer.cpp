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

// libFuzzer target: the packets a client sends to the replication source
// listener of 'pull' (untrusted network input), split into MySQL frames by
// their 3-byte length header, as the listener reads them. They drive
// minimysql::connection_context in the order network_service does: the
// client greeting, an auth method switch when the client asked for another
// plugin, then the caching_sha2_password fast or full authentication (the
// "encrypted password" is decrypted with a real RSA key, and a request for
// the public key is answered), then the command loop, where a query is
// matched against the handshake regular expressions and a heartbeat period
// is parsed as the listener does. A fuzzer cannot produce a valid scramble or
// encrypted password, so the command loop runs whatever the authentication
// decided: the point is to reach every parser. A rejection is a
// std::exception (the listener closes that session); a crash, a
// std::terminate or a sanitizer report is a bug.

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <regex>
#include <string>
#include <vector>

#include "minimysql/connection_context.hpp"
#include "minimysql/handshake_query_patterns.hpp"

#include "opensslpp/evp_pkey.hpp"

namespace {

constexpr std::size_t frame_header_size{4U};

struct server_key {
  opensslpp::evp_pkey private_key;
  std::string public_pem;
};

// generated once per process: generating an RSA key takes tens of
// milliseconds
[[nodiscard]] const server_key &get_server_key() {
  static constexpr std::size_t key_bits{2048U};
  static const server_key key{[] {
    auto private_key{opensslpp::evp_pkey::generate(
        opensslpp::evp_pkey::get_rsa_algorithm_name(), key_bits)};
    auto public_pem{opensslpp::evp_pkey::export_public_pem(private_key)};
    return server_key{.private_key = std::move(private_key),
                      .public_pem = std::move(public_pem)};
  }()};
  return key;
}

// the listener's regular expressions, as network_service.cpp compiles them
[[nodiscard]] const std::vector<std::regex> &get_query_patterns() {
  static const std::vector<std::regex> patterns{
      std::regex{std::string{minimysql::heartbeat_period_query_pattern},
                 std::regex::icase},
      std::regex{std::string{minimysql::set_names_query_pattern},
                 std::regex::icase},
      std::regex{std::string{minimysql::slave_uuid_query_pattern},
                 std::regex::icase},
      std::regex{std::string{minimysql::replica_uuid_query_pattern},
                 std::regex::icase}};
  return patterns;
}

// splits the input into frames by their own length header; a short or
// lying header takes the rest
class frame_reader {
public:
  frame_reader(const std::uint8_t *data, std::size_t size)
      : data_{data}, size_{size} {}

  [[nodiscard]] bool next(minimysql::network_buffer_type &frame) {
    if (offset_ == size_) {
      return false;
    }
    auto length{size_ - offset_};
    if (length >= frame_header_size) {
      const std::size_t payload_size{
          static_cast<std::size_t>(data_[offset_]) |
          (static_cast<std::size_t>(data_[offset_ + 1U]) << 8U) |
          (static_cast<std::size_t>(data_[offset_ + 2U]) << 16U)};
      if (frame_header_size + payload_size <= length) {
        length = frame_header_size + payload_size;
      }
    }
    frame.assign(reinterpret_cast<const char *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                     std::next(data_, static_cast<std::ptrdiff_t>(offset_))),
                 length);
    offset_ += length;
    return true;
  }

private:
  const std::uint8_t *data_;
  std::size_t size_;
  std::size_t offset_{0U};
};

void run_query(const std::string &statement) {
  std::smatch match{};
  for (const auto &pattern : get_query_patterns()) {
    if (!std::regex_search(statement, match, pattern)) {
      continue;
    }
    if (match.size() > 1U && match[1].matched) {
      // the heartbeat period, parsed as the listener does
      const std::string captured{match[1].str()};
      std::int64_t period{0};
      [[maybe_unused]] const auto parsed{std::from_chars(
          std::data(captured), std::data(captured) + std::size(captured),
          period)};
    }
    return;
  }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data,
                                      std::size_t size) {
  const auto &key{get_server_key()};
  frame_reader reader{data, size};
  minimysql::network_buffer_type frame;
  try {
    minimysql::connection_context context{false, "rpl", "password"};
    [[maybe_unused]] const auto greeting{
        context.generate_encoded_server_greeting()};
    if (!reader.next(frame)) {
      return 0;
    }
    context.parse_client_greeting(frame);

    if (!context.check_shared_plugin_auth_supported()) {
      [[maybe_unused]] const auto method_switch{
          context.generate_encoded_auth_method_switch()};
      if (!reader.next(frame)) {
        return 0;
      }
      context.parse_client_auth_method_switch(frame);
    }

    if (context.check_client_fast_authentication()) {
      [[maybe_unused]] const auto fast_auth{context.generate_encoded_fast_auth()};
    } else {
      [[maybe_unused]] const auto full_auth{context.generate_encoded_full_auth()};
      if (!reader.next(frame)) {
        return 0;
      }
      context.parse_client_full_auth_response(frame);
      if (context.check_client_requested_public_key()) {
        [[maybe_unused]] const auto public_key{
            context.generate_encoded_public_key(key.public_pem)};
        if (!reader.next(frame)) {
          return 0;
        }
        context.parse_client_full_auth_response(frame);
      }
      [[maybe_unused]] const bool authenticated{
          context.check_client_full_authentication(key.private_key)};
    }
    [[maybe_unused]] const auto ok{context.generate_encoded_ok()};

    while (reader.next(frame)) {
      context.enter_command_loop_iteration();
      context.parse_client_command(frame);
      switch (context.get_client_mysql_command()) {
      case minimysql::client_command_type::query:
        run_query(context.get_client_statement());
        break;
      case minimysql::client_command_type::binlog_dump: {
        [[maybe_unused]] const auto non_blocking{
            context.check_binlog_non_blocking_dump()};
        [[maybe_unused]] const auto &binlog{context.get_binlog_filename()};
        [[maybe_unused]] const auto position{context.get_binlog_position()};
      } break;
      default:
        break;
      }
      [[maybe_unused]] const auto reply{context.generate_encoded_ok()};
    }
  } catch (const std::exception &) {
    // a clean rejection
  }
  return 0;
}
