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

// Property-based tests for minimysql::connection_context, the MySQL protocol
// side of the replication source listener used in 'pull' mode, built with
// Hegel (https://hegel.dev).
//
// connection_context is driven in memory, with MySQL Router's protocol codec
// (the one connection_context itself uses) playing the client:
// - a client with the right credentials completes the caching_sha2_password
//   handshake, and any other credentials are rejected;
// - client commands are parsed back with the fields they were sent with;
// - corrupted client packets are rejected with an exception, never anything
//   worse (run under AddressSanitizer for memory errors);
// - a binlog event sent to a replica is received back exactly, for any event
//   size, as a MySQL client reassembles packets (payloads of 0xFFFFFF bytes
//   or more are split into several packets).

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/asio/buffer.hpp>

#define BOOST_TEST_MODULE MinimysqlPropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

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

#include "minimysql/connection_context.hpp"
#include "minimysql/connection_context_fwd.hpp"
#include "minimysql/network_io_operations_fwd.hpp"

#include "opensslpp/digest_context.hpp"

#include "util/byte_span_fwd.hpp"

namespace {

namespace gs = hegel::generators;
namespace cp = classic_protocol;

using property_testing::require;
using property_testing::run_property;

using minimysql::network_buffer_type;

constexpr std::string_view auth_method{"caching_sha2_password"};
constexpr std::size_t frame_header_size{4U};
constexpr std::size_t max_packet_payload{0xFFFFFFU};

// ---------------------------------------------------------------------------
// the client side
// ---------------------------------------------------------------------------

// text of a given length range (TextParams has many optional fields, so it is
// filled member by member rather than with designated initializers)
[[nodiscard]] gs::Generator<std::string>
text_of_size(std::size_t min_size, std::size_t max_size,
             std::optional<std::uint32_t> max_codepoint = std::nullopt) {
  gs::TextParams params{};
  params.min_size = min_size;
  params.max_size = max_size;
  params.max_codepoint = max_codepoint;
  return gs::text(params);
}

// what a MySQL client sends for caching_sha2_password (fast authentication):
// SHA256(password) XOR SHA256(SHA256(SHA256(password)) . scramble)
[[nodiscard]] std::string client_scramble(std::string_view password,
                                          std::string_view challenge) {
  const std::string digest{"SHA256"};
  const auto hashed{opensslpp::digest_context::calculate(digest, password)};
  const auto double_hashed{
      opensslpp::digest_context::calculate(digest, hashed)};
  opensslpp::digest_context salted{digest};
  salted.update(double_hashed);
  salted.update(challenge);
  const auto salted_hash{salted.finalize()};
  std::string result(std::size(hashed), '\0');
  for (std::size_t index{0U}; index < std::size(hashed); ++index) {
    result[index] = static_cast<char>(
        static_cast<std::uint8_t>(hashed[index]) ^
        static_cast<std::uint8_t>(salted_hash[index]));
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

// decodes the server greeting and returns the challenge (scramble) it
// carries, without the trailing NUL that is sent with it
[[nodiscard]] std::string
decode_challenge(const network_buffer_type &server_greeting,
                 const minimysql::capability_bitset &capabilities) {
  auto buffer{boost::asio::buffer(server_greeting)};
  const auto decode_result{
      cp::decode<cp::frame::Frame<cp::message::server::Greeting>>(
          buffer, capabilities)};
  require(decode_result.has_value(),
          "the client cannot decode the server greeting");
  std::string challenge{decode_result.value().second.payload().auth_method_data()};
  if (!challenge.empty() && challenge.back() == '\0') {
    challenge.pop_back();
  }
  return challenge;
}

struct credentials {
  std::string username;
  std::string password;
};

std::ostream &operator<<(std::ostream &output, const credentials &value) {
  return output << "{user '" << value.username << "', password '"
                << value.password << "'}";
}

// MySQL user names and passwords are sent NUL-terminated, so they cannot
// contain NUL characters; both must be non-empty in the configuration
[[nodiscard]] gs::Generator<std::string> names() {
  static constexpr std::size_t max_size{40U};
  return text_of_size(1U, max_size)
      .filter([](const std::string &value) {
        return value.find('\0') == std::string::npos;
      });
}

[[nodiscard]] gs::Generator<credentials> credentials_generator() {
  return gs::compose([](const hegel::TestCase &tc) {
    return credentials{.username = tc.draw(names()),
                       .password = tc.draw(names())};
  });
}

// runs the server greeting / client greeting exchange: the client sends
// 'client_credentials', with its scramble computed for 'challenge_override'
// if set, or for the challenge from the server greeting otherwise
[[nodiscard]] bool authenticate(
    const credentials &server_credentials,
    const credentials &client_credentials,
    const std::optional<std::string> &challenge_override = std::nullopt) {
  minimysql::connection_context context{server_credentials.username,
                                        server_credentials.password};
  const auto server_greeting{context.generate_encoded_server_greeting()};
  const auto capabilities{context.get_server_capabilities()};
  const auto challenge{decode_challenge(server_greeting, capabilities)};
  require(challenge == context.get_server_auth_method_data(),
          "the challenge the client receives differs from the one the server "
          "verifies against");

  const cp::message::client::Greeting client_greeting{
      capabilities,
      static_cast<std::uint32_t>(max_packet_payload),
      0U,
      client_credentials.username,
      client_scramble(client_credentials.password,
                      challenge_override.value_or(challenge)),
      "",
      std::string{auth_method},
      ""};
  context.parse_client_greeting(
      encode_client_frame(1U, client_greeting, capabilities));
  require(context.get_client_username() == client_credentials.username,
          "the server parsed user '" + context.get_client_username() +
              "' instead of '" + client_credentials.username + "'");
  require(context.get_client_auth_method() == auth_method,
          "the server parsed auth method '" +
              context.get_client_auth_method() + "'");
  return context.check_client_authentication();
}

// a connection_context that has completed the handshake and is waiting for
// a command, as in the session's command loop
struct authenticated_session {
  minimysql::connection_context context;
  minimysql::capability_bitset capabilities;
};

[[nodiscard]] authenticated_session make_authenticated_session() {
  const credentials server{.username = "rpl", .password = "password"};
  authenticated_session result{
      .context = minimysql::connection_context{server.username,
                                               server.password},
      .capabilities = {}};
  const auto server_greeting{result.context.generate_encoded_server_greeting()};
  result.capabilities = result.context.get_server_capabilities();
  const auto challenge{decode_challenge(server_greeting, result.capabilities)};
  const cp::message::client::Greeting client_greeting{
      result.capabilities,
      static_cast<std::uint32_t>(max_packet_payload),
      0U,
      server.username,
      client_scramble(server.password, challenge),
      "",
      std::string{auth_method},
      ""};
  result.context.parse_client_greeting(
      encode_client_frame(1U, client_greeting, result.capabilities));
  require(result.context.check_client_authentication(),
          "the test session could not authenticate");
  result.context.enter_command_loop_iteration();
  return result;
}

// ---------------------------------------------------------------------------
// client commands
// ---------------------------------------------------------------------------

enum class command_kind : std::uint8_t { query, ping, quit, binlog_dump };

struct command {
  command_kind kind;
  std::string statement;  // query
  bool non_blocking;      // binlog_dump
  std::uint32_t server_id;
  std::string filename;
  std::uint32_t position;
};

std::ostream &operator<<(std::ostream &output, const command &value) {
  switch (value.kind) {
  case command_kind::query:
    return output << "query '" << value.statement << "'";
  case command_kind::ping:
    return output << "ping";
  case command_kind::quit:
    return output << "quit";
  case command_kind::binlog_dump:
    return output << "binlog_dump(" << (value.non_blocking ? "non-blocking, " : "")
                  << "server_id " << value.server_id << ", '" << value.filename
                  << "':" << value.position << ')';
  }
  return output;
}

[[nodiscard]] gs::Generator<command> commands() {
  return gs::compose([](const hegel::TestCase &tc) {
    static constexpr std::size_t max_statement_size{200U};
    command result{.kind = tc.draw(gs::sampled_from(
                       {command_kind::query, command_kind::ping,
                        command_kind::quit, command_kind::binlog_dump})),
                   .statement = {},
                   .non_blocking = false,
                   .server_id = 0U,
                   .filename = {},
                   .position = 0U};
    if (result.kind == command_kind::query) {
      result.statement = tc.draw(text_of_size(0U, max_statement_size));
    }
    if (result.kind == command_kind::binlog_dump) {
      result.non_blocking = tc.draw(gs::booleans());
      result.server_id = tc.draw(gs::integers<std::uint32_t>());
      result.filename = tc.draw(gs::from_regex("([a-z_]{1,20}\\.[0-9]{6})?"));
      result.position = tc.draw(gs::integers<std::uint32_t>());
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
  case command_kind::quit:
    return encode_client_frame(0U, cp::message::client::Quit{}, capabilities);
  case command_kind::binlog_dump: {
    using dump = cp::message::client::BinlogDump;
    stdx::flags<dump::Flags> flags{};
    if (value.non_blocking) {
      flags |= dump::Flags::non_blocking;
    }
    return encode_client_frame(
        0U, dump{flags, value.server_id, value.filename, value.position},
        capabilities);
  }
  }
  throw std::logic_error{"unknown command kind"};
}

// ---------------------------------------------------------------------------
// MySQL packets
// ---------------------------------------------------------------------------

// reassembles a payload from consecutive packets as a MySQL client does: a
// packet with a 0xFFFFFF-byte payload is continued by the next one
[[nodiscard]] std::string reassemble_payload(const network_buffer_type &frames) {
  std::string payload;
  std::size_t offset{0U};
  std::uint8_t expected_sequence_number{0U};
  while (true) {
    require(offset + frame_header_size <= std::size(frames),
            "the packets end in the middle of a packet header");
    const auto byte_at{[&frames](std::size_t index) {
      return static_cast<std::size_t>(static_cast<std::uint8_t>(frames[index]));
    }};
    const auto length{byte_at(offset) | (byte_at(offset + 1U) << 8U) |
                      (byte_at(offset + 2U) << 16U)};
    const auto sequence_number{static_cast<std::uint8_t>(byte_at(offset + 3U))};
    require(sequence_number == expected_sequence_number,
            "packet sequence number " + std::to_string(sequence_number) +
                " instead of " + std::to_string(expected_sequence_number));
    ++expected_sequence_number;
    offset += frame_header_size;
    require(offset + length <= std::size(frames),
            "a packet claims " + std::to_string(length) + " bytes but only " +
                std::to_string(std::size(frames) - offset) + " follow");
    payload.append(frames, offset, length);
    offset += length;
    if (length < max_packet_payload) {
      break;
    }
  }
  require(offset == std::size(frames),
          std::to_string(std::size(frames) - offset) +
              " unexpected byte(s) after the last packet");
  return payload;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// authentication
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(MinimysqlAcceptsMatchingCredentials) {
  run_property([](hegel::TestCase &tc) {
    const auto server{tc.draw("server", credentials_generator())};
    require(authenticate(server, server),
            "the right credentials were rejected");
  });
}

BOOST_AUTO_TEST_CASE(MinimysqlRejectsOtherCredentials) {
  run_property([](hegel::TestCase &tc) {
    const auto server{tc.draw("server", credentials_generator())};
    const auto client{tc.draw("client", credentials_generator())};
    tc.assume(client.username != server.username ||
              client.password != server.password);
    require(!authenticate(server, client), "other credentials were accepted");
  });
}

BOOST_AUTO_TEST_CASE(MinimysqlRejectsScrambleForAnotherChallenge) {
  run_property([](hegel::TestCase &tc) {
    const auto server{tc.draw("server", credentials_generator())};
    // a reply computed for another challenge (e.g. replayed from an earlier
    // connection) must not be accepted
    const auto other_challenge{tc.draw(
        "other_challenge",
        text_of_size(20U, 20U, 0x7FU))};
    require(!authenticate(server, server, other_challenge),
            "a scramble for another challenge was accepted");
  });
}

// ---------------------------------------------------------------------------
// commands
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(MinimysqlParsesCommands) {
  run_property([](hegel::TestCase &tc) {
    const auto sent{tc.draw("command", commands())};
    auto session{make_authenticated_session()};
    session.context.parse_client_command(
        encode_command(sent, session.capabilities));
    const auto &context{session.context};
    switch (sent.kind) {
    case command_kind::query:
      require(context.get_client_mysql_command() ==
                      minimysql::client_command_type::query &&
                  context.get_client_statement() == sent.statement,
              "query parsed as statement '" + context.get_client_statement() +
                  "'");
      break;
    case command_kind::ping:
      require(context.get_client_mysql_command() ==
                  minimysql::client_command_type::ping,
              "ping parsed as another command");
      break;
    case command_kind::quit:
      require(context.get_client_mysql_command() ==
                  minimysql::client_command_type::quit,
              "quit parsed as another command");
      break;
    case command_kind::binlog_dump:
      require(context.get_client_mysql_command() ==
                  minimysql::client_command_type::binlog_dump,
              "binlog dump parsed as another command");
      require(context.check_binlog_non_blocking_dump() == sent.non_blocking &&
                  context.get_binlog_server_id() == sent.server_id &&
                  context.get_binlog_filename() == sent.filename &&
                  context.get_binlog_position() == sent.position,
              "binlog dump parsed as server_id " +
                  std::to_string(context.get_binlog_server_id()) + ", '" +
                  context.get_binlog_filename() + "':" +
                  std::to_string(context.get_binlog_position()));
      break;
    }
  });
}

// corrupted client packets (greetings and commands) must be rejected with an
// exception, which the session turns into closing the connection
BOOST_AUTO_TEST_CASE(MinimysqlRejectsCorruptedPacketsCleanly) {
  run_property([](hegel::TestCase &tc) {
    const auto corrupt_greeting{tc.draw("corrupt_greeting", gs::booleans())};
    minimysql::connection_context context{"rpl", "password"};
    network_buffer_type packet;
    if (corrupt_greeting) {
      const auto server_greeting{context.generate_encoded_server_greeting()};
      const auto capabilities{context.get_server_capabilities()};
      const auto challenge{decode_challenge(server_greeting, capabilities)};
      const cp::message::client::Greeting client_greeting{
          capabilities,
          static_cast<std::uint32_t>(max_packet_payload),
          0U,
          "rpl",
          client_scramble("password", challenge),
          "",
          std::string{auth_method},
          ""};
      packet = encode_client_frame(1U, client_greeting, capabilities);
    } else {
      auto session{make_authenticated_session()};
      packet = encode_command(tc.draw("command", commands()),
                              session.capabilities);
      context = std::move(session.context);
    }

    // flipped bytes, then optionally a different length
    const auto flips{tc.draw(
        "flips", gs::vectors(gs::tuples(gs::integers<std::size_t>(
                                            {.min_value = 0U,
                                             .max_value = std::size(packet) - 1U}),
                                        gs::integers<std::uint8_t>(
                                            {.min_value = 1U, .max_value = 0xFFU})),
                             {.max_size = 6U}))};
    for (const auto &[position, mask] : flips) {
      packet[position] = static_cast<char>(
          static_cast<std::uint8_t>(packet[position]) ^ mask);
    }
    if (tc.draw("resize", gs::booleans())) {
      packet.resize(tc.draw("new_size", gs::integers<std::size_t>(
                                            {.min_value = 0U,
                                             .max_value = std::size(packet) + 16U})),
                    '\x5A');
    }

    try {
      if (corrupt_greeting) {
        context.parse_client_greeting(packet);
        [[maybe_unused]] const auto accepted{
            context.check_client_authentication()};
      } else {
        context.parse_client_command(packet);
      }
    } catch (const std::exception &) {
      // a clean rejection
    }
  });
}

// ---------------------------------------------------------------------------
// binlog events
// ---------------------------------------------------------------------------

// an event sent to a replica (COM_BINLOG_DUMP) must arrive as an OK byte
// followed by exactly the event bytes, for any event size
BOOST_AUTO_TEST_CASE(MinimysqlBinlogEventFramesRoundTrip) {
  run_property([](hegel::TestCase &tc) {
    // small events, sizes around the single-packet limit, and events that
    // need more than one packet (MySQL events can be up to 1 GiB)
    const auto event_size{tc.draw(
        "event_size",
        gs::one_of({gs::integers<std::size_t>({.min_value = 0U,
                                               .max_value = 2000U}),
                    gs::integers<std::size_t>(
                        {.min_value = max_packet_payload - 3U,
                         .max_value = max_packet_payload + 3U}),
                    gs::integers<std::size_t>(
                        {.min_value = max_packet_payload,
                         .max_value = 2U * max_packet_payload + 10U})}))};
    std::vector<std::byte> event(event_size);
    for (std::size_t index{0U}; index < event_size; ++index) {
      event[index] = static_cast<std::byte>((index * 31U + 7U) & 0xFFU);
    }

    minimysql::connection_context context{"rpl", "password"};
    context.enter_command_loop_iteration();
    network_buffer_type frames;
    try {
      frames = context.generate_encoded_binlog_event(
          util::const_byte_span{event});
    } catch (const std::exception &e) {
      throw std::runtime_error{"an event of " + std::to_string(event_size) +
                               " bytes cannot be encoded: " + e.what()};
    }
    const auto payload{reassemble_payload(frames)};
    require(std::size(payload) == event_size + 1U && payload.front() == '\0',
            "an event of " + std::to_string(event_size) +
                " bytes arrives as a " + std::to_string(std::size(payload)) +
                "-byte payload");
    require(std::equal(std::cbegin(event), std::cend(event),
                        std::next(std::cbegin(payload)),
                        [](std::byte expected, char actual) {
                          return expected ==
                                 static_cast<std::byte>(
                                     static_cast<unsigned char>(actual));
                        }),
            "an event of " + std::to_string(event_size) +
                " bytes arrives with different content");
  });
}
