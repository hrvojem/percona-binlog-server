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

// Property-based tests for opensslpp::cipher_context in CTR mode, built with
// Hegel (https://hegel.dev).
//
// Binlog files are encrypted with an AES-CTR data cipher and are written and
// read in blocks at arbitrary file offsets, using
// cipher_context::create_with_offset() to start the key stream in the middle
// of a file. These properties check that:
// - encrypting from any offset gives the same bytes as an independent
//   reference CTR implementation (AES-ECB of a 128-bit big-endian counter);
// - encrypting data in arbitrary pieces gives the same bytes as encrypting it
//   in one call;
// - decrypting from arbitrary offsets gives back the original data.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#define BOOST_TEST_MODULE CipherContextPropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "property_test_helpers.hpp"

#include "opensslpp/cipher_context.hpp"
#include "opensslpp/cipher_context_fwd.hpp"

#include "util/byte_span_fwd.hpp"

namespace {

namespace gs = hegel::generators;

using property_testing::require;
using property_testing::run_property;

using byte_vector = std::vector<std::byte>;

constexpr std::size_t aes_block_size{16U};
constexpr std::size_t counter_size{sizeof(std::uint64_t)};

[[nodiscard]] byte_vector to_bytes(const std::vector<std::uint8_t> &raw) {
  byte_vector result(std::size(raw));
  std::ranges::transform(raw, std::begin(result),
                         [](std::uint8_t value) { return std::byte{value}; });
  return result;
}

[[nodiscard]] std::string to_hex(const byte_vector &data) {
  static constexpr std::string_view digits{"0123456789abcdef"};
  std::string result;
  for (const auto value : data) {
    const auto byte_value{std::to_integer<unsigned int>(value)};
    result += digits[byte_value >> 4U];
    result += digits[byte_value & 0x0FU];
  }
  return result;
}

// the data ciphers allowed for binlog encryption
[[nodiscard]] gs::Generator<std::string> ctr_ciphers() {
  return gs::sampled_from(std::vector<std::string>{"AES-128-CTR", "AES-192-CTR",
                                                   "AES-256-CTR"});
}

[[nodiscard]] byte_vector draw_key(const hegel::TestCase &tc,
                                   const std::string &cipher) {
  const auto key_size{opensslpp::cipher_context::get_key_size_in_bytes(cipher)};
  return to_bytes(
      tc.draw(gs::binary({.min_size = key_size, .max_size = key_size})));
}

// a 16-byte IV whose last 8 bytes (the part create_with_offset() treats as
// the counter) are drawn as an integer, so that values close to the 64-bit
// limit come up often
[[nodiscard]] byte_vector draw_iv(const hegel::TestCase &tc) {
  auto iv{to_bytes(tc.draw(gs::binary(
      {.min_size = aes_block_size - counter_size,
       .max_size = aes_block_size - counter_size})))};
  const auto counter{tc.draw(gs::one_of(
      {gs::integers<std::uint64_t>(),
       gs::integers<std::uint64_t>(
           {.min_value = std::numeric_limits<std::uint64_t>::max() - 64U,
            .max_value = std::numeric_limits<std::uint64_t>::max()})}))};
  for (std::size_t index{0U}; index < counter_size; ++index) {
    const auto shift{(counter_size - 1U - index) * 8U};
    iv.push_back(static_cast<std::byte>((counter >> shift) & 0xFFU));
  }
  return iv;
}

// reference CTR: key stream block 'i' is AES-ECB(key, IV + i), where IV is a
// 128-bit big-endian counter (this is what OpenSSL's CTR mode does when data
// is encrypted from the start of a stream)
[[nodiscard]] byte_vector reference_encrypt(const std::string &ctr_cipher,
                                            const byte_vector &key,
                                            const byte_vector &iv,
                                            std::uint64_t offset,
                                            const byte_vector &plaintext) {
  std::string ecb_cipher{ctr_cipher};
  ecb_cipher.replace(ecb_cipher.rfind("CTR"), 3U, "ECB");

  byte_vector result(std::size(plaintext));
  for (std::size_t index{0U}; index < std::size(plaintext); ++index) {
    const std::uint64_t position{offset + index};
    // counter block = IV + position / 16 (128-bit, with carry)
    std::array<std::byte, aes_block_size> counter_block{};
    std::uint64_t to_add{position / aes_block_size};
    unsigned int carry{0U};
    for (std::size_t byte_index{aes_block_size}; byte_index-- > 0U;) {
      const auto sum{std::to_integer<unsigned int>(iv[byte_index]) +
                     static_cast<unsigned int>(to_add & 0xFFU) + carry};
      counter_block[byte_index] = static_cast<std::byte>(sum & 0xFFU);
      carry = sum >> 8U;
      to_add >>= 8U;
    }
    std::array<std::byte, aes_block_size> key_stream_block{};
    opensslpp::cipher_context ecb{
        opensslpp::cipher_context_operation_type::encryption, ecb_cipher,
        util::const_byte_span{key}};
    ecb.update(util::const_byte_span{counter_block},
               util::byte_span{key_stream_block});
    result[index] = plaintext[index] ^ key_stream_block[position % aes_block_size];
  }
  return result;
}

[[nodiscard]] byte_vector
process_at_offset(opensslpp::cipher_context_operation_type operation,
                  const std::string &cipher, const byte_vector &key,
                  const byte_vector &iv, std::uint64_t offset,
                  const byte_vector &input) {
  auto context{opensslpp::cipher_context::create_with_offset(
      offset, operation, cipher, util::const_byte_span{key},
      util::const_byte_span{iv})};
  byte_vector output(std::size(input));
  context.update(util::const_byte_span{input}, util::byte_span{output});
  return output;
}

// splits [0, size) at the given points
[[nodiscard]] std::vector<std::pair<std::size_t, std::size_t>>
split_ranges(std::size_t size, const std::vector<std::size_t> &points) {
  std::set<std::size_t> cuts{0U, size};
  for (const auto point : points) {
    cuts.insert(std::min(point, size));
  }
  std::vector<std::pair<std::size_t, std::size_t>> result;
  for (auto it{std::cbegin(cuts)}; std::next(it) != std::cend(cuts); ++it) {
    result.emplace_back(*it, *std::next(it));
  }
  return result;
}

constexpr std::size_t max_data_size{300U};

[[nodiscard]] gs::Generator<std::vector<std::uint8_t>> data() {
  return gs::binary({.min_size = 0U, .max_size = max_data_size});
}

} // anonymous namespace

BOOST_AUTO_TEST_CASE(CipherContextOffsetMatchesCounterReference) {
  run_property([](hegel::TestCase &tc) {
    const auto cipher{tc.draw("cipher", ctr_ciphers())};
    const auto key{draw_key(tc, cipher)};
    const auto iv{draw_iv(tc)};
    // offsets up to 2^40 (1 TiB), well beyond any binlog file
    const auto offset{tc.draw(
        "offset", gs::integers<std::uint64_t>(
                      {.min_value = 0U, .max_value = 1ULL << 40U}))};
    const auto plaintext{to_bytes(tc.draw("plaintext", data()))};
    tc.note("iv = " + to_hex(iv));

    const auto actual{process_at_offset(
        opensslpp::cipher_context_operation_type::encryption, cipher, key, iv,
        offset, plaintext)};
    const auto expected{reference_encrypt(cipher, key, iv, offset, plaintext)};
    require(actual == expected,
            "create_with_offset(" + std::to_string(offset) +
                ") encryption differs from the reference: " + to_hex(actual) +
                " instead of " + to_hex(expected));
  });
}

BOOST_AUTO_TEST_CASE(CipherContextSplitEncryptionMatchesWhole) {
  run_property([](hegel::TestCase &tc) {
    const auto cipher{tc.draw("cipher", ctr_ciphers())};
    const auto key{draw_key(tc, cipher)};
    const auto iv{draw_iv(tc)};
    const auto plaintext{to_bytes(tc.draw("plaintext", data()))};
    const auto points{tc.draw(
        "split_points",
        gs::vectors(gs::integers<std::size_t>(
            {.min_value = 0U, .max_value = max_data_size})))};
    tc.note("iv = " + to_hex(iv));

    const auto whole{process_at_offset(
        opensslpp::cipher_context_operation_type::encryption, cipher, key, iv,
        0U, plaintext)};
    byte_vector pieces;
    for (const auto &[begin, end] : split_ranges(std::size(plaintext), points)) {
      const byte_vector chunk(
          std::next(std::cbegin(plaintext), static_cast<std::ptrdiff_t>(begin)),
          std::next(std::cbegin(plaintext), static_cast<std::ptrdiff_t>(end)));
      const auto encrypted{process_at_offset(
          opensslpp::cipher_context_operation_type::encryption, cipher, key,
          iv, begin, chunk)};
      pieces.insert(std::end(pieces), std::cbegin(encrypted),
                    std::cend(encrypted));
    }
    require(pieces == whole,
            "encrypting in pieces gives " + to_hex(pieces) + " instead of " +
                to_hex(whole));
  });
}

BOOST_AUTO_TEST_CASE(CipherContextDecryptionAtOffsetsRoundTrips) {
  run_property([](hegel::TestCase &tc) {
    const auto cipher{tc.draw("cipher", ctr_ciphers())};
    const auto key{draw_key(tc, cipher)};
    const auto iv{draw_iv(tc)};
    const auto plaintext{to_bytes(tc.draw("plaintext", data()))};
    const auto points{tc.draw(
        "split_points",
        gs::vectors(gs::integers<std::size_t>(
            {.min_value = 0U, .max_value = max_data_size})))};
    tc.note("iv = " + to_hex(iv));

    const auto ciphertext{process_at_offset(
        opensslpp::cipher_context_operation_type::encryption, cipher, key, iv,
        0U, plaintext)};
    byte_vector decrypted;
    for (const auto &[begin, end] :
         split_ranges(std::size(ciphertext), points)) {
      const byte_vector chunk(
          std::next(std::cbegin(ciphertext), static_cast<std::ptrdiff_t>(begin)),
          std::next(std::cbegin(ciphertext), static_cast<std::ptrdiff_t>(end)));
      const auto plain_chunk{process_at_offset(
          opensslpp::cipher_context_operation_type::decryption, cipher, key,
          iv, begin, chunk)};
      decrypted.insert(std::end(decrypted), std::cbegin(plain_chunk),
                       std::cend(plain_chunk));
    }
    require(decrypted == plaintext,
            "decrypting in pieces gives " + to_hex(decrypted) +
                " instead of " + to_hex(plaintext));
  });
}
