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

// Property-based tests for the parsers of values that come from users (the
// configuration file and the command line) or from servers, built with Hegel
// (https://hegel.dev):
// - binsrv::size_unit ("checkpoint_size", "file_size": "512M") and
//   binsrv::time_unit ("checkpoint_interval", "connect_timeout": "30s")
//   accept exactly <digits>[<unit>] whose value fits in 64 bits, with the
//   value an independent model computes, and print back to the same value;
// - binsrv::events::composite_binlog_name ("binlog.000042") accepts exactly
//   the canonical names, prints them back unchanged and never wraps around;
// - util::ctime_timestamp (the "search_by_timestamp" argument) accepts
//   exactly the valid "YYYY-MM-DDTHH:MM:SS[.fraction]" timestamps, with the
//   value an independent calendar model computes;
// - util::semantic_version (the server version from FORMAT_DESCRIPTION
//   events) accepts exactly "<major>.<minor>.<patch>[-<extra>]" with 8-bit
//   components;
// - the filesystem storage backend accepts exactly "file://<path>" URIs of
//   existing directories, and object URIs point inside that directory;
// - storage_config::get_masked_uri() hides credentials and keeps the rest;
// - the S3 storage backend accepts exactly the valid "http[s]://" endpoint
//   URIs and "s3://<bucket>.<region>" URIs, and uses the endpoint or region,
//   bucket, path and credentials they name; credentials written with any
//   valid percent-encoding authenticate against an S3-compatible server
//   (when one is given through PBS_TEST_S3_ENDPOINT, PBS_TEST_S3_ACCESS_KEY,
//   PBS_TEST_S3_SECRET_KEY and PBS_TEST_S3_BUCKET);
// - a keyring file is accepted exactly when it has the supported version,
//   unique key IDs, supported ciphers and keys of the right length, and then
//   holds exactly the keys written to it;
// - a configuration file is accepted exactly when its sections pass the
//   documented checks, and then holds exactly the values written to it;
// - damaged keyring and configuration files (truncated, a byte changed, a
//   part replaced with a JSON value of another type) are rejected with a
//   regular exception, never with a crash or an allocation failure.

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <new>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>

#include <boost/lexical_cast.hpp>

#include <boost/url/parse.hpp>
#include <boost/url/scheme.hpp>
#include <boost/url/url.hpp>

#define BOOST_TEST_MODULE ConfigValuePropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "property_test_helpers.hpp"

#include "binsrv/filesystem_storage_backend.hpp"
#include "binsrv/keyring_record.hpp"
#include "binsrv/keyring_record_collection.hpp"
#include "binsrv/log_severity.hpp"
#include "binsrv/main_config.hpp"
#include "binsrv/s3_storage_backend.hpp"
#include "binsrv/size_unit.hpp"
#include "binsrv/storage_backend_type.hpp"
#include "binsrv/storage_config.hpp"
#include "binsrv/time_unit.hpp"

#include "binsrv/events/composite_binlog_name.hpp"

#include "util/ctime_timestamp.hpp"
#include "util/semantic_version.hpp"

namespace {

namespace gs = hegel::generators;

using property_testing::require;
using property_testing::run_property;

[[nodiscard]] gs::Generator<std::string>
text_of_size(std::size_t min_size, std::size_t max_size,
             std::optional<std::uint32_t> max_codepoint = std::nullopt) {
  gs::TextParams params{};
  params.min_size = min_size;
  params.max_size = max_size;
  params.max_codepoint = max_codepoint;
  return gs::text(params);
}

[[nodiscard]] bool is_digit(char character) noexcept {
  return character >= '0' && character <= '9';
}

// the decimal value of a string of digits, if it fits in 64 bits
[[nodiscard]] std::optional<std::uint64_t>
model_decimal(std::string_view digits) {
  std::uint64_t result{0U};
  for (const char digit : digits) {
    if (!is_digit(digit) ||
        __builtin_mul_overflow(result, std::uint64_t{10U}, &result) ||
        __builtin_add_overflow(result,
                               static_cast<std::uint64_t>(digit - '0'),
                               &result)) {
      return std::nullopt;
    }
  }
  return result;
}

// ---------------------------------------------------------------------------
// size_unit and time_unit
// ---------------------------------------------------------------------------

struct unit_symbol {
  char symbol;
  std::uint64_t multiplier;
};

constexpr std::array<unit_symbol, 5> size_symbols{
    {{.symbol = 'K', .multiplier = 1ULL << 10U},
     {.symbol = 'M', .multiplier = 1ULL << 20U},
     {.symbol = 'G', .multiplier = 1ULL << 30U},
     {.symbol = 'T', .multiplier = 1ULL << 40U},
     {.symbol = 'P', .multiplier = 1ULL << 50U}}};

constexpr std::array<unit_symbol, 4> time_symbols{
    {{.symbol = 's', .multiplier = 1ULL},
     {.symbol = 'm', .multiplier = 60ULL},
     {.symbol = 'h', .multiplier = 60ULL * 60ULL},
     {.symbol = 'd', .multiplier = 60ULL * 60ULL * 24ULL}}};

// the value of "<digits>[<symbol>]", or nothing when the text is not of that
// form or the value does not fit in 64 bits
template <std::size_t N>
[[nodiscard]] std::optional<std::uint64_t>
model_unit(std::string_view text, const std::array<unit_symbol, N> &symbols) {
  std::size_t digits{0U};
  while (digits < std::size(text) && is_digit(text[digits])) {
    ++digits;
  }
  if (digits == 0U) {
    return std::nullopt;
  }
  const auto base{model_decimal(text.substr(0U, digits))};
  if (!base.has_value()) {
    return std::nullopt;
  }
  const auto suffix{text.substr(digits)};
  if (suffix.empty()) {
    return base;
  }
  if (std::size(suffix) != 1U) {
    return std::nullopt;
  }
  for (const auto &[symbol, multiplier] : symbols) {
    if (symbol == suffix.front()) {
      std::uint64_t value{};
      if (__builtin_mul_overflow(*base, multiplier, &value)) {
        return std::nullopt;
      }
      return value;
    }
  }
  return std::nullopt;
}

// unit texts: mostly well-formed, with numbers of every magnitude (including
// ones near and beyond 64 bits) and both known and unknown unit symbols
[[nodiscard]] gs::Generator<std::string> unit_texts(std::string_view symbols) {
  const std::string symbol_choices{symbols};
  return gs::one_of(
      {gs::compose([symbol_choices](const hegel::TestCase &tc) {
         std::string result{tc.draw(gs::one_of(
             {gs::integers<std::uint64_t>().map(
                  [](std::uint64_t value) { return std::to_string(value); }),
              gs::from_regex("[0-9]{1,24}", true)}))};
         // nothing, a known symbol, or an unknown one
         const auto suffix{tc.draw(gs::integers<std::size_t>(
             {.min_value = 0U, .max_value = 2U}))};
         if (suffix == 1U) {
           result += symbol_choices[tc.draw(gs::integers<std::size_t>(
               {.min_value = 0U,
                .max_value = std::size(symbol_choices) - 1U}))];
         } else if (suffix == 2U) {
           result += tc.draw(text_of_size(1U, 2U, 0x7FU));
         }
         return result;
       }),
       gs::from_regex("[-+ ]?[0-9]{0,3}[ _.,]?[0-9]{0,3}[KMGTPkmgtpsdh_ B]{0,2}",
                      true),
       text_of_size(0U, 8U)});
}

template <typename Unit, std::size_t N>
void check_unit(std::string_view text,
                const std::array<unit_symbol, N> &symbols) {
  const auto expected{model_unit(text, symbols)};
  std::optional<Unit> parsed;
  try {
    parsed.emplace(text);
  } catch (const std::logic_error &) {
    // std::invalid_argument and std::out_of_range
  } catch (const std::exception &e) {
    throw std::runtime_error{"'" + std::string{text} +
                             "' was rejected with an unexpected exception: " +
                             e.what()};
  }
  if (!expected.has_value()) {
    require(!parsed.has_value(),
            "'" + std::string{text} + "' was accepted as " +
                std::to_string(parsed->get_value()));
    return;
  }
  require(parsed.has_value(), "'" + std::string{text} +
                                  "' was rejected, expected " +
                                  std::to_string(*expected));
  require(parsed->get_value() == *expected,
          "'" + std::string{text} + "' was parsed as " +
              std::to_string(parsed->get_value()) + ", expected " +
              std::to_string(*expected));
  const auto printed{parsed->to_string()};
  const Unit reparsed{printed};
  require(reparsed.get_value() == *expected,
          "'" + std::string{text} + "' printed as '" + printed +
              "' which parses as " + std::to_string(reparsed.get_value()));
  [[maybe_unused]] const auto description{parsed->get_description()};
}

// ---------------------------------------------------------------------------
// timestamps
// ---------------------------------------------------------------------------

[[nodiscard]] bool is_leap_year(std::int64_t year) noexcept {
  return (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
}

[[nodiscard]] std::int64_t days_in_month(std::int64_t year,
                                         std::int64_t month) noexcept {
  static constexpr std::array<std::int64_t, 12> days{31, 28, 31, 30, 31, 30,
                                                     31, 31, 30, 31, 30, 31};
  return month == 2 && is_leap_year(year)
             ? 29
             : days[static_cast<std::size_t>(month - 1)];
}

// days since 1970-01-01 of a proleptic Gregorian date
[[nodiscard]] std::int64_t days_from_civil(std::int64_t year,
                                           std::int64_t month,
                                           std::int64_t day) noexcept {
  std::int64_t days{0};
  for (std::int64_t current{1970}; current < year; ++current) {
    days += is_leap_year(current) ? 366 : 365;
  }
  for (std::int64_t current{year}; current < 1970; ++current) {
    days -= is_leap_year(current) ? 366 : 365;
  }
  for (std::int64_t current{1}; current < month; ++current) {
    days += days_in_month(year, current);
  }
  return days + day - 1;
}

// the range of dates Boost.Date_Time supports
constexpr std::int64_t min_year{1400};
constexpr std::int64_t max_year{9999};

struct timestamp_fields {
  std::int64_t year;
  std::int64_t month;
  std::int64_t day;
  std::int64_t hour;
  std::int64_t minute;
  std::int64_t second;
  std::string fraction;
};

[[nodiscard]] std::string two_digits(std::int64_t value) {
  return (value < 10 ? "0" : "") + std::to_string(value);
}

[[nodiscard]] std::string format_timestamp(const timestamp_fields &fields) {
  std::string year{std::to_string(fields.year)};
  year.insert(0U, std::size(year) < 4U ? 4U - std::size(year) : 0U, '0');
  return year + '-' + two_digits(fields.month) + '-' + two_digits(fields.day) +
         'T' + two_digits(fields.hour) + ':' + two_digits(fields.minute) +
         ':' + two_digits(fields.second) + fields.fraction;
}

[[nodiscard]] std::optional<std::time_t>
model_timestamp(const timestamp_fields &fields) {
  if (fields.year < min_year || fields.year > max_year || fields.month < 1 ||
      fields.month > 12 || fields.day < 1 ||
      fields.day > days_in_month(fields.year, fields.month) ||
      fields.hour > 23 || fields.minute > 59 || fields.second > 59) {
    return std::nullopt;
  }
  static constexpr std::int64_t seconds_per_day{86400};
  // std::int64_t and std::time_t are different types on some platforms
  const std::time_t result =
      days_from_civil(fields.year, fields.month, fields.day) *
          seconds_per_day +
      fields.hour * 3600 + fields.minute * 60 + fields.second;
  return result;
}

[[nodiscard]] gs::Generator<timestamp_fields> timestamp_field_values() {
  return gs::compose([](const hegel::TestCase &tc) {
    const auto field{[&tc](std::int64_t min_value, std::int64_t max_value) {
      return tc.draw(gs::integers<std::int64_t>(
          {.min_value = min_value, .max_value = max_value}));
    }};
    // mostly valid values, sometimes just outside the valid range
    const bool valid{tc.draw(gs::integers<int>({.min_value = 0,
                                                .max_value = 3})) != 0};
    timestamp_fields result{};
    result.year = valid ? field(1970, 2100) : field(1000, 9999);
    result.month = valid ? field(1, 12) : field(0, 13);
    result.day = valid ? field(1, days_in_month(result.year, result.month))
                       : field(0, 32);
    result.hour = valid ? field(0, 23) : field(0, 99);
    result.minute = valid ? field(0, 59) : field(0, 99);
    result.second = valid ? field(0, 59) : field(0, 99);
    result.fraction = tc.draw(gs::from_regex("(\\.[0-9]{1,6})?", true));
    return result;
  });
}

[[nodiscard]] std::time_t timestamp_value(std::int64_t year) {
  return *model_timestamp({.year = year,
                           .month = year == min_year ? 1 : 12,
                           .day = year == min_year ? 1 : 31,
                           .hour = year == min_year ? 0 : 23,
                           .minute = year == min_year ? 0 : 59,
                           .second = year == min_year ? 0 : 59,
                           .fraction = {}});
}

// ---------------------------------------------------------------------------
// binlog names and server versions
// ---------------------------------------------------------------------------

constexpr std::uint32_t max_binlog_sequence_number{999999U};

// whether "<base>.<6 digits>" is the canonical name of a binlog, according
// to the documented format
[[nodiscard]] bool model_binlog_name(std::string_view text) {
  static constexpr std::size_t digits{6U};
  if (std::size(text) <= digits + 1U) {
    return false;
  }
  const auto separator{std::size(text) - digits - 1U};
  if (text[separator] != '.') {
    return false;
  }
  const auto number{model_decimal(text.substr(separator + 1U))};
  const auto base{text.substr(0U, separator)};
  return number.has_value() && *number != 0U &&
         base.find('/') == std::string_view::npos;
}

// the components of "<major>.<minor>.<patch>[-<extra>]", or nothing
[[nodiscard]] std::optional<std::array<std::uint8_t, 3>>
model_version(std::string_view text) {
  if (const auto dash{text.find('-')}; dash != std::string_view::npos) {
    text = text.substr(0U, dash);
  }
  std::array<std::uint8_t, 3> result{};
  for (std::size_t index{0U}; index < std::size(result); ++index) {
    const auto dot{text.find('.')};
    const auto component{text.substr(0U, dot)};
    const auto value{model_decimal(component)};
    if (component.empty() || !value.has_value() ||
        *value > std::numeric_limits<std::uint8_t>::max()) {
      return std::nullopt;
    }
    result.at(index) = static_cast<std::uint8_t>(*value);
    if (index + 1U == std::size(result)) {
      if (dot != std::string_view::npos) {
        return std::nullopt;
      }
    } else {
      if (dot == std::string_view::npos) {
        return std::nullopt;
      }
      text = text.substr(dot + 1U);
    }
  }
  return result;
}

// ---------------------------------------------------------------------------
// storage URIs
// ---------------------------------------------------------------------------

// a fresh directory for one test case, removed afterwards
class scratch_directory {
public:
  scratch_directory() {
    std::random_device device;
    path_ = std::filesystem::temp_directory_path() /
            ("pbs-uri-test-" + std::to_string(device()) + '-' +
             std::to_string(device()));
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
};

[[nodiscard]] binsrv::storage_config make_storage_config(std::string uri) {
  binsrv::storage_config config{};
  config.get<"backend">() = binsrv::storage_backend_type::file;
  config.get<"uri">() = std::move(uri);
  return config;
}


// ---------------------------------------------------------------------------
// keyring and configuration files
// ---------------------------------------------------------------------------

void write_file(const std::filesystem::path &path, std::string_view content) {
  std::ofstream output{path, std::ios::binary | std::ios::trunc};
  output.write(std::data(content),
               static_cast<std::streamsize>(std::size(content)));
  if (!output) {
    throw std::runtime_error{"cannot write '" + path.string() + "'"};
  }
}

// runs a loader, turning a regular exception into a rejection (its message
// is returned); anything else fails the property
[[nodiscard]] std::optional<std::string>
load_cleanly(const std::function<void()> &loader) {
  try {
    loader();
  } catch (const std::bad_alloc &e) {
    throw std::runtime_error{std::string{"rejected with std::bad_alloc: "} +
                             e.what()};
  } catch (const std::exception &e) {
    const std::string message{e.what()};
    if (message.find("bad_alloc") != std::string::npos) {
      throw std::runtime_error{"rejected with an allocation failure: " +
                               message};
    }
    return message;
  } catch (...) {
    throw std::runtime_error{"rejected with a non-standard exception"};
  }
  return std::nullopt;
}

// damages a text: truncation, a changed byte, or a part replaced with a JSON
// value of another type
[[nodiscard]] std::string damage_text(const hegel::TestCase &tc,
                                      std::string text) {
  const auto kind{
      tc.draw(gs::integers<int>({.min_value = 0, .max_value = 2}))};
  const auto position{tc.draw(gs::integers<std::size_t>(
      {.min_value = 0U, .max_value = std::size(text)}))};
  if (kind == 0) {
    text.resize(position);
  } else if (kind == 1) {
    if (position < std::size(text)) {
      text[position] = static_cast<char>(
          static_cast<unsigned char>(text[position]) ^
          tc.draw(gs::integers<std::uint8_t>(
              {.min_value = 1U, .max_value = 255U})));
    }
  } else {
    const auto length{tc.draw(gs::integers<std::size_t>(
        {.min_value = 0U, .max_value = std::size(text) - position}))};
    text.replace(position, length,
                 tc.draw(gs::sampled_from<std::string>(
                     {"", "null", "-1", "0", "1.5", "1e999",
                      "18446744073709551616", "\"\"", "\"x\"", "[]", "{}",
                      "true", "\"ZZ\"", "\"0\"", "[[[[[[[[[[[[[[[[[[[["})));
  }
  return text;
}

struct cipher_choice {
  std::string_view name;
  // the key length, for ciphers the keyring supports
  std::optional<std::size_t> key_size;
};

constexpr std::array<cipher_choice, 11> keyring_ciphers{
    {{.name = "AES-128-ECB", .key_size = 16U},
     {.name = "AES-256-ECB", .key_size = 32U},
     {.name = "AES-192-CBC", .key_size = 24U},
     {.name = "AES-128-CTR", .key_size = 16U},
     {.name = "AES-256-CTR", .key_size = 32U},
     {.name = "AES-192-GCM", .key_size = 24U},
     // a cipher OpenSSL knows in a mode the keyring does not support, and
     // unknown names
     {.name = "AES-128-OFB", .key_size = std::nullopt},
     {.name = "AES-512-CTR", .key_size = std::nullopt},
     {.name = "AES-128", .key_size = std::nullopt},
     {.name = "", .key_size = std::nullopt},
     {.name = "AES-128-CTR ", .key_size = std::nullopt}}};

struct keyring_key {
  std::string id;
  std::size_t cipher;
  std::vector<std::uint8_t> data;
  // the hex text written to the file (normally the hex of 'data')
  std::string data_hex;
  bool valid_hex;
};

struct keyring_spec {
  std::uint32_t version;
  std::vector<keyring_key> keys;
};

[[nodiscard]] std::string to_hex(const std::vector<std::uint8_t> &data,
                                 bool lowercase) {
  static constexpr std::string_view upper{"0123456789ABCDEF"};
  static constexpr std::string_view lower{"0123456789abcdef"};
  const auto digits{lowercase ? lower : upper};
  std::string result;
  for (const auto byte : data) {
    result += digits[byte >> 4U];
    result += digits[byte & 0x0FU];
  }
  return result;
}

// a valid keyring with at most one defect, so that every check is exercised
// on its own (with several defects, any one check would reject the file)
[[nodiscard]] gs::Generator<keyring_spec> keyring_specs() {
  return gs::compose([](const hegel::TestCase &tc) {
    keyring_spec spec{.version = 1U, .keys = {}};
    static constexpr std::size_t supported_ciphers{6U};
    const auto count{tc.draw(
        gs::integers<std::size_t>({.min_value = 0U, .max_value = 5U}))};
    for (std::size_t index{0U}; index < count; ++index) {
      keyring_key key{};
      key.id = tc.draw(gs::one_of({gs::sampled_from<std::string>(
                                       {"alpha", "beta", "gamma", ""}),
                                   text_of_size(1U, 8U)}));
      while (std::ranges::find(spec.keys, key.id, &keyring_key::id) !=
             std::cend(spec.keys)) {
        key.id += '#';
      }
      key.cipher = tc.draw(gs::integers<std::size_t>(
          {.min_value = 0U, .max_value = supported_ciphers - 1U}));
      const auto size{*keyring_ciphers.at(key.cipher).key_size};
      for (std::size_t byte{0U}; byte < size; ++byte) {
        key.data.push_back(tc.draw(gs::integers<std::uint8_t>()));
      }
      key.data_hex = to_hex(key.data, tc.draw(gs::booleans()));
      key.valid_hex = true;
      spec.keys.push_back(std::move(key));
    }

    if (tc.draw(gs::booleans())) {
      return spec;
    }
    const auto defect{
        tc.draw(gs::integers<int>({.min_value = 0, .max_value = 5}))};
    if (defect == 0 || spec.keys.empty()) {
      spec.version = tc.draw(gs::integers<std::uint32_t>());
      return spec;
    }
    auto &key{spec.keys[tc.draw(gs::integers<std::size_t>(
        {.min_value = 0U, .max_value = std::size(spec.keys) - 1U}))]};
    switch (defect) {
    case 1:
      // the ID of another key (or of itself, when there is only one key)
      spec.keys.push_back(key);
      break;
    case 2:
      key.cipher = tc.draw(gs::integers<std::size_t>(
          {.min_value = supported_ciphers,
           .max_value = std::size(keyring_ciphers) - 1U}));
      break;
    case 3: {
      const auto size{tc.draw(
          gs::integers<std::size_t>({.min_value = 0U, .max_value = 40U}))};
      key.data.resize(size, std::uint8_t{0x5AU});
      key.data_hex = to_hex(key.data, false);
      break;
    }
    case 4:
      key.data_hex += '0';
      key.valid_hex = false;
      break;
    default:
      if (key.data_hex.empty()) {
        key.data_hex = "zz";
      } else {
        key.data_hex[tc.draw(gs::integers<std::size_t>(
            {.min_value = 0U, .max_value = std::size(key.data_hex) - 1U}))] =
            tc.draw(gs::sampled_from<char>({'g', 'G', 'x', ' ', '-', '\0',
                                            '+'}));
      }
      key.valid_hex = false;
      break;
    }
    return spec;
  });
}

[[nodiscard]] std::string keyring_json(const keyring_spec &spec) {
  boost::json::array keys;
  for (const auto &key : spec.keys) {
    keys.push_back(boost::json::object{
        {"id", key.id},
        {"cipher", keyring_ciphers.at(key.cipher).name},
        {"data_hex", key.data_hex}});
  }
  return boost::json::serialize(boost::json::object{
      {"version", spec.version}, {"keys", std::move(keys)}});
}

[[nodiscard]] bool model_keyring(const keyring_spec &spec) {
  if (spec.version != 1U) {
    return false;
  }
  std::vector<std::string> ids;
  for (const auto &key : spec.keys) {
    if (std::ranges::find(ids, key.id) != std::cend(ids)) {
      return false;
    }
    ids.push_back(key.id);
    const auto &cipher{keyring_ciphers.at(key.cipher)};
    if (!key.valid_hex || !cipher.key_size.has_value() ||
        std::size(key.data) != *cipher.key_size) {
      return false;
    }
  }
  return true;
}

// a configuration file as a JSON object, with the outcome the documented
// checks give for it
struct config_spec {
  boost::json::object json;
  bool valid;
  std::string description;
};

// a valid configuration with at most one defect, so that every check is
// exercised on its own
[[nodiscard]] gs::Generator<config_spec> config_specs() {
  return gs::compose([](const hegel::TestCase &tc) {
    static const std::vector<std::string> defects{
        "log_level",           "connection_endpoint",
        "connection_port",     "connection_timeout",
        "server_id",           "mode",
        "rewrite_mode",        "rewrite_file_size_unit",
        "rewrite_file_size",   "source_port_zero",
        "source_port_range",   "source_read_timeout",
        "source_write_timeout", "authentication_user",
        "authentication_password", "authentication_plugin",
        "backend",             "checkpoint_size",
        "checkpoint_interval", "encryption_format",
        "encryption_cipher",   "missing_field"};
    std::string defect;
    if (!tc.draw(gs::booleans())) {
      defect = tc.draw(gs::sampled_from(defects));
    }
    config_spec spec{
        .json = {}, .valid = defect.empty(), .description = defect};
    const auto is_defect{[&defect](std::string_view name) {
      return defect == name;
    }};
    const auto choose{[&tc](std::initializer_list<std::string> values) {
      return tc.draw(gs::sampled_from(std::vector<std::string>{values}));
    }};
    const auto valid_uint32{[&tc] {
      return static_cast<std::int64_t>(tc.draw(gs::integers<std::uint32_t>(
          {.min_value = 1U,
           .max_value = std::numeric_limits<std::uint32_t>::max()})));
    }};
    const auto out_of_range{[&tc](std::int64_t max_value) {
      return tc.draw(gs::sampled_from<std::int64_t>(
          {-1, max_value + 1, std::numeric_limits<std::int64_t>::min()}));
    }};
    // unit texts that are valid, or not, for the given symbols
    const auto unit_text{[&tc](std::string_view symbols, bool valid,
                               const auto &symbol_table) {
      return tc.draw(unit_texts(symbols).filter(
          [valid, &symbol_table](const std::string &text) {
            return model_unit(text, symbol_table).has_value() == valid;
          }));
    }};
    static constexpr auto uint16_max{
        static_cast<std::int64_t>(std::numeric_limits<std::uint16_t>::max())};
    static constexpr auto uint32_max{
        static_cast<std::int64_t>(std::numeric_limits<std::uint32_t>::max())};

    // logger
    spec.json["logger"] = boost::json::object{
        {"level", is_defect("log_level")
                      ? choose({"verbose", "", "ERROR", "error "})
                      : choose({"trace", "debug", "info", "warning", "error",
                                "fatal"})},
        {"file", ""}};

    // connection: either a DNS SRV name, or both host and port
    boost::json::object connection;
    const bool use_dns{tc.draw(gs::booleans())};
    bool has_host{!use_dns};
    bool has_port{!use_dns};
    bool has_dns{use_dns};
    if (is_defect("connection_endpoint")) {
      const auto broken{tc.draw(gs::sampled_from<std::string>(
          {"nothing", "host only", "port only", "dns and host",
           "dns and port", "all"}))};
      spec.description += " (" + broken + ")";
      has_host = broken == "host only" || broken == "dns and host" ||
                 broken == "all";
      has_port = broken == "port only" || broken == "dns and port" ||
                 broken == "all";
      has_dns = broken.starts_with("dns") || broken == "all";
    }
    if (has_host) {
      connection["host"] = tc.draw(text_of_size(0U, 12U));
    }
    if (has_port) {
      connection["port"] =
          is_defect("connection_port")
              ? out_of_range(uint16_max)
              : static_cast<std::int64_t>(tc.draw(gs::integers<std::uint16_t>()));
    }
    if (has_dns) {
      connection["dns_srv_name"] = tc.draw(text_of_size(0U, 12U));
    }
    if (is_defect("connection_port") && !has_port) {
      // nothing to put out of range
      spec.valid = true;
    }
    connection["user"] = tc.draw(text_of_size(0U, 8U));
    connection["password"] = tc.draw(text_of_size(0U, 8U));
    connection["connect_timeout"] = is_defect("connection_timeout")
                                        ? out_of_range(uint32_max)
                                        : valid_uint32();
    connection["read_timeout"] = valid_uint32();
    connection["write_timeout"] = valid_uint32();
    spec.json["connection"] = std::move(connection);

    // replication
    boost::json::object replication;
    replication["server_id"] =
        is_defect("server_id") ? out_of_range(uint32_max) : valid_uint32();
    replication["idle_time"] = valid_uint32();
    replication["verify_checksum"] = tc.draw(gs::booleans());
    const bool with_rewrite{is_defect("rewrite_mode") ||
                            is_defect("rewrite_file_size_unit") ||
                            is_defect("rewrite_file_size") ||
                            tc.draw(gs::integers<int>(
                                {.min_value = 0, .max_value = 3})) == 0};
    std::string mode{with_rewrite ? "gtid" : choose({"position", "gtid"})};
    if (is_defect("mode")) {
      mode = choose({"row", "", "GTID"});
    } else if (is_defect("rewrite_mode")) {
      mode = "position";
    }
    replication["mode"] = mode;
    if (with_rewrite) {
      std::string file_size;
      if (is_defect("rewrite_file_size_unit")) {
        file_size = unit_text("KMGTP_kB", false, size_symbols);
      } else if (is_defect("rewrite_file_size")) {
        file_size = std::to_string(tc.draw(gs::integers<std::uint32_t>(
            {.min_value = 0U, .max_value = 1023U})));
      } else {
        file_size = tc.draw(gs::sampled_from<std::string>(
            {"1024", "1K", "16M", "1G", "4096"}));
      }
      replication["rewrite"] = boost::json::object{
          {"base_file_name", "rewritten"}, {"file_size", file_size}};
    }
    spec.json["replication"] = std::move(replication);

    // replication source
    boost::json::object source;
    if (is_defect("source_port_zero")) {
      source["port"] = 0;
    } else if (is_defect("source_port_range")) {
      source["port"] = out_of_range(uint16_max);
    } else {
      source["port"] = static_cast<std::int64_t>(tc.draw(
          gs::integers<std::uint16_t>({.min_value = 1U, .max_value = 65535U})));
    }
    source["read_timeout"] = is_defect("source_read_timeout")
                                 ? std::int64_t{0}
                                 : valid_uint32();
    source["write_timeout"] = is_defect("source_write_timeout")
                                  ? std::int64_t{0}
                                  : valid_uint32();
    source["authentication"] = boost::json::object{
        {"user", is_defect("authentication_user") ? std::string{}
                                                  : choose({"rpl", "replica"})},
        {"password", is_defect("authentication_password")
                         ? std::string{}
                         : choose({"password", "p@ss\"word\\"})},
        {"plugin", is_defect("authentication_plugin")
                       ? choose({"mysql_native_password", "",
                                 "caching_sha2_password "})
                       : std::string{"caching_sha2_password"}}};
    spec.json["replication_source"] = std::move(source);

    // keyring (optional, its file is not read when the configuration is
    // loaded)
    if (tc.draw(gs::booleans())) {
      spec.json["keyring"] =
          boost::json::object{{"uri", "file:///nonexistent/keyring.json"}};
    }

    // storage
    boost::json::object storage;
    storage["backend"] = is_defect("backend") ? choose({"nfs", "", "FILE"})
                                              : choose({"file", "s3"});
    storage["uri"] = tc.draw(text_of_size(0U, 16U));
    if (is_defect("checkpoint_size") || tc.draw(gs::booleans())) {
      storage["checkpoint_size"] = unit_text(
          "KMGTP_kB", !is_defect("checkpoint_size"), size_symbols);
    }
    if (is_defect("checkpoint_interval") || tc.draw(gs::booleans())) {
      storage["checkpoint_interval"] = unit_text(
          "smhdSMHDw", !is_defect("checkpoint_interval"), time_symbols);
    }
    if (is_defect("encryption_format") || is_defect("encryption_cipher") ||
        tc.draw(gs::booleans())) {
      storage["encryption"] = boost::json::object{
          {"format", is_defect("encryption_format") ? choose({"aes", ""})
                                                    : std::string{"generic"}},
          {"kek_id", "alpha"},
          {"cipher", is_defect("encryption_cipher")
                         ? choose({"AES-256-CBC", "AES-128-GCM", "AES-512-CTR",
                                   ""})
                         : choose({"AES-128-CTR", "AES-192-CTR",
                                   "AES-256-CTR"})}};
    }
    spec.json["storage"] = std::move(storage);

    if (is_defect("missing_field")) {
      const auto [section, field]{
          tc.draw(gs::sampled_from<std::pair<std::string, std::string>>(
              {{"logger", "level"},
               {"connection", "user"},
               {"replication", "server_id"},
               {"replication", "mode"},
               {"replication_source", "authentication"},
               {"storage", "uri"},
               {"storage", ""},
               {"logger", ""}}))};
      if (field.empty()) {
        spec.json.erase(section);
      } else {
        spec.json[section].as_object().erase(field);
      }
      spec.description += " (" + section + (field.empty() ? "" : "." + field) +
                          ")";
    }
    return spec;
  });
}

// the value of a member of a nested JSON object, or null
[[nodiscard]] const boost::json::value &
json_at(const boost::json::object &root, std::string_view section,
        std::string_view field) {
  static const boost::json::value null_value{};
  const auto *section_value{root.if_contains(section)};
  if (section_value == nullptr || !section_value->is_object()) {
    return null_value;
  }
  const auto *field_value{section_value->as_object().if_contains(field)};
  return field_value == nullptr ? null_value : *field_value;
}


// ---------------------------------------------------------------------------
// S3 storage URIs
// ---------------------------------------------------------------------------

struct s3_uri_spec {
  std::string uri;
  bool valid;
  // for valid URIs: what the backend must report
  bool endpoint_form;
  std::string endpoint; // "<scheme>://<host>[:<port>]" or the region
  std::string bucket;
  std::string root_path;
  bool credentials;
  std::string description; // why it is invalid, for messages
};

// path segments without '/' (an encoded '/' is a separator once decoded),
// "." or ".."; '%', spaces and other characters that need encoding are kept
[[nodiscard]] gs::Generator<std::string> s3_path_segments() {
  return gs::one_of({gs::from_regex("[A-Za-z0-9_~-]{1,8}", true),
                     gs::from_regex("[A-Za-z0-9 %!$&'()*+,;=:@._~-]{1,8}",
                                    true),
                     text_of_size(1U, 6U)})
      .filter([](const std::string &segment) {
        return segment.find('/') == std::string::npos && segment != "." &&
               segment != "..";
      });
}

// a valid S3 storage URI (endpoint or region form) with at most one defect
[[nodiscard]] gs::Generator<s3_uri_spec> s3_uri_specs() {
  return gs::compose([](const hegel::TestCase &tc) {
    static const std::vector<std::string> defects{
        "scheme",        "user_only",     "query",
        "fragment",      "no_bucket",     "region_empty",
        "bucket_empty",  "region_dotted", "s3_port",
        "s3_no_host"};
    std::string defect;
    if (!tc.draw(gs::booleans())) {
      defect = tc.draw(gs::sampled_from(defects));
    }
    const auto is_defect{[&defect](std::string_view name) {
      return defect == name;
    }};
    // the defects that only apply to one form decide the form
    const bool s3_only{is_defect("region_empty") || is_defect("bucket_empty") ||
                       is_defect("region_dotted") || is_defect("s3_port") ||
                       is_defect("s3_no_host")};
    const bool endpoint_form{!s3_only && tc.draw(gs::booleans())};
    const bool endpoint_only{is_defect("no_bucket") || is_defect("scheme")};

    s3_uri_spec spec{.uri = {},
                     .valid = defect.empty(),
                     .endpoint_form = endpoint_form || endpoint_only,
                     .endpoint = {},
                     .bucket = {},
                     .root_path = {},
                     .credentials = false,
                     .description = defect};
    boost::urls::url uri;

    // user info: none, both parts (any characters, encoded as needed), or a
    // user without a password
    const auto userinfo{
        tc.draw(gs::integers<int>({.min_value = 0, .max_value = 2}))};
    if (is_defect("user_only")) {
      uri.set_user(tc.draw(text_of_size(1U, 8U)));
    } else if (userinfo != 0) {
      const auto user{tc.draw(text_of_size(0U, 8U))};
      const auto password{tc.draw(text_of_size(0U, 12U))};
      uri.set_user(user);
      uri.set_password(password);
      spec.credentials = !user.empty() || !password.empty();
    }

    const auto segments{tc.draw(gs::vectors(
        s3_path_segments(), {.min_size = 0U, .max_size = 3U}))};
    if (spec.endpoint_form) {
      const auto scheme{is_defect("scheme")
                            ? tc.draw(gs::sampled_from<std::string>(
                                  {"ftp", "file", "s4", "httpx"}))
                            : tc.draw(gs::sampled_from<std::string>(
                                  {"http", "https"}))};
      uri.set_scheme(scheme);
      const auto host{tc.draw(gs::one_of(
          {gs::from_regex("[a-z0-9-]{1,10}(\\.[a-z0-9-]{1,10}){0,2}", true),
           gs::sampled_from<std::string>({"127.0.0.1", "10.0.0.7"})}))};
      uri.set_host(host);
      spec.endpoint = scheme + "://" + host;
      if (tc.draw(gs::booleans())) {
        const auto port{tc.draw(gs::integers<std::uint16_t>())};
        uri.set_port_number(port);
        spec.endpoint += ':' + std::to_string(port);
      }
      // the first segment is the bucket, the rest is the path
      if (!is_defect("no_bucket")) {
        spec.bucket = tc.draw(gs::from_regex("[a-z0-9][a-z0-9.-]{2,12}", true));
        uri.segments().push_back(spec.bucket);
      }
      spec.root_path = "/";
      if (is_defect("no_bucket")) {
        // an empty path, or just "/"
        uri.set_path(tc.draw(gs::booleans()) ? "" : "/");
      } else {
        for (const auto &segment : segments) {
          uri.segments().push_back(segment);
          if (spec.root_path != "/") {
            spec.root_path += '/';
          }
          spec.root_path += segment;
        }
      }
    } else {
      uri.set_scheme("s3");
      spec.bucket = tc.draw(gs::from_regex("[a-z0-9-]{1,12}", true));
      auto region{tc.draw(gs::sampled_from<std::string>(
          {"us-east-1", "eu-west-1", "ap-southeast-2", "xx-test-9"}))};
      std::string host{spec.bucket + '.' + region};
      if (is_defect("region_empty")) {
        host = spec.bucket + '.';
      } else if (is_defect("bucket_empty")) {
        host = '.' + region;
      } else if (is_defect("region_dotted")) {
        host = spec.bucket + '.' + region + ".amazonaws.com";
      }
      // a host without a region would make the backend ask AWS for the
      // bucket's region, so it is never generated
      if (!is_defect("s3_no_host")) {
        uri.set_host(host);
      } else {
        uri.set_encoded_authority("");
      }
      if (is_defect("s3_port")) {
        uri.set_port_number(9000U);
      }
      spec.endpoint = region;
      for (const auto &segment : segments) {
        uri.segments().push_back(segment);
        spec.root_path += '/';
        spec.root_path += segment;
      }
    }
    if (is_defect("query")) {
      uri.set_query("versionId=1");
    }
    if (is_defect("fragment")) {
      uri.set_fragment("x");
    }
    spec.uri = uri.c_str();
    return spec;
  });
}

[[nodiscard]] binsrv::storage_config
make_s3_storage_config(std::string uri,
                       const std::filesystem::path &buffer_directory) {
  binsrv::storage_config config{};
  config.get<"backend">() = binsrv::storage_backend_type::s3;
  config.get<"uri">() = std::move(uri);
  config.get<"fs_buffer_directory">() = buffer_directory.string();
  return config;
}

struct s3_test_server {
  std::string endpoint;
  std::string access_key;
  std::string secret_key;
  std::string bucket;
};

[[nodiscard]] std::optional<s3_test_server> s3_test_server_from_environment() {
  const auto get{[](const char *name) {
    const char *value{std::getenv(name)}; // NOLINT(concurrency-mt-unsafe)
    return value == nullptr ? std::string{} : std::string{value};
  }};
  s3_test_server result{.endpoint = get("PBS_TEST_S3_ENDPOINT"),
                        .access_key = get("PBS_TEST_S3_ACCESS_KEY"),
                        .secret_key = get("PBS_TEST_S3_SECRET_KEY"),
                        .bucket = get("PBS_TEST_S3_BUCKET")};
  if (result.endpoint.empty() || result.access_key.empty() ||
      result.secret_key.empty() || result.bucket.empty()) {
    return std::nullopt;
  }
  return result;
}

// one of the valid percent-encodings of a user info part: characters that
// must be encoded always are, others sometimes, with either hex case
[[nodiscard]] std::string encode_userinfo_part(const hegel::TestCase &tc,
                                               std::string_view text,
                                               bool password) {
  static constexpr std::string_view upper{"0123456789ABCDEF"};
  static constexpr std::string_view lower{"0123456789abcdef"};
  std::string result;
  for (const char character : text) {
    const auto byte{static_cast<unsigned char>(character)};
    const bool unreserved{(byte >= 'A' && byte <= 'Z') ||
                          (byte >= 'a' && byte <= 'z') ||
                          (byte >= '0' && byte <= '9') || byte == '-' ||
                          byte == '.' || byte == '_' || byte == '~'};
    // sub-delimiters may appear as they are, ':' only in the password
    const bool sub_delimiter{std::string_view{"!$&'()*+,;="}.find(character) !=
                             std::string_view::npos};
    const bool may_stay{unreserved || sub_delimiter ||
                        (password && character == ':')};
    if (may_stay && tc.draw(gs::integers<int>(
                        {.min_value = 0, .max_value = 3})) != 0) {
      result += character;
    } else {
      const auto digits{tc.draw(gs::booleans()) ? upper : lower};
      result += '%';
      result += digits[byte >> 4U];
      result += digits[byte & 0x0FU];
    }
  }
  return result;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// properties
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(SizeUnitMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const auto text{tc.draw("text", unit_texts("KMGTP_kB"))};
    check_unit<binsrv::size_unit>(text, size_symbols);
  });
}

BOOST_AUTO_TEST_CASE(TimeUnitMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const auto text{tc.draw("text", unit_texts("smhdSMHDw"))};
    check_unit<binsrv::time_unit>(text, time_symbols);
  });
}

BOOST_AUTO_TEST_CASE(BinlogNameRoundTrip) {
  run_property([](hegel::TestCase &tc) {
    const auto base{
        tc.draw("base", gs::from_regex("[A-Za-z0-9_.-]{1,16}", true))};
    const auto number{tc.draw(
        "number", gs::integers<std::uint32_t>(
                      {.min_value = 1U,
                       .max_value = max_binlog_sequence_number}))};
    const binsrv::events::composite_binlog_name name{base, number};
    const auto text{name.str()};
    const auto parsed{binsrv::events::composite_binlog_name::parse(text)};
    require(parsed == name, "'" + text + "' parsed as '" + parsed.str() + "'");

    // the next binlog: the sequence number plus one, never wrapping around
    std::optional<binsrv::events::composite_binlog_name> next;
    try {
      next.emplace(name.next());
    } catch (const std::invalid_argument &) {
    }
    if (number == max_binlog_sequence_number) {
      require(!next.has_value(), "the binlog after '" + text + "' is '" +
                                     next->str() + "'");
    } else {
      require(next.has_value() && next->get_base_name() == base &&
                  next->get_sequence_number() == number + 1U,
              "wrong binlog after '" + text + "'");
    }
  });
}

BOOST_AUTO_TEST_CASE(BinlogNameParsingMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const auto text{tc.draw(
        "text",
        gs::one_of(
            {gs::from_regex("[a-z/ .\\x00-]{0,6}[.-]?[0-9+ -]{4,8}", true),
             text_of_size(0U, 16U)}))};
    std::optional<binsrv::events::composite_binlog_name> parsed;
    try {
      parsed.emplace(binsrv::events::composite_binlog_name::parse(text));
    } catch (const std::invalid_argument &) {
    }
    const bool expected{model_binlog_name(text)};
    require(parsed.has_value() == expected,
            "'" + text + "' was " + (expected ? "rejected" : "accepted"));
    if (parsed.has_value()) {
      require(parsed->str() == text,
              "'" + text + "' printed back as '" + parsed->str() + "'");
    }
  });
}

BOOST_AUTO_TEST_CASE(TimestampRoundTrip) {
  run_property([](hegel::TestCase &tc) {
    const auto value{tc.draw(
        "value", gs::integers<std::time_t>(
                     {.min_value = timestamp_value(min_year),
                      .max_value = timestamp_value(max_year)}))};
    const util::ctime_timestamp timestamp{value};
    const auto text{timestamp.iso_extended_str()};
    util::ctime_timestamp parsed{};
    require(util::ctime_timestamp::try_parse(text, parsed),
            "'" + text + "' was rejected");
    require(parsed.get_value() == value,
            "'" + text + "' parsed as " + std::to_string(parsed.get_value()) +
                ", expected " + std::to_string(value));
  });
}

BOOST_AUTO_TEST_CASE(TimestampParsingMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const auto fields{tc.draw("fields", timestamp_field_values())};
    const auto text{format_timestamp(fields)};
    const auto expected{model_timestamp(fields)};
    util::ctime_timestamp parsed{};
    const bool accepted{util::ctime_timestamp::try_parse(text, parsed)};
    if (!expected.has_value()) {
      require(!accepted, "'" + text + "' was accepted as " +
                             parsed.iso_extended_str());
      return;
    }
    require(accepted, "'" + text + "' was rejected");
    require(parsed.get_value() == *expected,
            "'" + text + "' parsed as " + std::to_string(parsed.get_value()) +
                ", expected " + std::to_string(*expected));
  });
}

BOOST_AUTO_TEST_CASE(TimestampParsingOfArbitraryTextIsSane) {
  run_property([](hegel::TestCase &tc) {
    const auto text{tc.draw(
        "text",
        gs::one_of({gs::sampled_from<std::string>(
                        {"not-a-date-time", "+infinity", "-infinity",
                         "not_a_date_time", "2026-10-06", "2026-10-06T",
                         "20261006T120000", "2026-10-06 12:00:00",
                         "2026-Oct-06 12:00:00", "2026-10-06T12:00"}),
                    gs::from_regex("[0-9]{1,5}-[0-9]{1,3}-[0-9]{1,3}T[0-9]{1,"
                                   "3}:[0-9]{1,3}(:[0-9]{1,3})?[.,0-9]{0,4}",
                                   true),
                    text_of_size(0U, 24U)}))};
    util::ctime_timestamp parsed{};
    if (!util::ctime_timestamp::try_parse(text, parsed)) {
      return;
    }
    // an accepted timestamp is a real point in time that prints and parses
    // back to itself
    require(parsed.get_value() >= timestamp_value(min_year) &&
                parsed.get_value() <= timestamp_value(max_year),
            "'" + text + "' was accepted as " +
                std::to_string(parsed.get_value()) +
                ", outside of the supported date range");
    const auto printed{parsed.iso_extended_str()};
    util::ctime_timestamp reparsed{};
    require(util::ctime_timestamp::try_parse(printed, reparsed) &&
                reparsed == parsed,
            "'" + text + "' was accepted, but printed as '" + printed +
                "' which does not parse back");
  });
}

BOOST_AUTO_TEST_CASE(SemanticVersionParsingMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const auto text{tc.draw(
        "text",
        gs::one_of({gs::from_regex("[0-9]{1,3}(\\.[0-9]{1,3}){2}(-[a-z0-9.-]{"
                                   "0,8})?",
                                   true),
                    gs::from_regex("[0-9.+ -]{0,12}", true),
                    text_of_size(0U, 12U)}))};
    const auto expected{model_version(text)};
    std::optional<util::semantic_version> parsed;
    try {
      parsed.emplace(text);
    } catch (const std::invalid_argument &) {
    }
    require(parsed.has_value() == expected.has_value(),
            "'" + text + "' was " +
                (expected.has_value() ? "rejected" : "accepted"));
    if (!parsed.has_value()) {
      return;
    }
    require(parsed->get_major() == expected->at(0U) &&
                parsed->get_minor() == expected->at(1U) &&
                parsed->get_patch() == expected->at(2U),
            "'" + text + "' parsed as " + parsed->get_string());
  });
}

BOOST_AUTO_TEST_CASE(SemanticVersionEncodingRoundTrip) {
  run_property([](hegel::TestCase &tc) {
    // the encoding has two decimal digits per component
    const auto component{[&tc](const char *name) {
      return tc.draw(name, gs::integers<std::uint8_t>(
                               {.min_value = 0U, .max_value = 99U}));
    }};
    const util::semantic_version version{component("major"),
                                         component("minor"),
                                         component("patch")};
    const util::semantic_version decoded{version.get_encoded()};
    require(decoded.get_major() == version.get_major() &&
                decoded.get_minor() == version.get_minor() &&
                decoded.get_patch() == version.get_patch(),
            version.get_string() + " decoded as " + decoded.get_string());
    const util::semantic_version reparsed{version.get_string()};
    require(reparsed.get_encoded() == version.get_encoded(),
            version.get_string() + " parsed back as " + reparsed.get_string());
  });
}

BOOST_AUTO_TEST_CASE(FilesystemStorageUriMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const scratch_directory scratch;
    // a directory whose name needs percent-encoding in a URI
    const auto directory_name{tc.draw(
        "directory", gs::from_regex("[A-Za-z0-9 %#?@:;=&+,!$'()._~-]{1,12}",
                                    true)
                         .filter([](const std::string &value) {
                           return value != "." && value != "..";
                         }))};
    const auto root{scratch.path() / directory_name};
    std::filesystem::create_directories(root);

    // what a user may write in the configuration file
    boost::urls::url uri;
    uri.set_scheme_id(boost::urls::scheme::file);
    uri.set_encoded_authority("");
    const bool with_host{tc.draw("with_host", gs::booleans())};
    const bool with_port{tc.draw("with_port", gs::booleans())};
    const bool with_userinfo{tc.draw("with_userinfo", gs::booleans())};
    const bool with_query{tc.draw("with_query", gs::booleans())};
    const bool with_fragment{tc.draw("with_fragment", gs::booleans())};
    // a path with a NUL character (%00 in the URI) names no directory, even
    // when the part before it does
    const bool with_nul{tc.draw("with_nul", gs::booleans())};
    if (with_host) {
      uri.set_host("localhost");
    }
    if (with_port) {
      uri.set_port_number(8080U);
    }
    if (with_userinfo) {
      uri.set_userinfo("user:secret");
    }
    std::string path{root.generic_string()};
    if (with_nul) {
      path += std::string_view{"\0suffix", 7U};
    }
    uri.set_path(path);
    if (with_query) {
      uri.set_query("region=x");
    }
    if (with_fragment) {
      uri.set_fragment("part");
    }
    const std::string uri_text{uri.c_str()};
    const bool expected{!with_host && !with_port && !with_userinfo &&
                        !with_query && !with_fragment && !with_nul};

    std::optional<binsrv::filesystem_storage_backend> backend;
    try {
      backend.emplace(make_storage_config(uri_text));
    } catch (const std::invalid_argument &) {
    }
    require(backend.has_value() == expected,
            "'" + uri_text + "' was " + (expected ? "rejected" : "accepted"));
    if (!backend.has_value()) {
      return;
    }
    require(backend->get_description() ==
                "local filesystem - " + root.generic_string(),
            "'" + uri_text + "' opened as '" + backend->get_description() +
                "'");
    // object URIs point to the object inside the root directory
    static constexpr std::string_view object_name{"binlog.000001"};
    const auto object_uri{backend->get_object_uri(object_name)};
    const auto parsed{boost::urls::parse_absolute_uri(object_uri)};
    require(parsed.has_value() &&
                std::filesystem::path{std::string{parsed->path()}} ==
                    root / object_name,
            "object URI '" + object_uri + "' does not point to '" +
                (root / object_name).generic_string() + "'");
  });
}

BOOST_AUTO_TEST_CASE(StorageUriMaskingHidesCredentials) {
  run_property([](hegel::TestCase &tc) {
    boost::urls::url uri;
    uri.set_scheme(tc.draw("scheme", gs::sampled_from<std::string>(
                                         {"s3", "http", "https"})));
    const auto user{tc.draw("user", text_of_size(1U, 12U))};
    const auto password{tc.draw("password", text_of_size(0U, 12U))};
    uri.set_user(user);
    uri.set_password(password);
    uri.set_host(tc.draw("host", gs::from_regex("[a-z0-9]{1,8}(\\.[a-z0-9]{"
                                                "1,8}){0,2}",
                                                true)));
    if (tc.draw("with_port", gs::booleans())) {
      uri.set_port_number(tc.draw("port", gs::integers<std::uint16_t>()));
    }
    uri.set_path(tc.draw("path", text_of_size(0U, 16U)));
    const std::string uri_text{uri.c_str()};

    const auto masked{make_storage_config(uri_text).get_masked_uri()};
    const auto parsed{boost::urls::parse_absolute_uri(masked)};
    require(parsed.has_value(), "'" + uri_text + "' was masked as '" +
                                    masked + "', which is not a URI");
    require(parsed->encoded_userinfo() == "***:***",
            "'" + uri_text + "' was masked as '" + masked + "'");
    require(parsed->scheme() == uri.scheme() &&
                parsed->encoded_host_and_port() ==
                    uri.encoded_host_and_port() &&
                parsed->encoded_path() == uri.encoded_path(),
            "'" + uri_text + "' was masked as '" + masked +
                "', which points elsewhere");
  });
}

BOOST_AUTO_TEST_CASE(KeyringFileMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const auto spec{tc.draw("keyring", keyring_specs())};
    const scratch_directory scratch;
    const auto path{scratch.path() / "keyring.json"};
    const auto text{keyring_json(spec)};
    write_file(path, text);

    std::optional<binsrv::keyring_record_collection> keyring;
    const auto error{load_cleanly([&] { keyring.emplace(path.string()); })};
    const bool expected{model_keyring(spec)};
    require(keyring.has_value() == expected,
            "'" + text + "' was " +
                (expected ? "rejected: " + error.value_or("") : "accepted"));
    if (!keyring.has_value()) {
      return;
    }
    for (const auto &key : spec.keys) {
      require(keyring->contains_key(key.id),
              "key '" + key.id + "' is missing");
      const auto &record{keyring->get_key(key.id)};
      require(record.get<"cipher">() == keyring_ciphers.at(key.cipher).name,
              "key '" + key.id + "' has cipher '" + record.get<"cipher">() +
                  "'");
      require(record.get<"data_hex">().to_hex_string() ==
                  to_hex(key.data, false),
              "key '" + key.id + "' has data " +
                  record.get<"data_hex">().to_hex_string());
    }
    static constexpr std::string_view absent_id{"no such key \x01"};
    require(!keyring->contains_key(absent_id), "an absent key was found");
    bool absent_rejected{false};
    try {
      [[maybe_unused]] const auto &absent{keyring->get_key(absent_id)};
    } catch (const std::out_of_range &) {
      absent_rejected = true;
    }
    require(absent_rejected, "getting an absent key did not throw");
    [[maybe_unused]] const auto description{keyring->get_description()};
  });
}

BOOST_AUTO_TEST_CASE(KeyringDamagedFileFailsCleanly) {
  run_property([](hegel::TestCase &tc) {
    auto spec{tc.draw("keyring", keyring_specs())};
    const scratch_directory scratch;
    const auto path{scratch.path() / "keyring.json"};
    const auto text{damage_text(tc, keyring_json(spec))};
    write_file(path, text);
    [[maybe_unused]] const auto error{load_cleanly([&] {
      const binsrv::keyring_record_collection keyring{path.string()};
      [[maybe_unused]] const auto description{keyring.get_description()};
    })};
  });
}

BOOST_AUTO_TEST_CASE(ConfigFileMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const auto spec{tc.draw("config", config_specs())};
    const scratch_directory scratch;
    const auto path{scratch.path() / "config.json"};
    const auto text{boost::json::serialize(spec.json)};
    write_file(path, text);

    std::optional<binsrv::main_config> config;
    const auto error{load_cleanly([&] { config.emplace(path.string()); })};
    require(config.has_value() == spec.valid,
            "'" + text + "' was " +
                (spec.valid ? "rejected: " + error.value_or("")
                            : "accepted, expected a rejection for: " +
                                  spec.description));
    if (!config.has_value()) {
      return;
    }
    const auto &root{config->root()};
    const auto &json{spec.json};
    const auto same_string{[&](std::string_view section,
                               std::string_view field,
                               const std::string &actual) {
      require(json_at(json, section, field).as_string() == actual,
              std::string{section} + '.' + std::string{field} + " is '" +
                  actual + "' in '" + text + "'");
    }};
    const auto same_number{[&](std::string_view section,
                               std::string_view field, std::uint64_t actual) {
      require(json_at(json, section, field).to_number<std::uint64_t>() ==
                  actual,
              std::string{section} + '.' + std::string{field} + " is " +
                  std::to_string(actual) + " in '" + text + "'");
    }};
    const auto &logger{root.get<"logger">()};
    same_string("logger", "level",
                std::string{binsrv::to_string_view(logger.get<"level">())});
    const auto &connection{root.get<"connection">()};
    same_string("connection", "user", connection.get<"user">());
    same_string("connection", "password", connection.get<"password">());
    if (connection.get<"host">().has_value()) {
      same_string("connection", "host", *connection.get<"host">());
    }
    if (connection.get<"port">().has_value()) {
      same_number("connection", "port", *connection.get<"port">());
    }
    if (connection.get<"dns_srv_name">().has_value()) {
      same_string("connection", "dns_srv_name",
                  *connection.get<"dns_srv_name">());
    }
    same_number("connection", "connect_timeout",
                connection.get<"connect_timeout">());
    same_number("connection", "read_timeout",
                connection.get<"read_timeout">());
    same_number("connection", "write_timeout",
                connection.get<"write_timeout">());
    const auto &replication{root.get<"replication">()};
    same_number("replication", "server_id", replication.get<"server_id">());
    same_number("replication", "idle_time", replication.get<"idle_time">());
    require(json_at(json, "replication", "verify_checksum").as_bool() ==
                replication.get<"verify_checksum">(),
            "replication.verify_checksum differs");
    same_string("replication", "mode",
                boost::lexical_cast<std::string>(replication.get<"mode">()));
    const auto &source{root.get<"replication_source">()};
    same_number("replication_source", "port", source.get<"port">());
    same_number("replication_source", "read_timeout",
                source.get<"read_timeout">());
    same_number("replication_source", "write_timeout",
                source.get<"write_timeout">());
    const auto &storage{root.get<"storage">()};
    same_string("storage", "backend",
                boost::lexical_cast<std::string>(storage.get<"backend">()));
    same_string("storage", "uri", storage.get<"uri">());
    if (storage.get<"checkpoint_size">().has_value()) {
      const auto expected{model_unit(
          json_at(json, "storage", "checkpoint_size").as_string(),
          size_symbols)};
      require(expected == storage.get<"checkpoint_size">()->get_value(),
              "storage.checkpoint_size differs in '" + text + "'");
    }
    if (storage.get<"checkpoint_interval">().has_value()) {
      const auto expected{model_unit(
          json_at(json, "storage", "checkpoint_interval").as_string(),
          time_symbols)};
      require(expected == storage.get<"checkpoint_interval">()->get_value(),
              "storage.checkpoint_interval differs in '" + text + "'");
    }
    require(root.get<"keyring">().has_value() == json.contains("keyring"),
            "keyring section presence differs in '" + text + "'");
  });
}

BOOST_AUTO_TEST_CASE(ConfigDamagedFileFailsCleanly) {
  run_property([](hegel::TestCase &tc) {
    const auto spec{tc.draw("config", config_specs())};
    const scratch_directory scratch;
    const auto path{scratch.path() / "config.json"};
    write_file(path, damage_text(tc, boost::json::serialize(spec.json)));
    [[maybe_unused]] const auto error{
        load_cleanly([&] { const binsrv::main_config config{path.string()}; })};
  });
}

BOOST_AUTO_TEST_CASE(S3StorageUriMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const auto spec{tc.draw("uri", s3_uri_specs())};
    const scratch_directory scratch;
    std::optional<binsrv::s3_storage_backend> backend;
    std::string error;
    try {
      backend.emplace(make_s3_storage_config(spec.uri, scratch.path()));
    } catch (const std::invalid_argument &e) {
      error = e.what();
    }
    require(backend.has_value() == spec.valid,
            "'" + spec.uri + "' was " +
                (spec.valid ? "rejected: " + error
                            : "accepted, expected a rejection for: " +
                                  spec.description));
    if (!backend.has_value()) {
      return;
    }

    // "AWS S3 (SDK x.y.z) - <what>": compare <what>
    const auto description{backend->get_description()};
    const auto what{description.substr(description.find(") - ") + 4U)};
    const std::string expected{
        (spec.endpoint_form ? "endpoint: " : "region: ") + spec.endpoint +
        ", bucket: " + spec.bucket + ", path: " + spec.root_path +
        ", credentials: " + (spec.credentials ? "***hidden***" : "none")};
    require(what == expected, "'" + spec.uri + "' is described as '" + what +
                                  "', expected '" + expected + "'");

    // object URIs point to the object under the root path of the bucket
    static constexpr std::string_view object_name{"binlog.000001"};
    const auto object_uri{backend->get_object_uri(object_name)};
    const auto parsed{boost::urls::parse_absolute_uri(object_uri)};
    require(parsed.has_value(), "'" + spec.uri + "' gives the object URI '" +
                                    object_uri + "', which is not a URI");
    std::string key{spec.root_path};
    if (!key.empty() && key.back() != '/') {
      key += '/';
    }
    key += object_name;
    std::string expected_path{spec.endpoint_form ? "/" + spec.bucket + key
                                                 : key};
    std::string actual_path{parsed->path()};
    if (!expected_path.starts_with('/')) {
      expected_path.insert(0U, 1U, '/');
    }
    if (!actual_path.starts_with('/')) {
      actual_path.insert(0U, 1U, '/');
    }
    require(actual_path == expected_path,
            "'" + spec.uri + "' gives the object URI '" + object_uri +
                "', whose path is '" + actual_path + "' instead of '" +
                expected_path + "'");
    if (!spec.endpoint_form) {
      require(parsed->host() == spec.bucket,
              "'" + spec.uri + "' gives the object URI '" + object_uri +
                  "' for another bucket");
    }
  });
}

BOOST_AUTO_TEST_CASE(S3StorageCredentialsAreDecoded) {
  const auto server{s3_test_server_from_environment()};
  if (!server.has_value()) {
    BOOST_TEST_MESSAGE("skipped: PBS_TEST_S3_ENDPOINT, PBS_TEST_S3_ACCESS_KEY, "
                       "PBS_TEST_S3_SECRET_KEY and PBS_TEST_S3_BUCKET are not "
                       "set");
    return;
  }
  hegel::Settings settings{};
  // one HTTP request per test case
  settings.test_cases = 50U;
  settings.suppress_health_check = {hegel::HealthCheck::TooSlow};
  run_property(
      [&server](hegel::TestCase &tc) {
        const scratch_directory scratch;
        // the real credentials in some valid encoding, or with one
        // character of the secret changed
        const bool wrong_secret{tc.draw("wrong_secret", gs::booleans())};
        auto secret{server->secret_key};
        if (wrong_secret) {
          auto &character{secret[tc.draw(
              "position",
              gs::integers<std::size_t>(
                  {.min_value = 0U, .max_value = std::size(secret) - 1U}))]};
          character = character == 'x' ? 'y' : 'x';
        }
        const auto uri{"http://" +
                       encode_userinfo_part(tc, server->access_key, false) +
                       ':' + encode_userinfo_part(tc, secret, true) + '@' +
                       server->endpoint + '/' + server->bucket +
                       "/credentials-test"};
        tc.note("uri: " + uri);
        binsrv::s3_storage_backend backend{
            make_s3_storage_config(uri, scratch.path())};
        bool listed{true};
        std::string error;
        try {
          [[maybe_unused]] const auto objects{backend.list_objects()};
        } catch (const std::exception &e) {
          listed = false;
          error = e.what();
        }
        if (wrong_secret) {
          require(!listed, "'" + uri + "' with a wrong secret was accepted");
        } else {
          require(listed, "'" + uri + "' was not accepted: " + error);
        }
      },
      settings);
}
