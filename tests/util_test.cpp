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

// Unit tests of util helpers at their exact limits: byte_range and
// read_file_content (gaps found by mutation testing, see extra/qa/mull).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#define BOOST_TEST_MODULE UtilTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <boost/test/tools/old/interface.hpp>

#include "util/byte_range.hpp"
#include "util/dynamic_byte_buffer_fwd.hpp"
#include "util/file_operations_helpers.hpp"

namespace {

// a temporary file of 'size' bytes 0, 1, 2, ... removed when it goes out of
// scope
class temp_file {
public:
  explicit temp_file(std::size_t size)
      : path_{std::filesystem::temp_directory_path() /
              ("pbs_util_test_" +
               std::to_string(std::chrono::steady_clock::now()
                                  .time_since_epoch()
                                  .count()))} {
    std::ofstream output{path_, std::ios_base::binary};
    for (std::size_t index{0U}; index < size; ++index) {
      output.put(static_cast<char>(index & 0xFFU));
    }
  }
  temp_file(const temp_file &) = delete;
  temp_file &operator=(const temp_file &) = delete;
  temp_file(temp_file &&) = delete;
  temp_file &operator=(temp_file &&) = delete;
  ~temp_file() {
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path &path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

} // anonymous namespace

BOOST_AUTO_TEST_CASE(ByteRangeLimitsAndText) {
  constexpr auto max{std::numeric_limits<std::uint64_t>::max()};
  // a range may end exactly at the largest offset, not one past it
  BOOST_CHECK_NO_THROW(static_cast<void>(util::byte_range(1U, max - 1U)));
  BOOST_CHECK_THROW(static_cast<void>(util::byte_range(1U, max)),
                    std::overflow_error);
  // offset - last byte, both inclusive
  BOOST_CHECK_EQUAL(util::byte_range(10U, 5U).to_string(), "10-14");
  BOOST_CHECK_EQUAL(util::byte_range(10U).to_string(), "10-");
  BOOST_CHECK_EQUAL(util::byte_range(0U, 0U).to_string(), "<empty>");
  BOOST_CHECK_EQUAL(util::byte_range().to_string(), "<full>");
}

BOOST_AUTO_TEST_CASE(ReadFileContentAtItsLimits) {
  constexpr std::size_t size{10U};
  const temp_file file{size};
  // the whole file from an offset: the rest of it
  const auto rest{util::read_file_content("file", file.path(), size,
                                          util::byte_range{3U})};
  BOOST_REQUIRE_EQUAL(std::size(rest), size - 3U);
  BOOST_CHECK(rest.front() == std::byte{3U});
  // from the end of the file: nothing, which is not an error
  BOOST_CHECK(util::read_file_content("file", file.path(), size,
                                      util::byte_range{size})
                  .empty());
  BOOST_CHECK_THROW(static_cast<void>(util::read_file_content(
                        "file", file.path(), size, util::byte_range{size + 1U})),
                    std::out_of_range);
  // exactly 'max_size' bytes may be loaded, one more may not
  BOOST_CHECK_EQUAL(std::size(util::read_file_content(
                        "file", file.path(), size, util::byte_range{0U, size})),
                    size);
  BOOST_CHECK_THROW(static_cast<void>(util::read_file_content(
                        "file", file.path(), size - 1U,
                        util::byte_range{0U, size})),
                    std::out_of_range);
}
