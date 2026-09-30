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

#ifndef TESTS_PROPERTY_TEST_HELPERS_HPP
#define TESTS_PROPERTY_TEST_HELPERS_HPP

// helpers shared by the Hegel (https://hegel.dev) property-based tests

#include <functional>
#include <source_location>
#include <stdexcept>
#include <string>

#include <boost/test/framework.hpp>
#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

namespace property_testing {

// runs a Hegel property inside the current Boost.Test test case: the test
// case name is used both in the failure report and as the Hegel example
// database key, so that a failure found once is replayed first next time
inline void
run_property(const std::function<void(hegel::TestCase &)> &body,
             const std::source_location location =
                 std::source_location::current()) {
  hegel::test(body,
              hegel::TestLocation{
                  boost::unit_test::framework::current_test_case().p_name.get(),
                  location.file_name(), static_cast<int>(location.line())});
}

// a property is violated by throwing, so that Hegel can shrink the input
inline void require(bool condition, const std::string &message) {
  if (!condition) {
    throw std::runtime_error{message};
  }
}

} // namespace property_testing

#endif // TESTS_PROPERTY_TEST_HELPERS_HPP
