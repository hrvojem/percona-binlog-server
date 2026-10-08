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

// Property-based tests for binsrv::select_purge_victim_count, the victim
// selection of the purge_binlogs operation, built with Hegel
// (https://hegel.dev). The stateful storage property test exercises purging
// end to end against a real storage; here the selection and its guards are
// checked directly over generated binlog record lists against an independent
// model.
//
// Records share one base name and carry increasing sequence numbers. A target
// binlog name is generated to be either present in the list, absent but with
// the same base name, or of a different base name, so every branch is
// exercised:
// - an empty storage is rejected;
// - a target with a different base name than the records is rejected (checked
//   before presence);
// - a target that is not present is rejected;
// - a target that is the current tail (the last record) is rejected, so at
//   least one record always remains;
// - otherwise the victim count is the length of the prefix ending with the
//   target (inclusive), which is at least one and leaves at least one record.

#include <cstddef>
#include <cstdint>
#include <exception>
#include <string_view>
#include <vector>

#define BOOST_TEST_MODULE PurgeSelectionPropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "property_test_helpers.hpp"

#include "binsrv/storage_core.hpp"

#include "binsrv/events/composite_binlog_name.hpp"

namespace {

namespace gs = hegel::generators;

using binsrv::events::composite_binlog_name;

using property_testing::require;
using property_testing::run_property;

constexpr std::string_view records_base_name{"binlog"};
constexpr std::string_view other_base_name{"mysqld-bin"};

// a storage of 'count' records sharing records_base_name, with sequence
// numbers 1, 2, ..., count (so names are distinct and in storage order)
[[nodiscard]] binsrv::binlog_record_container make_records(std::size_t count) {
  binsrv::binlog_record_container records;
  records.reserve(count);
  for (std::size_t index{0U}; index != count; ++index) {
    binsrv::binlog_record record;
    record.name = composite_binlog_name{
        records_base_name, static_cast<std::uint32_t>(index + 1U)};
    records.push_back(std::move(record));
  }
  return records;
}

enum class target_kind : std::uint8_t {
  present,          // a name that is in the storage
  absent_same_base, // a name with the right base but no matching record
  different_base,   // a name with a different base
};

[[nodiscard]] bool purge_selection_threw(
    const binsrv::binlog_record_container &records,
    const composite_binlog_name &target, std::size_t &victim_count) {
  try {
    victim_count = binsrv::select_purge_victim_count(records, target);
    return false;
  } catch (const std::exception &) {
    return true;
  }
}

BOOST_AUTO_TEST_CASE(PurgeVictimSelectionMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const auto count{tc.draw(
        "count", gs::integers<std::size_t>({.min_value = 0U,
                                            .max_value = 8U}))};
    const auto records{make_records(count)};

    const auto kind{tc.draw(
        "kind", gs::sampled_from({target_kind::present,
                                  target_kind::absent_same_base,
                                  target_kind::different_base}))};

    // build the target and, in parallel, the model's expectation
    composite_binlog_name target;
    bool expect_throw{false};
    std::size_t expected_victim_count{0U};

    if (count == 0U) {
      // the empty-storage guard fires first, whatever the target is
      target = composite_binlog_name{records_base_name, 1U};
      expect_throw = true;
    } else if (kind == target_kind::different_base) {
      const auto sequence{tc.draw(
          "seq", gs::integers<std::uint32_t>({.min_value = 1U,
                                              .max_value = 16U}))};
      target = composite_binlog_name{other_base_name, sequence};
      expect_throw = true; // base-name guard
    } else if (kind == target_kind::absent_same_base) {
      // a sequence number past the end is guaranteed not to be present
      const auto extra{tc.draw(
          "extra", gs::integers<std::uint32_t>({.min_value = 1U,
                                                .max_value = 8U}))};
      target = composite_binlog_name{
          records_base_name, static_cast<std::uint32_t>(count) + extra};
      expect_throw = true; // not-present guard
    } else {
      // present: pick one of the records by position
      const auto position{tc.draw(
          "position",
          gs::integers<std::size_t>({.min_value = 0U,
                                     .max_value = count - 1U}))};
      target = composite_binlog_name{
          records_base_name, static_cast<std::uint32_t>(position + 1U)};
      if (position == count - 1U) {
        expect_throw = true; // tail guard: the last record must remain
      } else {
        expected_victim_count = position + 1U;
      }
    }

    std::size_t victim_count{0U};
    const bool threw{purge_selection_threw(records, target, victim_count)};
    require(threw == expect_throw,
            "the purge victim selection must accept or reject the target "
            "exactly as the model does");
    if (threw) {
      return;
    }

    require(victim_count == expected_victim_count,
            "the victim count must equal the length of the prefix ending with "
            "the target");
    require(victim_count >= 1U,
            "a successful purge must drop at least one record");
    require(victim_count < records.size(),
            "a successful purge must leave at least one record to preserve the "
            "resume position");
  });
}

} // namespace
