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

// Property-based tests for the record-selection logic of the search
// operations (operations/search_helpers.cpp), built with Hegel
// (https://hegel.dev). The MTR suite exercises these operations end to end on
// a live server; here the selection is checked over many generated binlog
// record sets against an independent model.
//
// GTIDs are drawn from a tiny universe (a few individual GTIDs), so that a
// binlog record's GTID set, and the target set, are subsets of it that overlap
// often. The production selection runs on real binsrv::gtids::gtid_set values,
// while the model runs on std::set<int> bitmasks of the same universe, so the
// two are genuinely independent.
//
// - select_records_by_gtid_set:
//   * an empty storage, and (for a non-empty storage) a non-GTID-mode storage,
//     are rejected;
//   * the search succeeds exactly when the target set is covered by the union
//     of all records' GTID sets;
//   * on success the selected positions are a strictly increasing subsequence,
//     every selected record's GTID set intersects the target, the union of the
//     selected records' GTID sets covers the target, and (when the target is
//     non-empty) dropping the last selected record no longer covers it - i.e.
//     the search stops as soon as the target is covered;
//   * the selected positions equal those an independent greedy model produces.
// - select_records_by_timestamp:
//   * an empty storage is rejected, and so is a timestamp older than every
//     record (nothing selected);
//   * on success the selected positions are exactly the leading records
//     0, 1, ... up to the first whose minimum timestamp exceeds the target,
//     each selected record's minimum timestamp is within the target, and the
//     first excluded record's minimum timestamp exceeds it.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <iterator>
#include <ranges>
#include <set>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

#define BOOST_TEST_MODULE SearchHelpersPropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "property_test_helpers.hpp"

#include "operations/search_helpers.hpp"

#include "binsrv/storage_core.hpp"

#include "binsrv/gtids/common_types.hpp"
#include "binsrv/gtids/gtid_set.hpp"
#include "binsrv/gtids/tag.hpp"
#include "binsrv/gtids/uuid.hpp"

#include "util/ctime_timestamp.hpp"
#include "util/ctime_timestamp_range.hpp"

namespace {

namespace gs = hegel::generators;

using binsrv::gtids::gno_t;
using binsrv::gtids::gtid_set;
using binsrv::gtids::tag;
using binsrv::gtids::uuid;

using property_testing::require;
using property_testing::run_property;

// a tiny GTID universe: two source UUIDs, three GNOs each, so six individual
// GTIDs that record sets and the target set are drawn from
constexpr std::array uuid_pool{
    std::string_view{"11111111-aaaa-1111-aaaa-111111111111"},
    std::string_view{"22222222-bbbb-2222-bbbb-222222222222"}};
constexpr std::size_t gnos_per_uuid{3U};
constexpr std::size_t universe_size{std::size(uuid_pool) * gnos_per_uuid};
constexpr unsigned full_mask{(1U << universe_size) - 1U};

// adds the GTID that the universe index names to the set
void add_universe_gtid(gtid_set &target, std::size_t index) {
  const uuid source{uuid_pool.at(index / gnos_per_uuid)};
  const gno_t gno{binsrv::gtids::min_gno + index % gnos_per_uuid};
  target.add(source, tag{}, gno);
}

// a real gtid_set from a universe bitmask
[[nodiscard]] gtid_set gtid_set_from_mask(unsigned mask) {
  gtid_set result;
  for (std::size_t index{0U}; index != universe_size; ++index) {
    if ((mask & (1U << index)) != 0U) {
      add_universe_gtid(result, index);
    }
  }
  return result;
}

// the same bitmask as a model set of universe indices
[[nodiscard]] std::set<std::size_t> model_from_mask(unsigned mask) {
  std::set<std::size_t> result;
  for (std::size_t index{0U}; index != universe_size; ++index) {
    if ((mask & (1U << index)) != 0U) {
      result.insert(index);
    }
  }
  return result;
}

[[nodiscard]] gs::Generator<unsigned> masks() {
  return gs::integers<unsigned>({.min_value = 0U, .max_value = full_mask});
}

struct record_spec {
  bool has_gtids;
  unsigned mask;
};

// draws a storage: a list of records, each either carrying a GTID subset or no
// GTIDs at all (added_gtids unset, as a binlog with no transactions)
[[nodiscard]] std::vector<record_spec> draw_record_specs(hegel::TestCase &tc) {
  const auto count{
      tc.draw("count", gs::integers<std::size_t>({.min_value = 0U,
                                                  .max_value = 8U}))};
  std::vector<record_spec> specs;
  specs.reserve(count);
  for (std::size_t index{0U}; index != count; ++index) {
    const bool has_gtids{tc.draw(gs::booleans())};
    const unsigned mask{has_gtids ? tc.draw(masks()) : 0U};
    specs.push_back({.has_gtids = has_gtids, .mask = mask});
  }
  return specs;
}

[[nodiscard]] binsrv::binlog_record_container
records_from_gtid_specs(const std::vector<record_spec> &specs) {
  binsrv::binlog_record_container records;
  records.reserve(specs.size());
  for (const auto &spec : specs) {
    binsrv::binlog_record record;
    if (spec.has_gtids) {
      record.added_gtids = gtid_set_from_mask(spec.mask);
    }
    records.push_back(std::move(record));
  }
  return records;
}

// true if select_records_by_gtid_set threw (the operation's failure path)
[[nodiscard]] bool gtid_search_threw(
    const binsrv::binlog_record_container &records, bool gtid_mode,
    const gtid_set &target, std::vector<std::size_t> &selected) {
  try {
    selected = operations::select_records_by_gtid_set(records, gtid_mode,
                                                      target);
    return false;
  } catch (const std::runtime_error &) {
    return true;
  }
}

BOOST_AUTO_TEST_CASE(GtidSearchRejectsEmptyStorage) {
  run_property([](hegel::TestCase &tc) {
    const binsrv::binlog_record_container records;
    const auto target{gtid_set_from_mask(tc.draw("target", masks()))};
    std::vector<std::size_t> selected;
    require(gtid_search_threw(records, tc.draw(gs::booleans()), target,
                              selected),
            "an empty storage must be rejected");
  });
}

BOOST_AUTO_TEST_CASE(GtidSearchRejectsNonGtidMode) {
  run_property([](hegel::TestCase &tc) {
    const auto specs{draw_record_specs(tc)};
    if (specs.empty()) {
      return; // the empty-storage error takes precedence; covered elsewhere
    }
    const auto records{records_from_gtid_specs(specs)};
    const auto target{gtid_set_from_mask(tc.draw("target", masks()))};
    std::vector<std::size_t> selected;
    require(gtid_search_threw(records, /*gtid_mode=*/false, target, selected),
            "a non-empty storage not in GTID mode must be rejected");
  });
}

BOOST_AUTO_TEST_CASE(GtidSearchMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const auto specs{draw_record_specs(tc)};
    if (specs.empty()) {
      return; // exercised by GtidSearchRejectsEmptyStorage
    }
    const auto records{records_from_gtid_specs(specs)};
    const unsigned target_mask{tc.draw("target", masks())};
    const auto target{gtid_set_from_mask(target_mask)};
    const auto target_model{model_from_mask(target_mask)};

    // independent model: the target is coverable exactly when every one of its
    // GTIDs appears in some record's GTID set
    std::set<std::size_t> covered;
    for (const auto &spec : specs) {
      if (spec.has_gtids) {
        const auto record_model{model_from_mask(spec.mask)};
        covered.insert(std::cbegin(record_model), std::cend(record_model));
      }
    }
    const bool expected_success{
        std::ranges::includes(covered, target_model)};

    std::vector<std::size_t> selected;
    const bool threw{
        gtid_search_threw(records, /*gtid_mode=*/true, target, selected)};
    require(threw != expected_success,
            "the search must succeed exactly when the target is covered by the "
            "union of the records' GTID sets");
    if (threw) {
      return;
    }

    // selected positions are a strictly increasing subsequence
    for (std::size_t position{0U}; position != selected.size(); ++position) {
      require(selected[position] < records.size(),
              "a selected position must be in range");
      if (position != 0U) {
        require(selected[position - 1U] < selected[position],
                "selected positions must be strictly increasing");
      }
    }

    // every selected record intersects the target and carries GTIDs, and the
    // union of the selected records covers the target
    std::set<std::size_t> selected_union;
    for (const auto record_index : selected) {
      const auto &spec{specs[record_index]};
      require(spec.has_gtids, "a selected record must carry GTIDs");
      const auto record_model{model_from_mask(spec.mask)};
      std::set<std::size_t> overlap;
      std::ranges::set_intersection(
          record_model, target_model,
          std::inserter(overlap, std::end(overlap)));
      require(!overlap.empty(),
              "a selected record's GTID set must intersect the target");
      selected_union.insert(std::cbegin(record_model), std::cend(record_model));
    }
    require(std::ranges::includes(selected_union, target_model),
            "the selected records must cover the target");

    // the search stops as soon as the target is covered: dropping the last
    // selected record leaves the target uncovered (only meaningful when the
    // target is non-empty, otherwise nothing is selected)
    if (!target_model.empty()) {
      require(!selected.empty(),
              "a non-empty, covered target must select at least one record");
      std::set<std::size_t> without_last;
      for (std::size_t position{0U}; position + 1U < selected.size();
           ++position) {
        const auto record_model{model_from_mask(specs[selected[position]].mask)};
        without_last.insert(std::cbegin(record_model), std::cend(record_model));
      }
      require(!std::ranges::includes(without_last, target_model),
              "dropping the last selected record must stop covering the target");
    }

    // and the positions match the independent greedy model exactly
    std::set<std::size_t> remaining{target_model};
    std::vector<std::size_t> selected_model;
    for (std::size_t index{0U}; index != specs.size(); ++index) {
      if (remaining.empty()) {
        break;
      }
      const auto &spec{specs[index]};
      if (!spec.has_gtids) {
        continue;
      }
      const auto record_model{model_from_mask(spec.mask)};
      std::set<std::size_t> overlap;
      std::ranges::set_intersection(
          record_model, remaining, std::inserter(overlap, std::end(overlap)));
      if (overlap.empty()) {
        continue;
      }
      for (const auto value : record_model) {
        remaining.erase(value);
      }
      selected_model.push_back(index);
    }
    require(selected == selected_model,
            "the selected positions must match the model");
  });
}

// ---------------------------------------------------------------------------
// timestamp search
// ---------------------------------------------------------------------------

constexpr std::time_t min_generated_timestamp{1};
constexpr std::time_t max_generated_timestamp{12};

[[nodiscard]] binsrv::binlog_record_container
records_from_min_timestamps(const std::vector<std::time_t> &mins) {
  binsrv::binlog_record_container records;
  records.reserve(mins.size());
  for (const auto min_timestamp : mins) {
    binsrv::binlog_record record;
    const util::ctime_timestamp min_value{min_timestamp};
    record.timestamps =
        util::ctime_timestamp_range{min_value, min_value};
    records.push_back(std::move(record));
  }
  return records;
}

[[nodiscard]] std::vector<std::time_t>
draw_min_timestamps(hegel::TestCase &tc) {
  return tc.draw("mins",
                 gs::vectors(gs::integers<std::time_t>(
                     {.min_value = min_generated_timestamp,
                      .max_value = max_generated_timestamp})));
}

[[nodiscard]] bool timestamp_search_threw(
    const binsrv::binlog_record_container &records,
    const util::ctime_timestamp &timestamp,
    std::vector<std::size_t> &selected) {
  try {
    selected = operations::select_records_by_timestamp(records, timestamp);
    return false;
  } catch (const std::runtime_error &) {
    return true;
  }
}

BOOST_AUTO_TEST_CASE(TimestampSearchRejectsEmptyStorage) {
  run_property([](hegel::TestCase &tc) {
    const binsrv::binlog_record_container records;
    const util::ctime_timestamp timestamp{tc.draw(
        "ts", gs::integers<std::time_t>({.min_value = 0,
                                         .max_value = max_generated_timestamp +
                                                      1}))};
    std::vector<std::size_t> selected;
    require(timestamp_search_threw(records, timestamp, selected),
            "an empty storage must be rejected");
  });
}

BOOST_AUTO_TEST_CASE(TimestampSearchSelectsLeadingRun) {
  run_property([](hegel::TestCase &tc) {
    const auto mins{draw_min_timestamps(tc)};
    if (mins.empty()) {
      return; // exercised by TimestampSearchRejectsEmptyStorage
    }
    const auto records{records_from_min_timestamps(mins)};
    // span the range plus the ends, so "too old" and "all selected" both occur
    const std::time_t raw_target{tc.draw(
        "ts", gs::integers<std::time_t>(
                  {.min_value = min_generated_timestamp - 1,
                   .max_value = max_generated_timestamp + 1}))};
    const util::ctime_timestamp timestamp{raw_target};

    // independent model: the number of leading records with min <= target
    std::size_t leading{0U};
    while (leading != mins.size() && mins[leading] <= raw_target) {
      ++leading;
    }

    std::vector<std::size_t> selected;
    const bool threw{timestamp_search_threw(records, timestamp, selected)};
    require(threw == (leading == 0U),
            "the search fails exactly when the target is older than every "
            "record");
    if (threw) {
      return;
    }

    require(selected.size() == leading,
            "the number of selected records must match the model");
    for (std::size_t position{0U}; position != selected.size(); ++position) {
      require(selected[position] == position,
              "selected records must be the leading run 0, 1, ...");
      require(mins[position] <= raw_target,
              "a selected record's minimum timestamp must not exceed the "
              "target");
    }
    if (leading != mins.size()) {
      require(mins[leading] > raw_target,
              "the first excluded record's minimum timestamp must exceed the "
              "target");
    }
  });
}

} // namespace
