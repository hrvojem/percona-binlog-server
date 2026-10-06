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

// Property-based tests for binsrv::gtids::gtid_set, built with Hegel
// (https://hegel.dev).
//
// Most properties compare gtid_set against a trivially correct model - a
// std::set of (uuid index, tag index, gno) triples over small pools of UUIDs,
// tags and GNOs (small pools make adjacent / overlapping intervals and
// repeated TSIDs common). The remaining ones check round trips over the full
// value domain and that invalid input is rejected.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <ostream>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <boost/lexical_cast.hpp>

// needed for binsrv::gtids::gtid_set_storage
#include <boost/container/small_vector.hpp> // IWYU pragma: keep

#define BOOST_TEST_MODULE GtidSetPropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "property_test_helpers.hpp"

#include "binsrv/gtids/common_types.hpp"
#include "binsrv/gtids/gtid.hpp"
#include "binsrv/gtids/gtid_set.hpp"
#include "binsrv/gtids/tag.hpp"
#include "binsrv/gtids/uuid.hpp"

#include "util/byte_span_fwd.hpp"
#include "util/byte_span_inserters.hpp"

namespace {

namespace gs = hegel::generators;

using binsrv::gtids::gno_t;
using binsrv::gtids::gtid;
using binsrv::gtids::gtid_set;
using binsrv::gtids::tag;
using binsrv::gtids::uuid;

constexpr std::array uuid_pool{
    std::string_view{"11111111-aaaa-1111-aaaa-111111111111"},
    std::string_view{"22222222-bbbb-2222-bbbb-222222222222"},
    std::string_view{"33333333-cccc-3333-cccc-333333333333"}};
// index 0 is the empty (untagged) tag
constexpr std::array tag_pool{std::string_view{}, std::string_view{"alpha"},
                              std::string_view{"beta"}};
// kept small so that adjacent / overlapping intervals are common
constexpr gno_t model_max_gno{8ULL};

// <tag> ::= [a-zA-Z_][a-zA-Z0-9_]{0,31} (see the grammar in gtid_set.cpp)
constexpr std::string_view tag_pattern{"[a-zA-Z_][a-zA-Z0-9_]{0,31}"};

using model_gtid = std::tuple<std::size_t, std::size_t, gno_t>;
using model_type = std::set<model_gtid>;

using property_testing::require;
using property_testing::run_property;

[[nodiscard]] uuid pool_uuid(std::size_t index) {
  return uuid{uuid_pool.at(index)};
}
[[nodiscard]] tag pool_tag(std::size_t index) { return tag{tag_pool.at(index)}; }

[[nodiscard]] gs::Generator<std::size_t> uuid_indexes() {
  return gs::integers<std::size_t>(
      {.min_value = 0U, .max_value = std::size(uuid_pool) - 1U});
}
[[nodiscard]] gs::Generator<std::size_t> tag_indexes() {
  return gs::integers<std::size_t>(
      {.min_value = 0U, .max_value = std::size(tag_pool) - 1U});
}
[[nodiscard]] gs::Generator<gno_t> model_gnos(gno_t min_value) {
  return gs::integers<gno_t>(
      {.min_value = min_value, .max_value = model_max_gno});
}

// ---------------------------------------------------------------------------
// operations applied to both gtid_set and the model
// ---------------------------------------------------------------------------

enum class operation_kind : std::uint8_t {
  add_gtid,             // operator+=(const gtid &)
  add_components,       // add(uuid, tag, gno)
  add_interval,         // add_interval(uuid, tag, lower, upper)
  subtract_gtid,        // operator-=(const gtid &)
  subtract_components,  // subtract(uuid, tag, gno)
  subtract_interval     // subtract_interval(uuid, tag, lower, upper)
};

struct operation {
  operation_kind kind;
  std::size_t uuid_index;
  std::size_t tag_index;
  gno_t lower;
  gno_t upper;
};

std::ostream &operator<<(std::ostream &output, const operation &op) {
  static constexpr std::array kind_names{
      std::string_view{"add_gtid"},      std::string_view{"add_components"},
      std::string_view{"add_interval"},  std::string_view{"subtract_gtid"},
      std::string_view{"subtract_components"},
      std::string_view{"subtract_interval"}};
  output << kind_names.at(static_cast<std::size_t>(op.kind)) << "(u"
         << op.uuid_index << ", '" << tag_pool.at(op.tag_index) << "', "
         << op.lower;
  if (op.kind == operation_kind::add_interval ||
      op.kind == operation_kind::subtract_interval) {
    output << ", " << op.upper;
  }
  return output << ')';
}

[[nodiscard]] gs::Generator<operation> operations() {
  return gs::compose([](const hegel::TestCase &tc) {
    const auto kind{tc.draw(gs::sampled_from(
        {operation_kind::add_gtid, operation_kind::add_components,
         operation_kind::add_interval, operation_kind::subtract_gtid,
         operation_kind::subtract_components,
         operation_kind::subtract_interval}))};
    const auto uuid_index{tc.draw(uuid_indexes())};
    const auto tag_index{tc.draw(tag_indexes())};
    const auto lower{tc.draw(model_gnos(binsrv::gtids::min_gno))};
    auto upper{lower};
    if (kind == operation_kind::add_interval ||
        kind == operation_kind::subtract_interval) {
      upper = tc.draw(model_gnos(lower));
    }
    return operation{.kind = kind,
                     .uuid_index = uuid_index,
                     .tag_index = tag_index,
                     .lower = lower,
                     .upper = upper};
  });
}

void apply_operation(const operation &op, gtid_set &gtids, model_type &model) {
  const auto current_uuid{pool_uuid(op.uuid_index)};
  const auto current_tag{pool_tag(op.tag_index)};
  switch (op.kind) {
  case operation_kind::add_gtid:
    gtids += gtid{current_uuid, current_tag, op.lower};
    break;
  case operation_kind::add_components:
    gtids.add(current_uuid, current_tag, op.lower);
    break;
  case operation_kind::add_interval:
    gtids.add_interval(current_uuid, current_tag, op.lower, op.upper);
    break;
  case operation_kind::subtract_gtid:
    gtids -= gtid{current_uuid, current_tag, op.lower};
    break;
  case operation_kind::subtract_components:
    gtids.subtract(current_uuid, current_tag, op.lower);
    break;
  case operation_kind::subtract_interval:
    gtids.subtract_interval(current_uuid, current_tag, op.lower, op.upper);
    break;
  }

  const bool adding{op.kind == operation_kind::add_gtid ||
                    op.kind == operation_kind::add_components ||
                    op.kind == operation_kind::add_interval};
  for (gno_t gno{op.lower}; gno <= op.upper; ++gno) {
    const model_gtid element{op.uuid_index, op.tag_index, gno};
    if (adding) {
      model.insert(element);
    } else {
      model.erase(element);
    }
  }
}

struct built_gtid_set {
  gtid_set gtids;
  model_type model;
};

[[nodiscard]] built_gtid_set
build_from_operations(const std::vector<operation> &ops) {
  built_gtid_set result{};
  for (const auto &op : ops) {
    apply_operation(op, result.gtids, result.model);
  }
  return result;
}

// reads back which pool GTIDs a set actually contains, so that tests of
// binary operations do not depend on how their operands were built
[[nodiscard]] model_type model_of(const gtid_set &gtids) {
  model_type result{};
  for (std::size_t uuid_index{0U}; uuid_index < std::size(uuid_pool);
       ++uuid_index) {
    for (std::size_t tag_index{0U}; tag_index < std::size(tag_pool);
         ++tag_index) {
      for (gno_t gno{binsrv::gtids::min_gno}; gno <= model_max_gno + 1ULL;
           ++gno) {
        if (gtids.contains(
                gtid{pool_uuid(uuid_index), pool_tag(tag_index), gno})) {
          result.emplace(uuid_index, tag_index, gno);
        }
      }
    }
  }
  return result;
}

void require_matches_model(const gtid_set &gtids, const model_type &model) {
  // checking one GNO past the model range as well to catch intervals that
  // were extended too far
  for (std::size_t uuid_index{0U}; uuid_index < std::size(uuid_pool);
       ++uuid_index) {
    for (std::size_t tag_index{0U}; tag_index < std::size(tag_pool);
         ++tag_index) {
      for (gno_t gno{binsrv::gtids::min_gno}; gno <= model_max_gno + 1ULL;
           ++gno) {
        const gtid current{pool_uuid(uuid_index), pool_tag(tag_index), gno};
        const bool expected{model.contains({uuid_index, tag_index, gno})};
        require(gtids.contains(current) == expected,
                "contains(" + current.str() + ") returned " +
                    (expected ? "false" : "true") + " for the GTID set '" +
                    gtids.str() + "'");
      }
    }
  }
  require(gtids.is_empty() == model.empty(),
          "is_empty() disagrees with the model for the GTID set '" +
              gtids.str() + "'");
}

// ---------------------------------------------------------------------------
// arbitrary GTID sets over the full value domain
// ---------------------------------------------------------------------------

[[nodiscard]] uuid draw_uuid(const hegel::TestCase &tc) {
  // mixing pool UUIDs (so that TSIDs repeat and intervals merge) with fully
  // random ones
  if (tc.draw(gs::booleans())) {
    return pool_uuid(tc.draw(uuid_indexes()));
  }
  const auto raw{tc.draw(gs::arrays<std::uint8_t, binsrv::gtids::uuid_length>(
      gs::integers<std::uint8_t>()))};
  binsrv::gtids::uuid_storage storage{};
  std::ranges::transform(raw, std::begin(storage),
                         [](std::uint8_t value) { return std::byte{value}; });
  const uuid result{storage};
  // the nil UUID is not a valid GTID source
  tc.assume(!result.is_empty());
  return result;
}

[[nodiscard]] gs::Generator<gtid_set> arbitrary_gtid_sets() {
  return gs::compose([](const hegel::TestCase &tc) {
    static constexpr std::size_t max_intervals{8U};
    const auto number_of_intervals{tc.draw(
        gs::integers<std::size_t>({.min_value = 0U, .max_value = max_intervals}))};
    gtid_set result{};
    for (std::size_t index{0U}; index < number_of_intervals; ++index) {
      const auto current_uuid{draw_uuid(tc)};
      const auto tag_name{tc.draw(gs::one_of(
          {gs::just(std::string{}), gs::from_regex(std::string{tag_pattern})}))};
      const auto lower{tc.draw(gs::integers<gno_t>(
          {.min_value = binsrv::gtids::min_gno,
           .max_value = binsrv::gtids::max_gno}))};
      const auto upper{tc.draw(gs::integers<gno_t>(
          {.min_value = lower, .max_value = binsrv::gtids::max_gno}))};
      result.add_interval(current_uuid, tag{tag_name}, lower, upper);
    }
    return result;
  });
}

[[nodiscard]] gs::Generator<gno_t> out_of_range_upper_bounds() {
  // the first GNO above the valid range, typed as gno_t ('+ 1ULL' would make
  // it 'unsigned long long', a different type from 'std::uint64_t' on Linux)
  static constexpr gno_t first_out_of_range_gno{binsrv::gtids::max_gno + 1U};
  // the edges are listed explicitly: UINT64_MAX is where 'upper + 1'
  // (half-open interval conversion) wraps around
  return gs::one_of(
      {gs::just(first_out_of_range_gno),
       gs::just(std::numeric_limits<gno_t>::max()),
       gs::integers<gno_t>({.min_value = first_out_of_range_gno,
                            .max_value = std::numeric_limits<gno_t>::max()})});
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// agreement with the model
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(GtidSetOperationsMatchModel) {
  run_property([](hegel::TestCase &tc) {
    const auto ops{tc.draw("ops", gs::vectors(operations()))};
    const auto built{build_from_operations(ops)};
    require_matches_model(built.gtids, built.model);
  });
}

BOOST_AUTO_TEST_CASE(GtidSetContainsTagsMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const auto ops{tc.draw("ops", gs::vectors(operations()))};
    const auto built{build_from_operations(ops)};
    const bool expected{std::ranges::any_of(
        built.model,
        [](const model_gtid &element) { return std::get<1>(element) != 0U; })};
    require(built.gtids.contains_tags() == expected,
            "contains_tags() disagrees with the model for the GTID set '" +
                built.gtids.str() + "'");
  });
}

BOOST_AUTO_TEST_CASE(GtidSetUnionMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const auto first{
        build_from_operations(tc.draw("first_ops", gs::vectors(operations())))};
    const auto second{build_from_operations(
        tc.draw("second_ops", gs::vectors(operations())))};

    auto expected{model_of(first.gtids)};
    const auto second_model{model_of(second.gtids)};
    expected.insert(std::cbegin(second_model), std::cend(second_model));
    require_matches_model(first.gtids + second.gtids, expected);
  });
}

BOOST_AUTO_TEST_CASE(GtidSetDifferenceMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const auto first{
        build_from_operations(tc.draw("first_ops", gs::vectors(operations())))};
    const auto second{build_from_operations(
        tc.draw("second_ops", gs::vectors(operations())))};

    auto expected{model_of(first.gtids)};
    for (const auto &element : model_of(second.gtids)) {
      expected.erase(element);
    }
    require_matches_model(first.gtids - second.gtids, expected);
  });
}

BOOST_AUTO_TEST_CASE(GtidSetIntersectsMatchesModel) {
  run_property([](hegel::TestCase &tc) {
    const auto first{
        build_from_operations(tc.draw("first_ops", gs::vectors(operations())))};
    const auto second{build_from_operations(
        tc.draw("second_ops", gs::vectors(operations())))};

    const auto second_model{model_of(second.gtids)};
    const bool expected{std::ranges::any_of(
        model_of(first.gtids), [&second_model](const model_gtid &element) {
          return second_model.contains(element);
        })};
    require(intersects(first.gtids, second.gtids) == expected,
            "intersects('" + first.gtids.str() + "', '" + second.gtids.str() +
                "') disagrees with the model");
  });
}

BOOST_AUTO_TEST_CASE(GtidSetInsertionOrderIndependent) {
  run_property([](hegel::TestCase &tc) {
    const auto elements{tc.draw(
        "elements",
        gs::vectors(gs::tuples(uuid_indexes(), tag_indexes(),
                               model_gnos(binsrv::gtids::min_gno))))};
    gtid_set forward{};
    for (const auto &[uuid_index, tag_index, gno] : elements) {
      forward += gtid{pool_uuid(uuid_index), pool_tag(tag_index), gno};
    }
    gtid_set backward{};
    for (auto it{std::crbegin(elements)}; it != std::crend(elements); ++it) {
      const auto &[uuid_index, tag_index, gno]{*it};
      backward += gtid{pool_uuid(uuid_index), pool_tag(tag_index), gno};
    }
    require(forward == backward && forward.str() == backward.str(),
            "adding the same GTIDs in reverse order produced '" +
                backward.str() + "' instead of '" + forward.str() + "'");
  });
}

// ---------------------------------------------------------------------------
// round trips
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(GtidSetTextRoundTrip) {
  run_property([](hegel::TestCase &tc) {
    const auto gtids{tc.draw("gtids", arbitrary_gtid_sets())};
    const gtid_set restored{gtids.str()};
    require(restored == gtids, "parsing '" + gtids.str() + "' produced '" +
                                   restored.str() + "'");
  });
}

// the storage index / metadata persists GTID sets as JSON strings via
// boost::lexical_cast, which goes through operator<< / operator>>
BOOST_AUTO_TEST_CASE(GtidSetLexicalCastRoundTrip) {
  run_property([](hegel::TestCase &tc) {
    const auto gtids{tc.draw("gtids", arbitrary_gtid_sets())};
    const auto text{boost::lexical_cast<std::string>(gtids)};
    const auto restored{boost::lexical_cast<gtid_set>(text)};
    require(restored == gtids,
            "lexical_cast round trip of '" + text + "' produced '" +
                restored.str() + "'");
  });
}

BOOST_AUTO_TEST_CASE(GtidSetBinaryRoundTrip) {
  run_property([](hegel::TestCase &tc) {
    const auto gtids{tc.draw("gtids", arbitrary_gtid_sets())};
    binsrv::gtids::gtid_set_storage buffer(gtids.calculate_encoded_size());
    util::byte_span destination{buffer};
    gtids.encode_to(destination);
    require(destination.empty(),
            "encode_to() wrote fewer bytes than calculate_encoded_size() "
            "reported");

    const gtid_set restored{util::const_byte_span{buffer}};
    require(restored == gtids, "binary round trip of '" + gtids.str() +
                                   "' produced '" + restored.str() + "'");
  });
}

// ---------------------------------------------------------------------------
// rejection of invalid input
// ---------------------------------------------------------------------------

// GNOs must be in [min_gno, max_gno] (max_gno is INT64_MAX, as in MySQL
// Server); the textual form must not silently accept anything else
BOOST_AUTO_TEST_CASE(GtidSetParseRejectsOutOfRangeGno) {
  run_property([](hegel::TestCase &tc) {
    const auto gno_text{tc.draw(
        "gno_text",
        gs::one_of({gs::just(std::string{"0"}),
                    out_of_range_upper_bounds().map(
                        [](gno_t value) { return std::to_string(value); }),
                    // every such number is > UINT64_MAX >= max_gno
                    gs::from_regex("[1-9][0-9]{20,30}")}))};
    const std::string text{std::string{uuid_pool[0]} + ':' + gno_text};
    try {
      const gtid_set parsed{text};
      throw std::runtime_error{"'" + text + "' was accepted and parsed as '" +
                               parsed.str() + "'"};
    } catch (const std::invalid_argument &) {
      // expected
    }
  });
}

BOOST_AUTO_TEST_CASE(GtidSetAddIntervalRejectsOutOfRangeUpperBound) {
  run_property([](hegel::TestCase &tc) {
    const auto lower{tc.draw(
        "lower", gs::integers<gno_t>({.min_value = binsrv::gtids::min_gno,
                                      .max_value = binsrv::gtids::max_gno}))};
    const auto upper{tc.draw("upper", out_of_range_upper_bounds())};
    gtid_set gtids{};
    try {
      gtids.add_interval(pool_uuid(0U), tag{}, lower, upper);
    } catch (const std::invalid_argument &) {
      return;
    }
    throw std::runtime_error{"add_interval() accepted an upper bound above "
                             "max_gno, resulting in '" +
                             gtids.str() + "'"};
  });
}

// subtracting an interval that extends past max_gno is harmless, but it must
// then remove everything from 'lower' up to max_gno (or be rejected)
BOOST_AUTO_TEST_CASE(GtidSetSubtractIntervalBeyondMaxGnoRemovesTail) {
  run_property([](hegel::TestCase &tc) {
    const auto lower{tc.draw(
        "lower", gs::integers<gno_t>({.min_value = binsrv::gtids::min_gno,
                                      .max_value = binsrv::gtids::max_gno}))};
    const auto upper{tc.draw("upper", out_of_range_upper_bounds())};
    // a single contiguous interval, so that the result does not depend on
    // subtracting across several stored intervals
    gtid_set gtids{};
    gtids.add_interval(pool_uuid(0U), tag{}, lower, binsrv::gtids::max_gno);
    try {
      gtids.subtract_interval(pool_uuid(0U), tag{}, lower, upper);
    } catch (const std::invalid_argument &) {
      return;
    }
    require(gtids.is_empty(), "subtract_interval(" + std::to_string(lower) +
                                  ", " + std::to_string(upper) + ") left '" +
                                  gtids.str() + "'");
  });
}

// the binary form arrives from the network (PREVIOUS_GTIDS_LOG events): the
// decoder must either reject it or produce a set that can be persisted, that
// is printed and parsed back (see GtidSetLexicalCastRoundTrip)
BOOST_AUTO_TEST_CASE(GtidSetDecodedFromBinaryIsPrintable) {
  run_property([](hegel::TestCase &tc) {
    static constexpr std::size_t max_elements{4U};
    // untagged (8.0) encoding: <number_of_tsids> followed by
    // (<uuid> <number_of_intervals> (<lower> <upper>)*)*
    const auto tsids{tc.draw(
        "tsids",
        gs::vectors(
            gs::tuples(gs::arrays<std::uint8_t, binsrv::gtids::uuid_length>(
                           gs::integers<std::uint8_t>()),
                       gs::vectors(gs::tuples(gs::integers<std::uint64_t>(),
                                              gs::integers<std::uint64_t>()),
                                   {.max_size = max_elements})),
            {.max_size = max_elements}))};

    std::size_t encoded_size{sizeof(std::uint64_t)};
    for (const auto &[raw_uuid, intervals] : tsids) {
      encoded_size += std::size(raw_uuid) + sizeof(std::uint64_t) +
                      std::size(intervals) * 2U * sizeof(std::uint64_t);
    }
    binsrv::gtids::gtid_set_storage buffer(encoded_size);
    util::byte_span remainder{buffer};
    util::insert_fixed_int_to_byte_span(remainder,
                                        std::uint64_t{std::size(tsids)});
    for (const auto &[raw_uuid, intervals] : tsids) {
      util::insert_byte_array_to_byte_span(remainder, raw_uuid);
      util::insert_fixed_int_to_byte_span(remainder,
                                          std::uint64_t{std::size(intervals)});
      for (const auto &[lower, upper] : intervals) {
        util::insert_fixed_int_to_byte_span(remainder, lower);
        util::insert_fixed_int_to_byte_span(remainder, upper);
      }
    }

    gtid_set decoded{};
    try {
      decoded = gtid_set{util::const_byte_span{buffer}};
    } catch (const std::invalid_argument &) {
      return; // rejecting malformed input is fine
    }
    const auto text{decoded.str()};
    gtid_set restored{};
    try {
      restored = gtid_set{text};
    } catch (const std::exception &e) {
      throw std::runtime_error{"decoded GTID set '" + text +
                               "' cannot be parsed back: " + e.what()};
    }
    require(restored == decoded,
            "decoded GTID set '" + text + "' parsed back as '" +
                restored.str() + "'");
  });
}
