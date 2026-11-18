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

// Property-based tests for binsrv::logger_repeat_adapter (PBS-118), built with
// Hegel (https://hegel.dev). The adapter wraps a basic_logger; log() /
// log_format() pass through, while log_repeat() / log_format_repeat() count
// messages per 'group_id' and forward only the first one of each group until
// reset_group() re-arms it (reset_group() returns the number of messages
// counted since the previous reset). The sender uses it to write 'fetched EOF'
// and 'waiting for first binlog in empty storage' once per idle period.
//
// A recording basic_logger double captures every line that passes the
// severity filter, and a random sequence of operations (plain / repeat logs at
// random severities and group ids, resets, min-level changes) is applied to
// both the adapter and a trivial model:
//
//   counts : map<group_id, size_t>   (missing == 0)
//   log_repeat(g, level, m):  forwarded  iff counts[g] == 0 && level >= min;
//                             counts[g]++ even if the severity filter drops it
//   reset_group(g):           returns counts[g], counts[g] = 0
//
// Behaviour pinned here that is not obvious from the class comment:
// - a repeat message dropped by the severity filter still uses up its group's
//   "first message" slot (it is counted before the wrapped logger filters);
// - if the wrapped logger throws from do_log(), the group's counter has
//   already advanced, so the failed message is never retried and later
//   messages in the group stay suppressed until reset_group();
// - group ids beyond the inline capacity (4) grow the counter vector to
//   'group_id + 1' elements; an unallocatable id (2^40 .. SIZE_MAX - 1) throws
//   std::bad_alloc or boost::container::length_error (not derived from
//   std::length_error) and leaves the other groups intact; under
//   AddressSanitizer the 2^40.. range needs allocator_may_return_null=1;
// - group_id == SIZE_MAX makes 'group_id + 1' wrap to 0, so register_repeat()
//   shrinks the vector to 0 and indexes it out of bounds (BOOST_ASSERT abort
//   in a debug build, an out-of-bounds write before the inline buffer in a
//   release build). That case is in the disabled test case
//   'LogRepeatWithMaxGroupIdIsMemorySafe' (run it explicitly with
//   --run_test=LogRepeatWithMaxGroupIdIsMemorySafe); production code only
//   ever uses group id 0.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#define BOOST_TEST_MODULE LoggerRepeatAdapterPropertyTests
// this include is needed as it provides the 'main()' function
// NOLINTNEXTLINE(misc-include-cleaner)
#include <boost/test/unit_test.hpp>

#include <boost/test/unit_test_suite.hpp>

#include <hegel/hegel.h>

#include "property_test_helpers.hpp"

#include "binsrv/basic_logger.hpp"
#include "binsrv/log_severity.hpp"
#include "binsrv/logger_repeat_adapter.hpp"

namespace {

namespace gs = hegel::generators;

using binsrv::log_severity;
using property_testing::require;
using property_testing::run_property;

struct record {
  log_severity level;
  std::string message;
  bool operator==(const record &) const = default;
};

std::ostream &operator<<(std::ostream &output, const record &rec) {
  return output << '[' << rec.level << "] " << rec.message;
}

// a basic_logger that keeps every line that passed the severity filter,
// split back into (level, message); optionally throws from do_log()
class recording_logger final : public binsrv::basic_logger {
public:
  explicit recording_logger(log_severity min_level) noexcept
      : basic_logger{min_level} {}

  [[nodiscard]] const std::vector<record> &records() const noexcept {
    return records_;
  }
  void set_throw_on_log(bool value) noexcept { throw_on_log_ = value; }

private:
  std::vector<record> records_;
  bool throw_on_log_{false};

  void do_log(std::string_view line) override {
    if (throw_on_log_) {
      throw std::runtime_error{"recording_logger: injected do_log failure"};
    }
    // line format: "[<timestamp>] [<padded level>] <message>"
    const auto level_open{line.find("] [")};
    const auto level_close{line.find("] ", level_open + 3U)};
    if (level_open == std::string_view::npos ||
        level_close == std::string_view::npos) {
      throw std::logic_error{"unexpected log line format: " +
                             std::string{line}};
    }
    auto label{line.substr(level_open + 3U, level_close - level_open - 3U)};
    label.remove_prefix(std::min(label.find_first_not_of(' '), label.size()));
    log_severity level{log_severity::delimiter};
    std::istringstream label_stream{std::string{label}};
    label_stream >> level;
    records_.push_back(
        record{.level = level,
               .message = std::string{line.substr(level_close + 2U)}});
  }
};

enum class op_kind : std::uint8_t {
  log,
  log_format,
  log_repeat,
  log_format_repeat,
  reset_group,
  set_min_level
};

struct operation {
  op_kind kind;
  std::size_t group_id;
  log_severity level;
  std::uint32_t payload;
};

std::ostream &operator<<(std::ostream &output, const operation &op) {
  static constexpr std::array<std::string_view, 6U> names{
      "log",          "log_format",  "log_repeat", "log_format_repeat",
      "reset_group",  "set_min_level"};
  return output << names[static_cast<std::size_t>(op.kind)]
                << "(group=" << op.group_id << ", level=" << op.level
                << ", payload=" << op.payload << ')';
}

[[nodiscard]] gs::Generator<log_severity> severities() {
  return gs::sampled_from({log_severity::trace, log_severity::debug,
                           log_severity::info, log_severity::warning,
                           log_severity::error, log_severity::fatal});
}

// mostly ids around the inline capacity (4), sometimes far beyond it
[[nodiscard]] gs::Generator<std::size_t> group_ids() {
  return gs::one_of({gs::integers<std::size_t>({.min_value = 0U, .max_value = 7U}),
                     gs::integers<std::size_t>({.min_value = 0U, .max_value = 4096U}),
                     gs::just(std::size_t{65535U})});
}

[[nodiscard]] gs::Generator<operation> operations() {
  return gs::compose([](const hegel::TestCase &tc) {
    const auto kind{tc.draw(gs::sampled_from(
        {op_kind::log, op_kind::log_format, op_kind::log_repeat,
         op_kind::log_format_repeat, op_kind::reset_group,
         op_kind::set_min_level}))};
    return operation{
        .kind = kind,
        .group_id = tc.draw(group_ids()),
        .level = tc.draw(severities()),
        .payload = tc.draw(gs::integers<std::uint32_t>())};
  });
}

[[nodiscard]] std::string plain_message(const operation &op) {
  return "plain " + std::to_string(op.payload);
}
[[nodiscard]] std::string repeat_message(const operation &op) {
  return "repeat g" + std::to_string(op.group_id) + ' ' +
         std::to_string(op.payload);
}

struct model {
  log_severity min_level{log_severity::info};
  std::map<std::size_t, std::size_t> counts;
  std::vector<record> expected;
  std::vector<std::size_t> expected_resets;

  [[nodiscard]] bool passes(log_severity level) const {
    return level >= min_level;
  }
};

// applies 'ops' to a fresh adapter and to the model, compares the forwarded
// lines and every reset_group() return value
void check_against_model(const std::vector<operation> &ops,
                         log_severity initial_min_level) {
  const auto logger{std::make_shared<recording_logger>(initial_min_level)};
  binsrv::logger_repeat_adapter adapter{logger};
  require(adapter.get_underlying() == logger,
          "get_underlying() does not return the wrapped logger");

  model mdl{};
  mdl.min_level = initial_min_level;
  std::vector<std::size_t> actual_resets;

  for (const auto &op : ops) {
    switch (op.kind) {
    case op_kind::log:
      adapter.log(op.level, plain_message(op));
      if (mdl.passes(op.level)) {
        mdl.expected.push_back({op.level, plain_message(op)});
      }
      break;
    case op_kind::log_format:
      adapter.log_format(op.level, "plain {}", op.payload);
      if (mdl.passes(op.level)) {
        mdl.expected.push_back({op.level, plain_message(op)});
      }
      break;
    case op_kind::log_repeat:
    case op_kind::log_format_repeat:
      if (op.kind == op_kind::log_repeat) {
        adapter.log_repeat(op.group_id, op.level, repeat_message(op));
      } else {
        adapter.log_format_repeat(op.group_id, op.level, "repeat g{} {}",
                                  op.group_id, op.payload);
      }
      if (mdl.counts[op.group_id]++ == 0U && mdl.passes(op.level)) {
        mdl.expected.push_back({op.level, repeat_message(op)});
      }
      break;
    case op_kind::reset_group: {
      actual_resets.push_back(adapter.reset_group(op.group_id));
      const auto found{mdl.counts.find(op.group_id)};
      mdl.expected_resets.push_back(
          found == mdl.counts.end() ? 0U : std::exchange(found->second, 0U));
      break;
    }
    case op_kind::set_min_level:
      logger->set_min_level(op.level);
      mdl.min_level = op.level;
      break;
    }
  }

  if (logger->records() != mdl.expected) {
    std::ostringstream oss;
    oss << "forwarded lines differ from the model\n  actual  :";
    for (const auto &rec : logger->records()) {
      oss << "\n    " << rec;
    }
    oss << "\n  expected:";
    for (const auto &rec : mdl.expected) {
      oss << "\n    " << rec;
    }
    require(false, oss.str());
  }
  if (actual_resets != mdl.expected_resets) {
    std::ostringstream oss;
    oss << "reset_group() return values differ from the model:";
    for (std::size_t i{0U}; i < actual_resets.size(); ++i) {
      oss << "\n  #" << i << " actual " << actual_resets[i] << " expected "
          << mdl.expected_resets[i];
    }
    require(false, oss.str());
  }
}

} // anonymous namespace

// the main model test: any interleaving of plain / repeat logs, resets and
// severity changes matches the model
BOOST_AUTO_TEST_CASE(OperationSequencesMatchModel) {
  hegel::Settings settings{};
  settings.test_cases = 500U;
  run_property(
      [](hegel::TestCase &tc) {
        const auto initial_min_level{tc.draw("initial_min_level", severities())};
        const auto ops{tc.draw(
            "ops", gs::vectors(operations(), {.max_size = 200U}))};
        check_against_model(ops, initial_min_level);
      },
      settings);
}

// only the first of any number of repeat messages in a group is forwarded,
// and reset_group() reports how many were counted; after reset the next one
// is forwarded again
BOOST_AUTO_TEST_CASE(OnlyFirstRepeatPerIdlePeriodIsForwarded) {
  run_property([](hegel::TestCase &tc) {
    const auto group_id{tc.draw("group_id", group_ids())};
    const auto periods{tc.draw(
        "messages_per_period",
        gs::vectors(gs::integers<std::size_t>({.min_value = 0U, .max_value = 50U}),
                    {.min_size = 1U, .max_size = 10U}))};
    const auto logger{std::make_shared<recording_logger>(log_severity::info)};
    binsrv::logger_repeat_adapter adapter{logger};
    std::size_t expected_lines{0U};
    for (const auto count : periods) {
      for (std::size_t i{0U}; i < count; ++i) {
        adapter.log_format_repeat(group_id, log_severity::info,
                                  "sender : fetched EOF at binlog.{:06}:{}", 1U,
                                  i);
      }
      if (count != 0U) {
        ++expected_lines;
      }
      const auto counted{adapter.reset_group(group_id)};
      require(counted == count, "reset_group() returned " +
                                    std::to_string(counted) + ", expected " +
                                    std::to_string(count));
      require(logger->records().size() == expected_lines,
              "forwarded " + std::to_string(logger->records().size()) +
                  " line(s), expected " + std::to_string(expected_lines));
    }
    // the forwarded line is the first message of its period (i == 0)
    for (const auto &rec : logger->records()) {
      require(rec.message == "sender : fetched EOF at binlog.000001:0",
              "unexpected forwarded message: " + rec.message);
    }
  });
}

// resetting groups that were never used (including ids far beyond the
// current vector size and SIZE_MAX) returns 0 and does not disturb existing
// groups
BOOST_AUTO_TEST_CASE(ResetOfUnusedGroupIsHarmless) {
  run_property([](hegel::TestCase &tc) {
    const auto used{tc.draw("used_group", gs::integers<std::size_t>(
                                              {.min_value = 0U, .max_value = 7U}))};
    const auto unused{tc.draw(
        "unused_group",
        gs::one_of({gs::integers<std::size_t>({.min_value = 0U, .max_value = 4096U}),
                    gs::integers<std::size_t>(),
                    gs::just(std::numeric_limits<std::size_t>::max())}))};
    tc.assume(unused != used);
    const auto logger{std::make_shared<recording_logger>(log_severity::trace)};
    binsrv::logger_repeat_adapter adapter{logger};
    adapter.log_repeat(used, log_severity::info, "first");
    require(adapter.reset_group(unused) == 0U,
            "reset of an unused group returned non-zero");
    adapter.log_repeat(used, log_severity::info, "second");
    require(logger->records().size() == 1U,
            "reset of an unused group re-armed another group");
    require(adapter.reset_group(used) == 2U,
            "reset of an unused group changed another group's counter");
  });
}

// group ids that cannot be allocated (2^40 .. SIZE_MAX - 1) fail with an
// exception and leave the adapter usable with its existing counters
BOOST_AUTO_TEST_CASE(UnallocatableGroupIdFailsCleanly) {
  hegel::Settings settings{};
  settings.test_cases = 50U;
  run_property(
      [](hegel::TestCase &tc) {
        const auto huge{tc.draw(
            "huge_group",
            gs::integers<std::size_t>(
                {.min_value = std::size_t{1U} << 40U,
                 .max_value = std::numeric_limits<std::size_t>::max() - 1U}))};
        const auto logger{
            std::make_shared<recording_logger>(log_severity::trace)};
        binsrv::logger_repeat_adapter adapter{logger};
        adapter.log_repeat(1U, log_severity::info, "armed");
        bool threw{false};
        try {
          adapter.log_repeat(huge, log_severity::info, "huge");
        } catch (const std::bad_alloc &) {
          threw = true;
        } catch (const std::length_error &) {
          threw = true;
        } catch (const std::exception &) {
          // boost::container::length_error ('get_next_capacity, allocator's
          // max size reached') derives from std::exception, not from
          // std::length_error
          threw = true;
        }
        require(threw, "log_repeat with group id " + std::to_string(huge) +
                           " did not throw");
        require(logger->records().size() == 1U,
                "message for an unallocatable group was forwarded");
        adapter.log_repeat(1U, log_severity::info, "suppressed");
        require(adapter.reset_group(1U) == 2U,
                "a failed huge-id call disturbed another group's counter");
      },
      settings);
}

// if the wrapped logger throws, the group's counter has already advanced:
// the first message is lost and the following ones are suppressed until
// reset_group() (pins current behaviour, see the header comment)
BOOST_AUTO_TEST_CASE(ThrowingLoggerStillConsumesFirstSlot) {
  run_property([](hegel::TestCase &tc) {
    const auto group_id{tc.draw("group_id", group_ids())};
    const auto followers{tc.draw(
        "followers", gs::integers<std::size_t>({.min_value = 1U, .max_value = 20U}))};
    const auto logger{std::make_shared<recording_logger>(log_severity::trace)};
    binsrv::logger_repeat_adapter adapter{logger};
    logger->set_throw_on_log(true);
    bool threw{false};
    try {
      adapter.log_repeat(group_id, log_severity::info, "lost");
    } catch (const std::runtime_error &) {
      threw = true;
    }
    require(threw, "the injected do_log failure did not propagate");
    logger->set_throw_on_log(false);
    for (std::size_t i{0U}; i < followers; ++i) {
      adapter.log_repeat(group_id, log_severity::info, "follower");
    }
    require(logger->records().empty(),
            "a message after a failed first message was forwarded");
    require(adapter.reset_group(group_id) == followers + 1U,
            "reset_group() did not count the failed message");
    adapter.log_repeat(group_id, log_severity::info, "after reset");
    require(logger->records().size() == 1U,
            "reset_group() did not re-arm the group after a failure");
  });
}

// a null logger is rejected at construction
BOOST_AUTO_TEST_CASE(NullLoggerIsRejected) {
  BOOST_CHECK_THROW(binsrv::logger_repeat_adapter{binsrv::basic_logger_ptr{}},
                    std::invalid_argument);
}

// DISABLED: demonstrates the SIZE_MAX overflow in register_repeat()
// ('group_id + 1UZ' wraps to 0, the counter vector is shrunk to 0 elements
// and then indexed with SIZE_MAX). Expected to abort on BOOST_ASSERT in a
// debug build and to be reported by AddressSanitizer in an ASan build; a
// release build silently writes before the inline buffer.
BOOST_AUTO_TEST_CASE(LogRepeatWithMaxGroupIdIsMemorySafe,
                     *boost::unit_test::disabled()) {
  const auto logger{std::make_shared<recording_logger>(log_severity::trace)};
  binsrv::logger_repeat_adapter adapter{logger};
  adapter.log_repeat(0U, log_severity::info, "group 0");
  bool threw{false};
  try {
    adapter.log_repeat(std::numeric_limits<std::size_t>::max(),
                       log_severity::info, "max");
  } catch (const std::exception &) {
    threw = true;
  }
  BOOST_TEST(threw);
  // group 0 must still be suppressed (its counter must survive)
  adapter.log_repeat(0U, log_severity::info, "group 0 again");
  BOOST_TEST(logger->records().size() == 1U);
  BOOST_TEST(adapter.reset_group(0U) == 2U);
}
