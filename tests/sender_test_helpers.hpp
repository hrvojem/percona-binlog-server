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

#ifndef TESTS_SENDER_TEST_HELPERS_HPP
#define TESTS_SENDER_TEST_HELPERS_HPP

// helpers shared by the tests that read a storage through
// operations::sender_context

#include <stdexcept>

#include "operations/sender_context.hpp"
#include "operations/sender_context_fwd.hpp"
#include "operations/sender_single_file_context_fwd.hpp"

#include "util/byte_span.hpp"

namespace sender_test {

// Since PBS-120 a sender_context must pass check_for_non_empty_and_validate()
// before get_event(), which raises std::logic_error until then, as
// network_service::handle_binlog_dump_command() does once per dump. This runs
// the check before every read instead: once it has passed, a repeated check
// only confirms that the binlog being read is still in the storage, so the
// stream is the same. An empty storage ('still_empty') is reported like the
// end of the stored data ('waiting'), so a reader keeps polling as a blocking
// dump does; a refused dump throws, as get_event() does on an error.
[[nodiscard]] inline operations::event_fetching_result_type
get_validated_event(operations::sender_context &reader,
                    util::const_byte_span &event) {
  switch (reader.check_for_non_empty_and_validate()) {
  case operations::sender_context_check_result_type::ready:
    break;
  case operations::sender_context_check_result_type::still_empty:
    event = {};
    return operations::event_fetching_result_type::waiting;
  case operations::sender_context_check_result_type::
      error_source_purged_required_gtids:
    throw std::runtime_error{"the dump was refused: the storage has purged "
                             "GTIDs the replica requires"};
  case operations::sender_context_check_result_type::error_non_existing_binlog:
    throw std::runtime_error{
        "the dump was refused: the requested binlog does not exist"};
  }
  return reader.get_event(event);
}

} // namespace sender_test

#endif // TESTS_SENDER_TEST_HELPERS_HPP
