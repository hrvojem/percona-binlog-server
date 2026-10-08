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

#ifndef OPERATIONS_SEARCH_HELPERS_HPP
#define OPERATIONS_SEARCH_HELPERS_HPP

#include <cstddef>
#include <vector>

#include "binsrv/storage_core_fwd.hpp"

#include "binsrv/gtids/gtid_set_fwd.hpp"

#include "util/ctime_timestamp_fwd.hpp"

namespace operations {

// The record-selection logic of the search_by_gtid_set and
// search_by_timestamp operations, factored out of their execute() methods so
// that it can be tested directly (see tests/search_helpers_property_test.cpp).
// Both return the positions, in storage order, of the binlog records that the
// operation would report, and both throw std::runtime_error carrying the
// operation's user-facing messages on the error conditions, leaving the
// parsing of the arguments and the formatting of the response to the caller.

// Starting from 'target', walks the binlog records in order and, for every
// record whose GTIDs still intersect the GTIDs left to cover, consumes that
// record's whole GTID set and selects the record, stopping once nothing is
// left to cover. 'gtid_mode' is storage::is_in_gtid_replication_mode().
// Throws when the storage is empty, when it is not in GTID replication mode,
// or when 'target' is not fully covered by the records.
[[nodiscard]] std::vector<std::size_t>
select_records_by_gtid_set(const binsrv::binlog_record_container &records,
                           bool gtid_mode,
                           const binsrv::gtids::gtid_set &target);

// Selects the leading binlog records up to (but not including) the first one
// whose minimum event timestamp is greater than 'timestamp'. Throws when the
// storage is empty or when 'timestamp' is older than every record (nothing is
// selected).
[[nodiscard]] std::vector<std::size_t>
select_records_by_timestamp(const binsrv::binlog_record_container &records,
                            const util::ctime_timestamp &timestamp);

} // namespace operations

#endif // OPERATIONS_SEARCH_HELPERS_HPP
