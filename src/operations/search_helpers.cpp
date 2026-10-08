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

#include "operations/search_helpers.hpp"

#include <cstddef>
#include <stdexcept>
#include <vector>

#include "binsrv/storage_core.hpp"

#include "binsrv/gtids/gtid_set.hpp"

#include "util/ctime_timestamp.hpp"

namespace operations {

[[nodiscard]] std::vector<std::size_t>
select_records_by_gtid_set(const binsrv::binlog_record_container &records,
                           bool gtid_mode,
                           const binsrv::gtids::gtid_set &target) {
  if (records.empty()) {
    throw std::runtime_error("Binlog storage is empty");
  }
  if (!gtid_mode) {
    throw std::runtime_error("GTID set search is not supported in storages "
                             "created in position-based replication mode");
  }

  binsrv::gtids::gtid_set remaining_gtids{target};
  std::vector<std::size_t> selected;
  for (std::size_t index{0U}; index != records.size(); ++index) {
    if (remaining_gtids.is_empty()) {
      break;
    }
    const auto &record{records[index]};
    if (!record.added_gtids.has_value()) {
      continue;
    }
    if (!binsrv::gtids::intersects(remaining_gtids, *record.added_gtids)) {
      continue;
    }
    remaining_gtids.subtract(*record.added_gtids);
    selected.push_back(index);
  }
  if (!remaining_gtids.is_empty()) {
    throw std::runtime_error("The specified GTID set cannot be covered");
  }
  return selected;
}

[[nodiscard]] std::vector<std::size_t>
select_records_by_timestamp(const binsrv::binlog_record_container &records,
                            const util::ctime_timestamp &timestamp) {
  if (records.empty()) {
    throw std::runtime_error("Binlog storage is empty");
  }

  std::vector<std::size_t> selected;
  for (std::size_t index{0U}; index != records.size(); ++index) {
    // stop at the first binlog file whose minimum timestamp is greater than
    // the provided one
    if (records[index].timestamps.get_min_timestamp() > timestamp) {
      break;
    }
    selected.push_back(index);
  }
  if (selected.empty()) {
    throw std::runtime_error("Timestamp is too old");
  }
  return selected;
}

} // namespace operations
