---
sut_path: /Users/hrvoje/work/percona-binlog-server
commit: 99de68207c84f83e0d477ebe63d2f531facbb954
updated: 2026-10-13
external_references:
  - path: https://claude.ai/artifact/9om6fiPd64QzVXbEWEtDT1
    why: QA bug-ticket list for binlog_server; open tickets used as leads for failure-prone areas (validated against code before use)
---

# Existing Antithesis Assertions

## Summary

The codebase has **no Antithesis instrumentation**. A case-insensitive search for
`antithesis` across `src/`, `tests/`, `extra/`, `cmake/` and `CMakeLists.txt`
finds nothing: no SDK header or library is included or linked, and no
`ALWAYS` / `SOMETIMES` / `REACHABLE` / `UNREACHABLE` (C++ SDK macros) or
`assert_*` calls exist.

Every SUT-side assertion suggested in the property evidence files is therefore
**missing** and would be new. The C++ SDK (`antithesis_sdk.h`, header-only) would
have to be added to the build, ideally behind a CMake option the way
`WITH_PROPERTY_TESTS` gates Hegel.

Existing non-Antithesis checks that overlap with suggested assertions:
- Hegel property tests under `tests/*_property_test.cpp` (in-process, no faults).
- QA black-box harnesses under `extra/qa/` (Python + Hypothesis, with
  fault injection via `fsfault`, Toxiproxy and `s3_fault_proxy.py`).
- MTR suite `mtr/binlog_streaming`.

## Assumptions

- The scan covers the source as of the commit above; vendored third-party
  code under `extra/mysql_protocol` was included in the search.

## Open Questions

- Whether to add the SDK as a fetched dependency (like Hegel) or vendor the
  single header.
