# Brief for property-discovery agents (Percona Binlog Server)

Repo: /Users/hrvoje/work/percona-binlog-server (commit 99de68207c84f83e0d477ebe63d2f531facbb954). Read-only on code; write only where told.
Refs dir: /Users/hrvoje/.claude/plugins/cache/antithesis-skills/antithesis-skills/0.1.0/antithesis-research/references/

Read first:
1. antithesis/scratchbook/sut-analysis.md (the SUT analysis — required, read fully)
2. <refs>/property-catalog.md (catalog format, assertion choice, Honest Summaries, Open Questions conventions, Evidence Files)
3. <refs>/validating-claims.md and <refs>/faults.md
4. antithesis/scratchbook/existing-assertions.md (there is NO Antithesis instrumentation; every SUT-side assertion you suggest is "missing")

External references (scope answer: "just this directory, whole binlog_server, use the open tickets"):
- https://claude.ai/artifact/9om6fiPd64QzVXbEWEtDT1 — QA bug-ticket list; local snapshot antithesis/scratchbook/.tickets-snapshot.txt. Tickets 2, 21, 22 fixed; others open. A ticket is a lead: the sut-analysis "Bug history" table records which were validated in code (with file:line). Don't build a property on an unvalidated claim.

Instructions:
> Examine the codebase through the lens of your assigned attention focus. Produce properties in the standard catalog format. For each property, include your confidence (high/medium/low) and what evidence in the codebase supports it. If this focus yields no relevant properties for this system, say so and explain why. Before you build a property on a claim from an external source — an issue or a doc — validate it per validating-claims.md.

Antithesis context for this SUT: C++ SDK (antithesis_sdk.h: ALWAYS, ALWAYS_OR_UNREACHABLE, SOMETIMES, REACHABLE, UNREACHABLE). Workload will likely be Python (existing QA harness extra/qa/fetch_crash_test.py has reusable oracles) using the Python SDK. Likely containers: mysqld source, binlog_server (fetch or pull), S3-compatible server (MinIO/Versity), replica mysqld and/or workload client. Node termination and clock faults may be disabled by default — flag properties that need them.

Prefer timing-, concurrency-, crash- and partial-failure-dependent properties (Antithesis' strength). Deterministic input-parsing bugs (e.g. tickets 1, 3, 11, 12) are already covered by Hegel property tests — mention but don't prioritise.

Output:
- For EACH property, write an evidence file antithesis/scratchbook/properties/{slug}.md (descriptive kebab-case slug; if the file exists, another agent got there first — pick a different slug, do NOT overwrite). Content per property-catalog.md "Evidence Files": what led to it, code paths with file:line, failure scenario, key observations, SUT-side instrumentation suggestions each marked missing/partial/exists vs existing-assertions.md, and open questions with why they matter and what changes depending on the answer. No frontmatter in evidence files.
- Write all your catalog entries (standard format: ### slug — Name, table with Type/Property/Invariant/Antithesis Angle/Why It Matters, plus a Priority line (P0/P1/P2), Confidence, Fault requirements, and **Open Questions:** list mirroring the evidence file, untagged) to antithesis/scratchbook/.discovery/props-<NN>-<focus>.md, followed by sections: Areas examined; Cross-property assumptions.
- Reply with ≤120 words: list of slugs with one-line rationale each.
