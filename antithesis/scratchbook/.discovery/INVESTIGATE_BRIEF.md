# Brief: investigate open questions (Percona Binlog Server Antithesis research)

Repo /Users/hrvoje/work/percona-binlog-server (commit 99de682, branch antithesis-research). Read-only on source code. Do not run builds or tests. Write ONLY the evidence files you were assigned (antithesis/scratchbook/properties/<slug>.md). Do not edit property-catalog.md (the orchestrator syncs it from your summary).

Read: <refs>/validating-claims.md and <refs>/property-catalog.md sections "Open Questions Conventions" and "Investigation Log" (<refs> = /Users/hrvoje/.claude/plugins/cache/antithesis-skills/antithesis-skills/0.1.0/antithesis-research/references). Context: antithesis/scratchbook/sut-analysis.md (skim as needed), antithesis/scratchbook/existing-assertions.md (no Antithesis instrumentation exists; every suggested SUT assertion is "missing"), ticket snapshot antithesis/scratchbook/.tickets-snapshot.txt (tickets are leads), antithesis/scratchbook/deployment-topology.md (for questions about faults/containers).

For EACH assigned property:
- Read the evidence file's open questions and why each matters; trace the code to answer them. If a question asks whether a reported defect is real, confirm per validating-claims.md with discriminating primary evidence (file:line), not the report.
- Don't fabricate. Unresolvable from code/docs -> leave open, tag `(partial: <one-line>)` if partial findings, `(needs human input)` only after exhausting code/docs (e.g. "is purge-during-fetch in contract", "is node termination enabled for the tenant", maintainer intent).
- Append `### Investigation Log` at the bottom with one `####` per question: Examined / Found / Not found / Conclusion.
- Update the evidence file: remove resolved questions from its open-questions list (keep the log), record answers and their implications, correct instrumentation suggestions vs existing-assertions.md.
- If an answer changes the property (invariant, assertion type, scope, priority) or invalidates it, say so explicitly in the evidence file.

Reply (≤60 words per property) with, per slug: resolved questions (short), remaining questions WITH their tags exactly as they should appear in the catalog, property changed? (what), invalidated? (why).
