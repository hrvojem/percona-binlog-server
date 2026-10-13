# Brief: property-catalog evaluation lens (Percona Binlog Server)

You are one of 4 evaluation agents. Fresh eyes: your bias is toward finding problems with the catalog, not defending it.

Read fully: antithesis/scratchbook/property-catalog.md, antithesis/scratchbook/sut-analysis.md, antithesis/scratchbook/deployment-topology.md. Paths for deeper context: antithesis/scratchbook/existing-assertions.md, antithesis/scratchbook/properties/ (evidence files — open when you need depth on a specific property), antithesis/scratchbook/property-relationships.md. Repo root /Users/hrvoje/work/percona-binlog-server (code read-only).

Method reference: /Users/hrvoje/.claude/plugins/cache/antithesis-skills/antithesis-skills/0.1.0/antithesis-research/references/property-evaluation.md — read the section for YOUR lens (and "Agent Output Format"); also validating-claims.md and faults.md in that directory.

External references (user's scope answer: "just this directory, whole binlog_server, use the open tickets"): https://claude.ai/artifact/9om6fiPd64QzVXbEWEtDT1 — QA bug-ticket list, local snapshot antithesis/scratchbook/.tickets-snapshot.txt; treat each ticket as a lead to validate, not a fact.

Instructions:
> Evaluate the property catalog through the lens of your assigned evaluation focus. Read the property evidence files when you need deeper context on a specific property. Produce concrete, specific findings — not vague concerns. Write your full analysis to the evidence file path provided, then return a structured summary. Begin your evidence file with the standard provenance frontmatter.

Frontmatter (exactly):
---
sut_path: /Users/hrvoje/work/percona-binlog-server
commit: 99de68207c84f83e0d477ebe63d2f531facbb954
updated: 2026-10-13
external_references:
  - path: https://claude.ai/artifact/9om6fiPd64QzVXbEWEtDT1
    why: QA bug-ticket list for binlog_server (local snapshot antithesis/scratchbook/.tickets-snapshot.txt); open tickets used as leads, each validated against the code before use
---

Do not edit any file other than your evidence file. Do NOT categorize findings as Gap/Bias/Refinement (synthesis does that).

Return (≤400 words): **Findings** ordered catalog-wide first, each with Property/Properties, Concern, Scope (catalog-wide | property-specific), Evidence (brief), Suggested action; **Passes**; **Uncertainties**.
