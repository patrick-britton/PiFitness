---
feature-id: <feature-id>
title: <human-readable feature name>
source: .features/designs_active/<feature-id>_design.md
created: <YYYY-MM-DD>
---

# UAT Feedback: <title>

## Summary
- Total ACs: <n>
- Pass: <n> | Fail: <n> | Pending: <n>

## Per-AC Results
| AC | Result | Notes |
|----|--------|-------|
| AC-1 | [Pass] / [Fail] | |
| AC-2 | [Pass] / [Fail] | |

<!-- Add one row per Acceptance Criteria from the design file -->

## Export Notes
<free-form tester feedback, defects, observations>

## Generated from
`<feature-id>_uat.html` (produced by `python tools/workflow/fd.py uat build <feature-id>`)