---
feature-id: <id>
title: <title from request>
category: <New Capability | Enhancement | Bug Fix>
status: draft
created: <YYYY-MM-DD>
last-updated: <YYYY-MM-DD>
surfaces: <comma-separated screens, tables, endpoints>
points:
request: .features/descriptions/<id>.md
---

# Design: <title>

## Resume Here
**progress:** 0/0 tasks complete
**next:** review
**blocked-by:** OQ-1

## Architecture

### Surfaces
- <target>: <one-line lineage result>

### Approach
- <plain step>

### Data and Migrations
- <additive change, or None>

### API Changes
- <endpoint change, or None>

## Acceptance Criteria
- [ ] AC-1: <what you do> -> <what you should see> (covers B1)
  - auto: `<existing command | new test <path>::<name> [invariant|contract|smoke|journey] | none (<reason>)>`
- [ ] AC-2: <what you do> -> <what you should see> (covers B2)
  - auto: `<existing command | new test <path>::<name> [invariant|contract|smoke|journey] | none (<reason>)>`

## Open Questions
- **OQ-1** Confirm understanding
  - **Question:** In my words: <2-3 sentences: what changes for the user, what stays the same>. I am assuming: <assumption 1>; <assumption 2>. Is this right? Default: yes, this is right.
  - **Answer:** (to be provided)
  - **Decision:** (to be recorded)
  - **Rationale:** <what in the design changes if this is wrong>
  - **Status:** open

## Task Plan
Filled by /feature-plan.

<!--
Example task blocks (replace everything under this heading when planning):

### T01 Add rest-time formatter
- **Surface:** frontend
- **Skill:** react
- **Points:** 1
- **Depends-on:** none
- **Traces-to:** AC-1
- **Anchors:**
  - `create`: `frontend/src/lib/restTimer.ts`
  - `pattern`: `frontend/src/lib/duration.ts` `formatDuration`
- **Detail:** Add a helper that turns a number of seconds into the label shown on the rest timer.
  Interface: `formatRest(seconds: number): string` returns "m:ss", for example 75 -> "1:15"; negative input returns "0:00".
- **Verify:** npm --prefix frontend run typecheck
- **Status:** todo

### T02 Show the rest timer on the Workout screen
- **Surface:** frontend
- **Skill:** react
- **Points:** 1
- **Depends-on:** T01
- **Traces-to:** AC-1
- **Anchors:**
  - `modify`: `frontend/src/screens/Workout.tsx` `useEffect`
  - `planned` <- T01 `frontend/src/lib/restTimer.ts` `formatRest(seconds: number): string`
- **Detail:** Render the remaining rest time with formatRest and clear the interval in the effect cleanup so leaving the screen stops the timer.
- **Verify:** npx vitest run frontend/src/screens/Workout.test.tsx
- **Status:** todo
-->