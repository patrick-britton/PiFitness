---
feature-id: fix-activity-processing-fixes
title: Activity Processing fixes
category: Bug Fix
created: 2026-11-23
---

# Feature: Activity Processing fixes

## 1. Problem
- When launched from the activities/activity processing step, processing fails on step 1: Syncing activities, with an error: "Internal server error: name 'run_step' is not defined"
- Processing update bars (previously) never animated on mobile
- Processing takes upwards of 60 seconds sometimes, and more for longer activities (ideally wanted to be faster)

## 2. What Good Looks Like
- B1: Activities can actually be processed in a way that downloads all relevant details, flattens and loads data to the correct tables, calculates the tracks listened to if a playlist is selected, and matches all segments.
- B2: Status bars fill up based on time passing and estimated total time
- B3: Backend/db processes and stored procedures are evaluated for potential performance improvements

## 3. Out of Scope
- altering our match criteria so that we start accidentally matching activities against the wrong courses and segments
- changing how our race replay animations work in a way that reduces the "smoothness" of the animation.

## 4. Known Context (optional)
- I know the activity_details_distance table is huge and that every-meter granularity on the activities may be excessive -- so while I want to have faster processing, I don't want to sacrifice the accuracy of the matching or the race comparisons that are performed.

## 5. Open Questions (optional)
- None
