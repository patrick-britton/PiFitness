---
feature-id: fix-ensure-all-activities-process
title: Ensure All Activities Process
category: Bug Fix
created: 2026-11-26
---

# Feature: Ensure All Activities Process

## 1. Problem
- Activity ID # 25165738610 doesn't appear in recent activities
- Activity ID # 25165738610 never matched to the 'Beeler Summit' course

## 2. What Good Looks Like
- B1: Expectation is that -- even if the user is not manually processing the most recent activity, that all prior activities for walking, hiking, running, trail running in excess of 1000 meters are "processed" -- which is to say that the details are downloaded, and course matching is attempted.
- B2: The only exception to B1 is the most recent run / trail run -- this should only be processed manually to ensure the playlist can be captured correctly -- but if new runs appear that are more recent, then it should be autoprocessed by the activity details and activity detail post processing task executions.
- B3: Activities displayed on the recent activities should have a little button that initiates background postprocessing for that activity

## 3. Out of Scope
- None

## 4. Known Context (optional)
- None

## 5. Open Questions (optional)
- None
