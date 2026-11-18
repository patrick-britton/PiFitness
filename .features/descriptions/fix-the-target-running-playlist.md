---
feature-id: fix-the-target-running-playlist
title: Fix the target running playlist
category: Bug Fix
created: 2026-11-18
---

# Feature: Fix the target running playlist

## 1. Problem
- When sending a manual shuffle of the playlist to Spotify, the application is sending ALL songs from the parent playlist (400+ tracks) instead of an autoshuffled mix of cumulative-duration limited track (~20 to 30 tracks).

## 2. What Good Looks Like
- B1: When reshuffling tracks after completing a run and processing the activity, the correct, child playlist & appropriate settings are used to write the track
- B2: Verify that the same bug does not apply to the auto-shuffled versions of the playlist.

## 3. Out of Scope
- Any frontend feature -- this is almost certainly a python/integration logic problem, though maybe there are db changes needed as well.

## 4. Known Context (optional)
- This happens when the user processes an activity, sending shuffled tracks back to spotify for a manually shuffled playlist.

## 5. Open Questions (optional)
- None
