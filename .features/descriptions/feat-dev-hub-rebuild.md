---
feature-id: feat-dev-hub-rebuild
title: dev-hub-rebuild
category: Enhancement
created: 2026-11-22
---

# Feature: dev-hub-rebuild

## 1. Problem
- feature file move to zz_feature_archive is a copy, not a move -- leaving the files behind in the original folder
- Trying to launch distinct functions give me a "such and such is already running" or a "process has not completed" error
- Launching the bootstrapper seems to launch it but then immediately close a window

## 2. What Good Looks Like
- B1: The dev_hub (`dev_hub.bat`) app is a full GUI for the limited scope of commands that need to be run
- B2: Background processes and commands are launched, but not new windows
- B3: The two webpages are better linked -- and can be toggled by tab without having to launch any new services or open new windows
- B4: Bootstrapper is adjusted to not need user input -- always defaults to 'keep' the database if relevant, and separate commands can launch the test version or the prd version.
- B5: I can pin this application to my windows taskbar

## 3. Out of Scope
- None

## 4. Known Context (optional)
- This is purely a local desktop app. No deployment beyond this repo.

## 5. Open Questions (optional)
- None
