# Known Issues / Bugs - feat-dev-hub-rebuild


### BUG-1 — DevHub launch dependent on taskbar pinning clarity
- **AC:** AC-1
- **Symptom:** Untested -- unclear how to launch the dev hub without the required task-bar-pin
- **Root cause:** DevHub launch mechanism was not clearly explained before UAT, making it unclear how to test AC-1 without taskbar pin in place
- **Fix:** Provided desktop shortcut at Desktop\PiFitness DevHub.lnk, root create_desktop_shortcut.bat, and explicit launch instructions via dev_hub.bat or shortcut; files: tools/create-devhub-shortcut.ps1, create_desktop_shortcut.bat, tools/workflow/dev_hub.html, tools/workflow/dev_hub.py
- **Status:** in-review
- **Since:** 2026-11-22T14:38:32

### BUG-2 — Bootstrap Dev label should be Migrate to Test
- **AC:** AC-5
- **Symptom:** 'Bootstrap Dev' shouldn't be a thing. We're already in Dev -- it should be 'Migrate to Test' and 'Migrate to PRD'
- **Root cause:** DevHub buttons were labeled 'Bootstrap DEV' and 'Bootstrap PRD', but dev machine is already DEV and deployment targets are Pi test and prd environments
- **Fix:** Renamed buttons and backend actions to 'Migrate to Test' (test target) and 'Migrate to PRD' (prd target) in DevHub UI and backend; files: tools/workflow/dev_hub.html, tools/workflow/dev_hub.py
- **Status:** in-review
- **Since:** 2026-11-22T14:38:35

### BUG-3 — Missing clear instructions and shortcut creation for taskbar pinning
- **AC:** AC-6
- **Symptom:** No instructions provided for how to pin to taskbar.
- **Root cause:** Desktop shortcut was not automatically generated on desktop and explicit pin instructions were missing
- **Fix:** Generated Desktop\PiFitness DevHub.lnk, added interactive instructions, root create_desktop_shortcut.bat, and in-app 'Pin to Taskbar Helper'; files: tools/create-devhub-shortcut.ps1, create_desktop_shortcut.bat, tools/workflow/dev_hub.html, tools/workflow/dev_hub.py
- **Status:** in-review
- **Since:** 2026-11-22T14:38:43
