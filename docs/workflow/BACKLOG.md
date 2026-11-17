# PiFitness Backlog

Out-of-scope findings and deferred enhancements (Statement W9). Agents append new items at the bottom with the next sequential ID and never expand current feature scope.

| ID | Item | Source | Target Phase | Size | Status |
|---|---|---|---|---|---|
| **B1** | Fold `backend/backend_functions/` into `backend/pifitness/core/` and domain modules. Break reverse imports to `integrations/` and `worker/`. | REPO_MAP K1 | Phase 8 | Large | Open |
| **B20** | Unbounded-read risk: `get_usual_foods` scans all of `food.diary_entries` without limit; `get_log_data` selects `*` from logging tables without limit. Add pagination and bounded index scans. | Perf audit | Phase 8 | Medium | Open |
| **B23** | Tappable elements lack immediate active pressed state (`active:`, `onPressIn`). | UI Principles R3 | Phase 8 | Small | Open |
| **B24** | Secondary flows should use bottom sheets rather than full modal dialogs on mobile. | UI Principles R6 | Phase 8 | Small | Open |
| **B25** | Android back-gesture handling is absent in Next.js frontend (`useBackHandler` / `popstate`). | UI Principles R7 | Phase 8 | Small | Open |
| **B26** | PWA manifest (`manifest.json`) and service worker offline caching absent. | UI Principles R10 | Phase 8 | Small | Open |
| **B40** | Full fast-lane pytest collection raises `RuntimeError: APP_ENV=unset` when run outside explicit dev environment wrapper. | Test suite | Phase 6 | Small | Open |
| **B41** | GIT_MODEL.md still documents retired test-up/test-down legacy deploy flow (D3/D16); update to reference autodeploy.sh after legacy scripts retired in task 6.1 | task 6.1 | Phase 6 | Small | Open |

