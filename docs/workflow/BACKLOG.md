# PiFitness Backlog

Out-of-scope findings and deferred enhancements. Agents append new items at the bottom with `python tools/workflow/fd.py backlog add <item>`.

| ID | Item | Source | Target Phase | Size | Status |
|---|---|---|---|---|---|
| B1 | Fold `backend/backend_functions/` into `backend/pifitness/core/` and domain modules. | Architecture review | Backlog | Large | Open |
| B20 | Unbounded-read risk: add keyset pagination and bounded scans to historical diary queries. | Performance audit | Backlog | Medium | Open |
| B23 | Add active pressed visual states (`active:`, `onPressIn`) to touch controls per UI Principles R3. | UI Principles R3 | Backlog | Small | Open |
| B24 | Secondary flows should use bottom sheets rather than full page changes per UI Principles R6. | UI Principles R6 | Backlog | Small | Open |
| B25 | Android back-gesture handling for modal/sheet dismissal per UI Principles R7. | UI Principles R7 | Backlog | Small | Open |
| B26 | PWA web app manifest (`manifest.json`) and service worker offline caching per UI Principles R10. | UI Principles R10 | Backlog | Small | Open |
