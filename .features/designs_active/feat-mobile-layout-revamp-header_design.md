---
feature-id: feat-mobile-layout-revamp-header
title: Mobile Layout Revamp, Header
category: New Capability
status: in-progress
created: 2026-11-21
last-updated: 2026-11-22
surfaces: component:frontend/pifitness/src/app/components/Header.tsx::Header, component:frontend/pifitness/src/app/components/Layout.tsx::Layout, component:frontend/pifitness/src/app/components/DebugPanel.tsx::DebugPanel, component:frontend/pifitness/src/app/admin/page.tsx::AdminPage, component:frontend/pifitness/src/app/admin/components/ThemeSettings.tsx::ThemeSettings, component:frontend/pifitness/src/app/components/ThemeToggle.tsx::ThemeToggle
points: 6
request: .features/descriptions/feat-mobile-layout-revamp-header.md
---

# Design: Mobile Layout Revamp, Header

## Resume Here
**progress:** 3/3 tasks complete
**next:** none
**blocked-by:** none

## Architecture

### Surfaces
- `component:frontend/pifitness/src/app/components/Header.tsx::Header`: `frontend/pifitness/src/app/components/Header.tsx`; `Header(): JSX.Element`; pattern `frontend/pifitness/src/app/components/ThemeToggle.tsx`; top bar containing brand title, viewport size, ThemeToggle, and debug button; deprecated and deleted.
- `component:frontend/pifitness/src/app/components/Layout.tsx::Layout`: `frontend/pifitness/src/app/components/Layout.tsx`; `Layout({ children }: LayoutProps): JSX.Element`; pattern `frontend/pifitness/src/app/layout.tsx`; remove Header and DebugPanel imports and JSX instances across all viewports.
- `component:frontend/pifitness/src/app/components/DebugPanel.tsx::DebugPanel`: `frontend/pifitness/src/app/components/DebugPanel.tsx`; `DebugPanel(): JSX.Element | null`; pattern `frontend/pifitness/src/app/components/Header.tsx`; debug panel component using `pifitness-debug` in localStorage; deprecated and deleted.
- `component:frontend/pifitness/src/app/admin/page.tsx::AdminPage`: `frontend/pifitness/src/app/admin/page.tsx`; `AdminPage(): JSX.Element`; pattern `frontend/pifitness/src/app/admin/page.tsx`; tabbed admin panel; add Theme tab configuration to `TABS`.
- `component:frontend/pifitness/src/app/admin/components/ThemeSettings.tsx::ThemeSettings`: `frontend/pifitness/src/app/admin/components/ThemeSettings.tsx` (new); `ThemeSettings(): JSX.Element`; pattern `frontend/pifitness/src/app/admin/components/ServiceConfig.tsx`; dedicated simple container displaying ThemeToggle.
- `component:frontend/pifitness/src/app/components/ThemeToggle.tsx::ThemeToggle`: `frontend/pifitness/src/app/components/ThemeToggle.tsx`; `ThemeToggle(): JSX.Element | null`; pattern `frontend/pifitness/src/app/components/Header.tsx`; relocated to dedicated Admin Theme view.

### Principles Alignment
- Frontend: `docs/UI_PRINCIPLES.md` (mobile-first responsive design, reclaiming vertical space, layout stability).

### Approach
1. Create `ThemeSettings.tsx` under `frontend/pifitness/src/app/admin/components/` rendering `ThemeToggle`.
2. Register the new Theme tab in `frontend/pifitness/src/app/admin/page.tsx`.
3. In `Layout.tsx`, remove imports and JSX elements for `Header` and `DebugPanel` across all viewports.
4. Delete `Header.tsx` and `DebugPanel.tsx` from `frontend/pifitness/src/app/components/`. Clean up any lingering references.
5. Verify application build and TypeScript types across the frontend.

### Data and Migrations
- None

### API Changes
- None

## Acceptance Criteria
- [ ] AC-1: Open the app on mobile portrait (360x740) and desktop (1440x900) -> The top Header bar is not rendered and main page content starts at the top of the viewport (covers B1)
  - auto: `powershell -NoProfile -Command "cd frontend\pifitness; npx tsc --noEmit"`
- [ ] AC-2: Inspect the UI and codebase -> The bug button (🐛) is absent, DebugPanel is removed from Layout and deleted, and localStorage 'pifitness-debug' is no longer toggled (covers B2)
  - auto: `powershell -NoProfile -Command "cd frontend\pifitness; npx tsc --noEmit"`
- [ ] AC-3: Navigate to Admin -> Theme tab on mobile (360x740) and desktop (1440x900) -> The ThemeToggle button is rendered on its own admin page and toggles between light and dark mode (covers B3)
  - auto: `powershell -NoProfile -Command "cd frontend\pifitness; npx tsc --noEmit"`

## Open Questions
- **OQ-1** Confirm understanding
  - **Question:** In my words: The top Header bar (displaying the "PiFitness" title, viewport dimensions, theme toggle, and debug button) is completely removed from the application shell, reclaiming vertical screen space. The debug button and DebugPanel component/functionality are deprecated and removed from the codebase. The light/dark mode ThemeToggle is relocated to the Admin menu on the DB Info page. Navigation (sidebar on desktop, bottom nav bar on mobile portrait, icon rail on mobile landscape) and all domain features (Activities, Food, Music, Health, Exercises, Admin) remain unchanged. I am assuming scope is bounded to B1 through B3. Is this right? Default: yes, this is right.
  - **Answer:** Confirmed
  - **Decision:** Scope is bounded to B1 through B3; header is removed, debug panel/button is deprecated and deleted, and theme toggle is relocated to Admin.
  - **Rationale:** Scope definition; establishes the core boundary for the feature.
  - **Status:** resolved

- **OQ-2** Viewport scope of Header removal
  - **Question:** The request problem notes that the app is primarily used on a smartphone, and B1 specifies "Header bar is gone". Should the top Header bar be removed across all viewports (desktop, portrait mobile, and landscape), or should it only be hidden on mobile screens? Default: Remove the Header bar across all viewports.
  - **Answer:** Go ahead and remove it from all viewports and deprecate all related code.
  - **Decision:** Remove Header unconditionally across all viewports (desktop, portrait, landscape) and deprecate all related header code.
  - **Rationale:** Determines whether Layout.tsx unconditionally omits Header or conditionally hides it only when isPortrait / isMobile.
  - **Status:** resolved

- **OQ-3** Placement of ThemeToggle on DB Info page
  - **Question:** B3 relocates the light/dark mode ThemeToggle to the DB Info page in Admin. Where on the DB Info page should it appear? Default: Place ThemeToggle in the top right header area of the DB Info container, alongside the sub-tab navigation on desktop and above the sub-tab selector on mobile.
  - **Answer:** Actually -- for now just give it it's own page in the admin section that only has a toggle button on it. Super simple and easy to move somewhere else later.  
  - **Decision:** Relocate ThemeToggle to its own dedicated tab ('Theme') in the Admin panel that displays only the toggle button.
  - **Rationale:** Determines the exact layout and visual positioning of ThemeToggle within DBInfo.tsx.
  - **Status:** resolved

## Task Plan

### T01 Add dedicated Theme tab to Admin panel
- **Surface:** frontend
- **Skill:** react
- **Points:** 2
- **Depends-on:** none
- **Traces-to:** AC-3
- **Anchors:**
  - `create`: `frontend/pifitness/src/app/admin/components/ThemeSettings.tsx`
  - `modify`: `frontend/pifitness/src/app/admin/page.tsx` `AdminPage`
  - `read`: `frontend/pifitness/src/app/components/ThemeToggle.tsx` `ThemeToggle`
- **Detail:** Create ThemeSettings component hosting ThemeToggle, and add a dedicated Theme tab to AdminPage. This relocates the theme toggle to the admin section as requested by the user.
  Interface: `default ThemeSettings(): JSX.Element` renders a card container with the ThemeToggle button.
  Finding: AdminPage in page.tsx defines TABS array on line 28; add `{ id: 'theme', label: 'Theme', icon: '🌓', component: ThemeSettings }`.
  Finding: ThemeToggle in ThemeToggle.tsx is a client component that manages light/dark mode via next-themes with hydration protection.
- **Verify:** powershell -NoProfile -Command "cd frontend\pifitness; npx tsc --noEmit"
- **Status:** complete

### T02 Remove Header and DebugPanel from Layout
- **Surface:** frontend
- **Skill:** react
- **Points:** 2
- **Depends-on:** none
- **Traces-to:** AC-1
- **Anchors:**
  - `modify`: `frontend/pifitness/src/app/components/Layout.tsx` `Layout`
  - `modify`: `frontend/pifitness/src/app/beachchanger/components/BeachViewer.tsx` `BeachViewer`
- **Detail:** In Layout.tsx, remove Header and DebugPanel imports and JSX elements so the top header bar is gone across all viewports. Clean up the kiosk comments in Layout.tsx and BeachViewer.tsx that reference DebugPanel.
  Finding: Layout.tsx renders Header on line 71 and DebugPanel on line 104; remove both so children content renders directly below the shell.
  Finding: Layout.tsx handles /beachchanger kiosk mode which previously hid Header; with Header gone, simplify chrome rendering while preserving desktop sidebar and mobile nav bar behavior.
  Finding: BeachViewer.tsx comment on lines 13-15 refers to DebugPanel staying for kiosk; update comment to reflect that DebugPanel is deprecated.
- **Verify:** powershell -NoProfile -Command "cd frontend\pifitness; npx tsc --noEmit"
- **Status:** complete

### T03 Delete deprecated Header and DebugPanel components
- **Surface:** frontend
- **Skill:** react
- **Points:** 2
- **Depends-on:** T02
- **Traces-to:** AC-2
- **Anchors:**
  - `delete`: `frontend/pifitness/src/app/components/Header.tsx` `Header`
  - `delete`: `frontend/pifitness/src/app/components/DebugPanel.tsx` `DebugPanel`
- **Detail:** Delete the deprecated Header.tsx and DebugPanel.tsx files from the codebase. With both components removed from Layout in T02, these files and the localStorage 'pifitness-debug' logic are fully deprecated and eliminated.
  Finding: Header.tsx held the 🐛 toggle button and 'pifitness-debug' localStorage write on lines 43-49.
  Finding: DebugPanel.tsx read and wrote 'pifitness-debug' localStorage on lines 52-64; neither are used anywhere else in the repository.
- **Verify:** powershell -NoProfile -Command "cd frontend\pifitness; npx tsc --noEmit"
- **Status:** complete
