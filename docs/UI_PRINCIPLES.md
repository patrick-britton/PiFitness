# UI Principles & Mobile Ergonomics (`UI_PRINCIPLES.md`)

This document defines the core user interface, interaction, and ergonomic principles for PiFitness. PiFitness is designed primarily as a mobile-first web application accessed from modern smartphones (roughly Pixel 9 Pro XL class viewports) over HTTPS, with desktop responsiveness.

Standard mobile reference viewports: `448x875` and `360x740`.

---

## 1. Navigation & Layout Principles

* **R1: Bottom Navigation within Thumb Reach:**
  * *Principle:* Primary navigation lives in a fixed bottom tab bar located within easy reach of the user's thumb.
  * *Reasoning:* Hand ergonomics on modern tall mobile screens make top-screen navigation difficult and uncomfortable for one-handed use.
* **R5: In-Place Contextual Results:**
  * *Principle:* Results and filter updates appear near the control that triggered them, not off-screen.
  * *Reasoning:* Off-screen rendering causes disorientation and missed feedback on tall, narrow mobile viewports.
* **R8: Edge-to-Edge with Safe Area Insets:**
  * *Principle:* The layout extends edge-to-edge while respecting device safe areas (system status bar, gesture navigation bar, hardware camera cutouts).
  * *Reasoning:* Content must never be obscured by mobile chrome or display cutouts. Next.js viewports use `viewportFit: "cover"` with `env(safe-area-inset-*)` padding.
* **R11: Zero Horizontal Scrolling:**
  * *Principle:* Pages and containers must never trigger horizontal scroll at mobile viewports (`document.documentElement.scrollWidth <= viewportWidth`).
  * *Reasoning:* Horizontal scroll breaks vertical touch navigation and masks critical information.

---

## 2. Touch Targets & Interaction Feedback

* **R2: Minimum 44px Touch Targets:**
  * *Principle:* All interactive controls (buttons, links, form inputs, icon toggles) must provide a touch target of at least 44x44px, with clear visual separation from adjacent interactive elements.
  * *Reasoning:* Prevents mis-taps and user frustration on high-DPI touchscreens.
* **R3: Immediate Pressed Visual Feedback:**
  * *Principle:* Every tappable element must provide an immediate visual active/pressed state upon touch down.
  * *Reasoning:* Touchscreens lack mouse hover states. Instant visual confirmation reassures the user that the tap was registered before async operations begin.
* **R4: Explicit Async Status & Error Feedback:**
  * *Principle:* Every asynchronous action must display an immediate loading indicator, followed by distinct success or error feedback.
  * *Reasoning:* Users must never be left wondering whether an action is in flight or has completed.
* **R6: Bottom Sheets for Secondary Flows:**
  * *Principle:* Secondary flows (quick edits, item details, filtering panels) open as sliding bottom sheets rather than navigating to full new pages.
  * *Reasoning:* Bottom sheets preserve the user's primary context and make dismissal fast and intuitive via swipe-down gestures.

---

## 3. UI States & Data Completeness

* **R9: Comprehensive State Design:**
  * *Principle:* Every view and list component must explicitly handle and render four distinct states:
    1. *Loading state:* Skeleton loaders or progress spinners matching the final layout shape.
    2. *Empty state:* Clear message with actionable guidance when zero items exist.
    3. *Populated state:* The standard view containing user data.
    4. *Error state:* Clear description of failure with a manual retry button.
  * *Reasoning:* A blank or unresponsive screen during network failure or initial empty state degrades user trust.

---

## 4. Platform, PWA & Form Inputs

* **R7: Gesture Navigation Integration:**
  * *Principle:* The Android/system back gesture must dismiss the topmost overlay, modal, or bottom sheet before navigating backward in browser history.
  * *Reasoning:* Prevents accidental navigation out of the app when the user simply intends to close a pop-up.
* **R10: Installable PWA over HTTPS:**
  * *Principle:* The frontend provides a valid web app manifest, icons, and HTTPS service configuration enabling home screen installation.
  * *Reasoning:* PWA installation provides full-screen app-like ergonomics on mobile devices without app store overhead.
* **R12: Semantic Input Types & Modes:**
  * *Principle:* Form inputs must use semantic HTML input types (`number`, `email`, `date`, `search`) and `inputMode="numeric"` for numerical entry.
  * *Reasoning:* Triggers the appropriate virtual keyboard layout on mobile devices and enables native browser validation.
* **R13: Clean Browser Console:**
  * *Principle:* Application pages and interactions must execute without unhandled JavaScript exceptions or console errors.
  * *Reasoning:* Console errors indicate broken resource loading, unhandled API states, or failing scripts that degrade the mobile experience.
