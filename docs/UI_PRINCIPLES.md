# UI_PRINCIPLES

Audience: frontend engineers and reviewers; anyone checking the UI on a phone. Target devices: modern phones of roughly Pixel 9 Pro XL class; rules are expressed relative to viewport width and safe areas, not pinned to one device's dimensions.
Standard test viewports (statement V5): 448x875 and 360x740.
Each rule names how it is checked. auto = a script can check it. human = needs a person on the phone. Rules marked auto-candidate are checkable but no check exists yet.

## Navigation and layout

| ID | Rule | Why (one line) | Check | How it is checked | Observed in code |
|---|---|---|---|---|---|
| **Navigation and layout** | | | | | |
| R1 | Primary navigation is a bottom tab bar within thumb reach. | Thumb reach is the main ergonomic constraint on phones of any size. | auto | Headless Playwright Chromium at 448x875 and 360x740 (tools/probe/probe5.py pattern): assert the bottom nav is fixed at the bottom and its bounding box sits inside the lower third of the viewport at both viewports. | frontend/pifitness/src/app/components/Layout.tsx:98 |
| R8 | The layout is edge-to-edge and respects safe areas (status bar, gesture bar, display cutouts). | Phone chrome and cutouts must not cover content on any device. | auto | Playwright at both viewports: assert the Next.js viewport config sets viewportFit "cover" and the bottom nav applies env(safe-area-inset-bottom) padding. | frontend/pifitness/src/app/layout.tsx:14-17 |
| R5 | Results appear where the eye already is (near the control that triggered them), not off-screen. | Off-screen results are missed on tall, narrow phone screens. | human | On the phone, open a search or filter view, tap the control, and confirm results render beside or under that control rather than elsewhere on the screen. | not observed |

## Touch and feedback

| **Touch and feedback** | | | | | |
| R2 | Touch targets are at least 44px in both dimensions, with adequate spacing between neighbors. | Fingers need >=44px targets with separation to avoid mis-taps. | auto | Playwright at both viewports: assert all buttons, links and inputs have bounding boxes >=44px in both dimensions, with gaps between neighbors. | frontend/pifitness/src/app/activities/beach/components/BeachVolleyball.tsx:413 |
| R3 | Every tappable element shows an immediate pressed state. | Touch has no hover, so a pressed visual confirms the tap landed. | human | On the phone, tap every button and link: each must show a pressed visual before navigation or action follows. | not observed |
| R4 | Every async action shows loading, then success or error feedback. | Users must know an action is in flight and how it ended. | human | Trigger an async action on the phone: confirm a loading indicator appears at once, then success or error replaces it. | frontend/pifitness/src/app/food/components/food-ui.tsx:17 |
| R6 | Secondary flows use bottom sheets, not page changes. | Overlays preserve context on small screens; page changes lose it. | human | Open a secondary flow on the phone and confirm it opens as a bottom sheet rather than a new page. | not observed |

## States and content

| **States and content** | | | | | |
| R9 | Every list or view has designed loading, empty and error states. | Users must see meaningful UI in every data state. | auto-candidate | Scan each list/view for loading/skeleton, empty and error components; no view enumeration or check script exists yet — Phase 2 verify gate or Phase 8 mobile checks to build. | frontend/pifitness/src/app/food/components/RecipeBox.tsx:114-502 |

## Platform and PWA

| **Platform and PWA** | | | | | |
| R7 | The Android back gesture closes the topmost overlay or sheet before navigating. | The system back gesture must dismiss overlays instead of exiting the app. | human | On the phone, open an overlay or sheet, perform the back gesture, and confirm the overlay closes before any navigation. | not observed |
| R10 | The app is installable as a PWA over HTTPS. | Installability is the mobile distribution path (statement E5). | human | On the phone over HTTPS, install from Chrome and confirm the app icon appears on the home screen. | not observed — frontend/pifitness/public contains only wasm/ (no manifest.json or service worker) and src/app has no manifest or service-worker registration |
| R11 | No horizontal scroll at the two test viewports (448x875, 360x740). | Horizontal scroll breaks mobile layouts and hides content. | auto | Playwright at both viewports: assert document.documentElement.scrollWidth <= viewport width after load. | not observed |
| R12 | Correct input types (numeric, email, search, etc.) on every input. | Correct types bring the right phone keyboard and built-in validation. | auto | Playwright: assert every <input> carries a type matching its data (number, email, search, date, ...) with inputMode on numeric inputs. | frontend/pifitness/src/app/activities/beach/components/BeachVolleyball.tsx:409-413 |
| R13 | Clean browser console (no errors) at both test viewports. | Console errors mark broken resources or failing scripts on phones. | auto | Playwright at both viewports: navigate and interact, then assert the console records no errors. | not observed |
