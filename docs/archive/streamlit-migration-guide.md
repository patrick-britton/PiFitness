# Streamlit → React Migration Guide

Use this reference when migrating a legacy Streamlit component to React.

## Approach
- **Read the legacy component first.** Identify what it renders, its widgets, and its data source. Streamlit often queries the database directly in Python; the React version must instead use the **Node API** (pre‑aggregated data). This usually spawns a `backend` task (new endpoint) and sometimes a `database` task — flag it to code-architect. Never query the DB from the frontend.
- **Behavioral parity, not a copy.** Understand what the component does, then rebuild to frontend‑dev standards (three layouts, dual‑theme, Chart.js/MapLibre). The React version will look and behave better.

## Construct Mapping

| Streamlit                        | React equivalent          |
|----------------------------------|---------------------------|
| `st.line_chart` / `st.bar_chart` / `st.altair_chart` | Chart.js (theme‑aware) |
| `st.map` / `st.pydeck_chart`     | MapLibre GL JS            |
| `st.dataframe` / `st.table`      | virtualized React table   |
| `st.selectbox` / `st.slider` / inputs | controlled React components |
| `st.session_state`               | React state / store       |
| top‑to‑bottom rerun model        | React data fetching + reactive state |

## Required Additions
Legacy Streamlit components are typically single‑column desktop with no dark mode. Every migrated component must also include:
- Mobile‑portrait layout
- Mobile‑landscape layout
- Full light/dark theming

## Decommission
Once a component is migrated and parity confirmed, hand the old Streamlit files and their now‑orphaned `pip` dependencies to **deployment-engineer** for safe removal.