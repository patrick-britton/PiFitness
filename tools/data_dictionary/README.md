# Data Lineage Tooling

The primary tool in this directory is `lineage.py`.

It statically parses the codebase (Python FastAPI, React/Next.js TypeScript/TSX, and SQL definitions) and queries the local DEV PostgreSQL catalog to build and maintain the project's data-lineage graph.

## Usage

### 1. Build / Refresh Lineage Graph (Run at start of session after DB sync)
```bash
python tools/data_dictionary/lineage.py build
```
Reads the local DEV database (`localhost:15432 / pifitness_dev`), dumps definitions to `docs/lineage/defs/`, and writes `docs/lineage/graph.json` and `docs/lineage/README.md`.

### 2. Query Lineage (Run during Feature Design & Plan workflows)
```bash
# Describe bidirectional dependencies and consumers of an object
python tools/data_dictionary/lineage.py describe table:activities.activities

# Trace upstream (where data comes from) or downstream (where data is displayed)
python tools/data_dictionary/lineage.py trace endpoint:GET /api/food/diary --up
python tools/data_dictionary/lineage.py trace table:food.diary_entries --down

# Search for nodes by name or column
python tools/data_dictionary/lineage.py search <words...>
```
