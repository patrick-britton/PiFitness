# Streamlit to Backend Mapping

Generated: 2026-06-24T18:43:00.000000

## Summary

- **Total Modules:** 6
- **Backend Functions Used:** 0
- **Database Calls:** 5
- **Frontend Helpers:** 0

## Module Details

### Home Module

- **Status:** ✅ Analyzed
- **File:** `frontend_functions/homepage.py`
- **Render Function:** `render_homepage`
- **Database Calls:**
  - `one_sql_result`

### Admin Module

- **Status:** ✅ Analyzed
- **File:** `frontend_functions/admin_module.py`
- **Render Function:** `render_admin_module`
- **Database Calls:**
  - `sql_to_list`
  - `sql_to_dict`
  - `qec`
  - `get_conn`

### Music Module

- **Status:** ✅ Analyzed
- **File:** `frontend_functions/music_module.py`
- **Render Function:** `render_music`
- **Database Calls:**
  - `one_sql_result`
  - `sql_to_dict`
  - `qec`
  - `get_conn`

### Health Module

- **Status:** ✅ Analyzed
- **File:** `frontend_functions/health_module.py`
- **Render Function:** `render_health_module`
- **Database Calls:**
  - `qec`
  - `get_conn`

#### Concrete Code Examples

**1. Weight Target Management:**
```python
# Database Query Example
tgt_sql = 'SELECT ts_utc, round(weight_total_g*0.00220462,1) as weight_lb FROM health.weight_target order by ts_utc desc'
ss.tgt_df = pd.read_sql_query(tgt_sql, con=get_conn(alchemy=True))

# Database Insert Example
date = str(date)
weight = int(round(weight/0.00220462,0))
sql = f"""INSERT INTO health.weight_target (ts_utc, weight_total_g)
            VALUES (%s::TIMESTAMPTZ, %s)
            ON CONFLICT (ts_utc) DO UPDATE SET
            weight_total_g = EXCLUDED.weight_total_g;"""
returns = qec(sql,[date,weight])
```

**2. Health Charting with Dynamic SQL:**
```python
# Dynamic SQL Generation for Weight Visualization
sql = f"SELECT {xaxis}, {xlimit}"
for y in yaxis:
    sql = f"{sql}, {y}"
sql = f"{sql} FROM health.vw_weight_viz WHERE {xlimit} > -{history_limit} ORDER BY {xlimit} ASC"

# Example parameters:
# xaxis = 'day_of_year', xlimit = 'relative_year'
# yaxis = ['total_lb', 'tgt_lb'], history_limit = 4
# Result: "SELECT day_of_year, relative_year, total_lb, tgt_lb FROM health.vw_weight_viz WHERE relative_year > -4 ORDER BY relative_year ASC"
```

**3. Photo Metadata Storage:**
```python
# File upload and database insert
ins_sql = f"""INSERT INTO health.photo_metadata (photo_type, file_name)
            VALUES (%s, %s)"""
params = [photo_type, fn]
qec(ins_sql, params)
```

**4. Body Dimensions Tracking:**
```python
# Multiple metric insert
ins_sql = """INSERT INTO health.body_dimensions(butt_cm, waist_cm, stomach_cm, chest_cm, neck_cm)
            VALUES (%s, %s, %s, %s, %s)"""
params = [butt_cm, waist_cm, stomach_cm, chest_cm, neck_cm]
qec(ins_sql, params)
```

#### Database Tables Accessed:
- `health.weight_target` - Weight target tracking
- `health.vw_weight_viz` - Weight visualization view
- `health.photo_metadata` - Photo upload metadata
- `health.body_dimensions` - Body measurement tracking

### Running Module

- **Status:** ✅ Analyzed
- **File:** `frontend_functions/running_module.py`
- **Render Function:** `render_running_module`
- **Database Calls:**
  - `one_sql_result`
  - `qec`
  - `get_conn`

### Food Module

- **Status:** ❌ Not Found
- **File:** `frontend_functions/running_module.py`
- **Render Function:** `None`
- **Database Calls:**
  - `one_sql_result`
  - `qec`
  - `get_conn`

## Data Flow Analysis

## Health Module Analysis

The Health module is one of the most comprehensive modules in the PiFitness application, handling four main functional areas:

### 1. Weight Target Management
- **Purpose:** Track and manage weight goals over time
- **Database Operations:**
  - Read: `SELECT ts_utc, round(weight_total_g*0.00220462,1) as weight_lb FROM health.weight_target`
  - Write: `INSERT INTO health.weight_target (ts_utc, weight_total_g) VALUES (...) ON CONFLICT DO UPDATE`
- **User Interaction:** Date picker and weight input with save functionality

### 2. Health Charting
- **Purpose:** Visualize weight and body composition trends
- **Database Operations:**
  - Dynamic SQL generation based on user-selected parameters (time scale, metrics, history period)
  - Queries `health.vw_weight_viz` view which aggregates weight data
- **Visualization:** Uses `render_weight_viz` from `viz_factory.body_comp`

### 3. Photo Documentation
- **Purpose:** Track progress photos (front and side views)
- **Database Operations:**
  - File system storage of uploaded images
  - Metadata storage in `health.photo_metadata` table
- **User Interaction:** File upload interface with preview and save

### 4. Body Dimensions Tracking
- **Purpose:** Track body measurements for progress monitoring
- **Database Operations:**
  - Insert into `health.body_dimensions` table with 5 measurements
- **User Interaction:** Form with 5 measurement inputs (butt, waist, stomach, chest, neck)

### Key Backend Dependencies:
- `backend_functions.database_functions.qec` - Primary query execution method
- `backend_functions.database_functions.get_conn` - Connection management
- `backend_functions.viz_factory.body_comp.render_weight_viz` - Visualization rendering

### Data Flow:
1. User interacts with Streamlit UI elements
2. Frontend collects input and constructs SQL queries
3. Database functions execute queries and return results
4. Results are displayed in tables or visualizations
5. User can add/update data through form submissions