"""T11-bug probe (read-only): efficiency metrics for reported activity 24358931863."""
from backend_functions.database_functions import sql_to_dict

AID = 24358931863

# 1) The report's efforts-view row for this activity (what the UI receives).
rows = sql_to_dict(
    "SELECT segment_id, segment_name, is_course, avg_cadence, avg_stride_length,"
    " avg_gct, avg_vert_ratio, prior10_avg_cadence, prior10_avg_stride_length,"
    " prior10_avg_gct, prior10_avg_vert_ratio"
    " FROM activities.vw_report_segment_efforts WHERE activity_id = %s",
    (AID,),
)
print("report rows:", len(rows))
for r in rows:
    print(r)

# 2) Raw prior-10 rows behind the course's prior-10 averages (null distribution).
if rows:
    seg = rows[0]["segment_id"]
    p10 = sql_to_dict(
        "SELECT p2.activity_id, p2.start_time_utc, p2.avg_cadence,"
        " p2.avg_stride_length, p2.avg_gct, p2.avg_vert_ratio"
        " FROM activities.segments_details p2"
        " WHERE p2.segment_id = %s AND p2.start_time_utc <"
        " (SELECT start_time_utc FROM activities.segments_details"
        "  WHERE segment_id = %s AND activity_id = %s)"
        " ORDER BY p2.start_time_utc DESC LIMIT 10",
        (seg, seg, AID),
    )
    print(f"\nprior-10 rows for segment {seg}: {len(p10)}")
    for r in p10:
        print(r)
    for col in ("avg_cadence", "avg_stride_length", "avg_gct", "avg_vert_ratio"):
        nn = sum(1 for r in p10 if r[col] is not None)
        print(f"  non-null {col}: {nn}/{len(p10)}")
