import math
from textwrap import wrap
from html import escape
import pandas as pd
from backend_functions.database_functions import get_conn
from backend_functions.file_handlers import save_artifact
from backend_functions.helper_functions import safe_float


def render_task_summary_svg(dark_mode=False, mobile=False):
    # Render a horizontal stacked bar chart as SVG.
    # Returns SVG as string.

    sql = "SELECT * FROM tasks.vw_task_summary_chart"
    df = pd.read_sql(sql=sql, con=get_conn(alchemy=True))
    row_height = 28
    max_label_width = 180
    bar_area_width = 100 if mobile else 300
    title = "Task Summary"
    font_family = "Arial"
    font_size = 12
    padding = 5
    title_height = 32
    label_color = "#E6E6E6"  if dark_mode else "#1F2937" # primary text (near-white)
    secondary_label_color = "#9CA3AF" if dark_mode else "#6B7280" # muted gray


    bar_colors = {
        "extract": "#0000F5",    # blue
        "load": "#999999",       # gray
        "transform": "#f0690f"   # orange
    }

    num_tasks = len(df)

    chart_height = title_height + padding + num_tasks * row_height + padding
    chart_width = padding + max_label_width + padding + bar_area_width + padding

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{chart_width}" height="{chart_height}" '
        f'viewBox="0 0 {chart_width} {chart_height}">'
    ]

    # Title (retained)
    svg.append(
        f'<text x="{padding}" y="{title_height - 10}" '
        f'font-family="{font_family}" font-size="{font_size + 4}" '
        f'font-weight="bold" fill="{label_color}">'
        f'{escape(title)}</text>'
    )

    y_offset = title_height + padding

    # Compute max_total (retained, NaN-safe)
    totals = (
        df["median_extract_s"].fillna(0)
        + df["median_load_s"].fillna(0)
        + df["median_transform_s"].fillna(0)
    )
    max_total = max(float(totals.max()), 1.0)


    for _, row in df.iterrows():
        task_raw = str(row["task_name"])
        task = escape(task_raw)
        age_label_text = row.get("age_label", "")

        # --- Sanitize bar values ---
        extract = safe_float(row["median_extract_s"])
        load = safe_float(row["median_load_s"])
        transform = safe_float(row["median_transform_s"])

        # --- Row vertical center ---
        row_center = y_offset + row_height / 2

        # --- Wrap task name ---
        max_chars = max(1, int(max_label_width / (font_size * 0.6)))
        wrapped_task = wrap(task, max_chars)

        # Combine task name + age_label
        if age_label_text:
            wrapped_lines = wrapped_task + [age_label_text]
        else:
            wrapped_lines = wrapped_task

        num_lines = len(wrapped_lines)

        # --- Draw right-justified, vertically centered labels ---
        label_x = padding + max_label_width  # right edge of label

        for i, line in enumerate(wrapped_lines):
            # Center multiple lines around row_center
            line_y = row_center - (num_lines - 1)/2 * font_size + i * font_size

            # Italic + light gray for age_label
            if i == len(wrapped_lines) - 1 and age_label_text:
                style = f'font-style="italic" fill="{secondary_label_color}"'
            else:
                style = f'fill="{label_color}"'

            svg.append(
                f'<text x="{label_x}" y="{line_y}" '
                f'font-family="{font_family}" font-size="{font_size}" '
                f'text-anchor="end" dominant-baseline="middle" {style}>{escape(line)}</text>'
            )

        # --- Bar geometry (cursor-based, absolute-scaled) ---
        bar_x = padding + max_label_width + padding
        bar_y = row_center - font_size / 2  # vertically centered
        cursor_x = bar_x

        if extract > 0:
            extract_w = bar_area_width * (extract / max_total)
            svg.append(
                f'<rect x="{cursor_x}" y="{bar_y}" width="{extract_w}" '
                f'height="{font_size}" fill="{bar_colors["extract"]}" />'
            )
            cursor_x += extract_w

        if load > 0:
            load_w = bar_area_width * (load / max_total)
            svg.append(
                f'<rect x="{cursor_x}" y="{bar_y}" width="{load_w}" '
                f'height="{font_size}" fill="{bar_colors["load"]}" />'
            )
            cursor_x += load_w

        if transform > 0:
            transform_w = bar_area_width * (transform / max_total)
            svg.append(
                f'<rect x="{cursor_x}" y="{bar_y}" width="{transform_w}" '
                f'height="{font_size}" fill="{bar_colors["transform"]}" />'
            )

        # Advance to next row
        y_offset += row_height

    svg.append("</svg>")

    file_path = save_artifact(subdir='charting/tasks',
                  filename='task_summary',
                  content="\n".join(svg),
                  extension='svg')

    return file_path





    sql = "SELECT * FROM logging.vw_db_biggest_table_chart"
    t_df = pd.read_sql(sql=sql, con=get_conn(alchemy=True))


def render_db_size_summary(dark_mode=False, mobile=False):
    # -------------------------------------------------
    # Load data
    # -------------------------------------------------
    df = pd.read_sql(
        "SELECT * FROM logging.vw_db_size_chart",
        con=get_conn(alchemy=True)
    )

    t_df = pd.read_sql(
        "SELECT * FROM logging.vw_db_biggest_table_chart",
        con=get_conn(alchemy=True)
    )

    # Normalize dates (CRITICAL)
    df["chart_date"] = pd.to_datetime(df["date_utc"]).dt.date
    t_df["chart_date"] = pd.to_datetime(t_df["date_utc"]).dt.date

    # -------------------------------------------------
    # Layout
    # -------------------------------------------------
    bar_area_width = 120 if mobile else 360
    chart_height = 160
    line_chart_height = chart_height
    title_height = 32
    x_axis_height = 26
    y_axis_width = 56
    padding = 6
    gap_between_charts = 6

    font_family = "Arial"
    font_size = 12
    title = "Database Size"

    label_color = "#E6E6E6" if dark_mode else "#1F2937"
    secondary_label_color = "#9CA3AF" if dark_mode else "#6B7280"

    bar_colors = {
        "tables": "#0000F5",
        "indexes": "#f0690f",
        "other": "#999999"
    }

    line_colors = ["#10B981", "#6366F1", "#F43F5E"]

    # -------------------------------------------------
    # Truncate by width (1px/day minimum)
    # -------------------------------------------------
    if len(df) > bar_area_width:
        df = df.tail(bar_area_width)

    dates = df["chart_date"].tolist()
    date_to_index = {d: i for i, d in enumerate(dates)}

    num_days = len(df)
    bar_width = max(1, int(bar_area_width / num_days))

    # -------------------------------------------------
    # Scale computation
    # -------------------------------------------------
    df[["table_size_mb", "index_size_mb", "other_size_mb"]] = (
        df[["table_size_mb", "index_size_mb", "other_size_mb"]].fillna(0)
    )

    df["total_mb"] = (
            df["table_size_mb"]
            + df["index_size_mb"]
            + df["other_size_mb"]
    )

    max_mb = max(
        df["total_mb"].max() or 0,
        t_df["total_size_mb"].max() or 0,
        1
    )

    def scale_y(value_mb, top, height):
        return top + height - (value_mb / max_mb) * height

    # -------------------------------------------------
    # SVG geometry
    # -------------------------------------------------
    svg_width = y_axis_width + bar_area_width + padding * 2
    svg_height = (
            title_height +
            chart_height +
            x_axis_height +
            gap_between_charts +
            line_chart_height +
            padding * 2
    )

    chart_left = padding + y_axis_width

    bar_top = title_height + padding
    bar_bottom = bar_top + chart_height
    x_axis_y = bar_bottom

    line_top = x_axis_y + x_axis_height + gap_between_charts
    line_bottom = line_top + line_chart_height

    # -------------------------------------------------
    # SVG init
    # -------------------------------------------------
    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{svg_width}" height="{svg_height}">'
    ]

    # -------------------------------------------------
    # Title
    # -------------------------------------------------
    svg.append(
        f'<text x="{padding}" y="{title_height - 10}" '
        f'font-family="{font_family}" font-size="{font_size + 2}" '
        f'fill="{label_color}">{title}</text>'
    )

    # -------------------------------------------------
    # Stacked bars
    # -------------------------------------------------
    for i, row in enumerate(df.itertuples()):
        x = chart_left + i * bar_width
        y_cursor = bar_bottom

        for value, color in [
            (row.table_size_mb, bar_colors["tables"]),
            (row.index_size_mb, bar_colors["indexes"]),
            (row.other_size_mb, bar_colors["other"]),
        ]:
            if value <= 0:
                continue

            h = (value / max_mb) * chart_height
            y_cursor -= h

            svg.append(
                f'<rect x="{x}" y="{y_cursor}" '
                f'width="{bar_width - 1}" height="{h}" '
                f'fill="{color}" />'
            )

    # -------------------------------------------------
    # Y-axis (bars only)
    # -------------------------------------------------
    svg.append(
        f'<line x1="{chart_left}" y1="{bar_top}" '
        f'x2="{chart_left}" y2="{bar_bottom}" '
        f'stroke="{secondary_label_color}" />'
    )

    for i in range(5):
        mb = (max_mb / 4) * i
        y = scale_y(mb, bar_top, chart_height)

        svg.append(
            f'<line x1="{chart_left - 5}" y1="{y}" '
            f'x2="{chart_left}" y2="{y}" '
            f'stroke="{secondary_label_color}" />'
        )

        svg.append(
            f'<text x="{chart_left - 8}" y="{y + 4}" '
            f'text-anchor="end" font-family="{font_family}" '
            f'font-size="{font_size - 2}" fill="{secondary_label_color}">'
            f'{int(mb)}</text>'
        )

    # -------------------------------------------------
    # X-axis (bars only)
    # -------------------------------------------------
    svg.append(
        f'<line x1="{chart_left}" y1="{x_axis_y}" '
        f'x2="{chart_left + bar_area_width}" y2="{x_axis_y}" '
        f'stroke="{secondary_label_color}" />'
    )

    label_interval = max(1, int(num_days / 6))
    for i, row in enumerate(df.itertuples()):
        if i % label_interval != 0:
            continue

        x = chart_left + i * bar_width + bar_width / 2
        label = row.chart_date.strftime("%m/%d")

        svg.append(
            f'<text x="{x}" y="{x_axis_y + 16}" '
            f'text-anchor="middle" font-family="{font_family}" '
            f'font-size="{font_size - 2}" fill="{secondary_label_color}">'
            f'{label}</text>'
        )

    # -------------------------------------------------
    # Line chart (index-aligned)
    # -------------------------------------------------
    for idx, table in enumerate(t_df["db_table"].unique()):
        table_df = t_df[t_df["db_table"] == table]
        path = []
        drawing = False

        for _, row in table_df.iterrows():
            if row.chart_date not in date_to_index:
                drawing = False
                continue

            i = date_to_index[row.chart_date]
            x = chart_left + i * bar_width + bar_width / 2
            y = scale_y(row.total_size_mb, line_top, line_chart_height)

            if not drawing:
                path.append(f"M {x},{y}")
                drawing = True
            else:
                path.append(f"L {x},{y}")

        if path:
            svg.append(
                f'<path d="{" ".join(path)}" '
                f'fill="none" stroke="{line_colors[idx]}" '
                f'stroke-width="2" />'
            )

    # -------------------------------------------------
    # Close + save
    # -------------------------------------------------
    svg.append("</svg>")

    file_path = save_artifact(
        subdir="charting/tasks",
        filename="db_size_summary",
        content="\n".join(svg),
        extension="svg"
    )

    return file_path
