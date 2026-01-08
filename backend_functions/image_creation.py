import math
from textwrap import wrap
from html import escape
import pandas as pd
from backend_functions.database_functions import get_conn
from backend_functions.file_handlers import save_artifact


def render_task_summary_svg():
    # Render a horizontal stacked bar chart as SVG.
    # Returns SVG as string.

    sql = "SELECT * FROM tasks.vw_task_summary_chart"
    df = pd.read_sql(sql=sql, con=get_conn(alchemy=True))
    row_height = 28
    max_label_width = 180
    bar_area_width = 100
    title = "Task Summary"
    font_family = "Arial"
    font_size = 12
    padding = 12
    title_height = 32

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
        f'font-weight="bold">{escape(title)}</text>'
    )

    y_offset = title_height + padding

    # Compute max_total (retained, NaN-safe)
    totals = (
        df["median_extract_s"].fillna(0)
        + df["median_load_s"].fillna(0)
        + df["median_transform_s"].fillna(0)
    )
    max_total = max(float(totals.max()), 1.0)

    # Safe float helper (add if not defined elsewhere)
    def safe_float(value) -> float:
        try:
            v = float(value)
            return 0.0 if math.isnan(v) else v
        except (TypeError, ValueError):
            return 0.0

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
                style = 'font-style="italic" fill="#888888"'
            else:
                style = ''

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