#!/usr/bin/env python3
"""
Analyze a schedule workbook with workers in rows and days in columns.

Expected layout:
  - One row per worker.
  - Day columns named Day_1 ... Day_378 (also accepts Day 1, day1, etc.).
  - The final two rows are excess and deficit, in that order, unless rows with
    labels containing "excess" or "deficit" are detected.
  - The first column contains worker IDs / row labels.

Outputs (written to --output-dir):
  - excess_deficit_per_day.png
  - c2d_cumulative_per_worker.png
  - c3d_cumulative_per_worker.png
  - worker_counters.csv
  - daily_excess_deficit.csv

Counter rules use displayed day numbers directly:
  c2d: LD, TC, or LQ when day // 7 == 6
  c3d: (LQ when day // 7 == 6) PLUS (LQ or LD when day // 7 is 5 or 6)

The c3d rule is implemented as the sum of those two conditions. Therefore, an
LQ on a day where day // 7 == 6 contributes twice, as specified by "plus".
All counter contributions are restricted to days 12 through 376 inclusive.
"""

from __future__ import annotations

import argparse
import re
import webbrowser
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import re
from plotly.subplots import make_subplots
import plotly.express as px

FIRST_DAY = 1
LAST_DAY = 378
COUNTER_FIRST_DAY = 12
COUNTER_LAST_DAY = 376

# Change these if the workbook uses different assignment codes.
LD_CODE = "LD"
LQ_CODE = "LQ"
L_CODE = "L"

BLUE = "\033[94m"
RED = "\033[91m"
RESET = "\033[0m"

def parse_day_number(column_name: object) -> int | None:
    """Return the day number from labels such as Day_12, Day 12, or day12."""
    match = re.fullmatch(r"\s*day[\s_-]*(\d+)\s*", str(column_name), flags=re.IGNORECASE)
    return int(match.group(1)) if match else None


def load_schedule(excel_path: Path):
    """Load the workbook and identify day columns, workers, excess, and deficit."""
    df = pd.read_excel(excel_path, sheet_name=0)

    if df.empty:
        raise ValueError("The selected worksheet is empty.")
    if len(df.columns) < 3:
        raise ValueError("Expected a label column and 378 day columns.")

    day_columns = {}
    df.columns = df.columns.str.lower()
    for col in df.columns:
        day_number = parse_day_number(col)
        if day_number is not None:
            if day_number in day_columns:
                raise ValueError(f"More than one column was detected for Day_{day_number}.")
            day_columns[day_number] = col

    expected_days = set(range(FIRST_DAY, LAST_DAY + 1))
    missing_days = sorted(expected_days - set(day_columns))
    if missing_days:
        preview = ", ".join(map(str, missing_days[:20]))
        suffix = "..." if len(missing_days) > 20 else ""
        raise ValueError(
            f"Could not identify all day columns. Missing day numbers: {preview}{suffix}. "
            "Name columns Day_1 through Day_378, or edit parse_day_number()."
        )

    # The first column is assumed to contain worker IDs / special-row labels.
    label_column = df.columns[0]
    labels = df[label_column].astype(str).str.strip()
    normalized_labels = labels.str.lower()

    morning_matches = normalized_labels.str.contains("time_worked_m", na=False)
    tardes_matches = normalized_labels.str.contains("time_worked_t", na=False)

    if morning_matches.any() and tardes_matches.any():
        morning_row = df.loc[morning_matches].iloc[0]
        tardes_row = df.loc[tardes_matches].iloc[0]
        special_indices = set(df.index[morning_matches | tardes_matches])

    worker_df = df.loc[~df.index.isin(special_indices)].copy()
    if worker_df.empty:
        raise ValueError("No worker rows remain after excluding excess/deficit rows.")

    # Sort day columns numerically rather than relying on Excel column order.
    ordered_day_columns = [day_columns[d] for d in range(FIRST_DAY, LAST_DAY + 1)]
    return df, label_column, worker_df, ordered_day_columns, morning_row, tardes_row


def calculate_counters(worker_df: pd.DataFrame, label_column: object, day_columns: list[object]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Calculate daily and cumulative c2d/l_dom/l_sat/cxx counters for each worker."""
    day_numbers = list(range(FIRST_DAY, LAST_DAY + 1))
    eligible_days = [day for day in day_numbers if COUNTER_FIRST_DAY <= day <= COUNTER_LAST_DAY]

    records = []
    daily_counter_records = []
    worker_df.columns = worker_df.columns.str.lower()
    for _, worker_row in worker_df.iterrows():
        worker = str(worker_row[label_column]).strip()
        c2d_total = 0
        l_dom_total = 0
        l_sat_total = 0
        cxx_total = 0
        for day in eligible_days:
            assignment = str(worker_row[day_columns[day - 1]]).strip().upper()
            if assignment in {"", "NAN", "NONE", "-"}:
                assignment = ""

            bucket = day % 7
            #c2d and l_dom calculation (l_dom can only be if c2d is there)
            if bucket == 0 and assignment in {LQ_CODE, L_CODE, "F"}:
                if day in eligible_days:
                    l_dom_total += 1
            if bucket == 6 and assignment in {LQ_CODE, L_CODE, "F"}:
                if day in eligible_days:
                    l_sat_total += 1
                    next_day_assignment = str(worker_row[day_columns[day]]).strip().upper()
                    if next_day_assignment in {LQ_CODE, L_CODE}:
                        c2d_total += 1
                        cxx_total += 1

            #cxx calculation, its a c2d but never on weekend
            if bucket <= 4:
                if assignment in {LQ_CODE, LD_CODE, L_CODE}:
                    if bucket < ( day + 1) % 7:
                        if day in eligible_days:
                            next_day_assignment = str(worker_row[day_columns[day]]).strip().upper()
                            if next_day_assignment in {LQ_CODE, LD_CODE, L_CODE}:
                                if day - 2 in eligible_days:
                                    prev_day_assignment = str(worker_row[day_columns[day - 2]]).strip().upper()
                                    if prev_day_assignment not in {LQ_CODE, LD_CODE, L_CODE, "F"}:
                                        cxx_total += 1

            daily_counter_records.append({
                "Worker": worker,
                "Day": day,
                "Assignment": assignment,
                "c2d_daily_increment": c2d_total,
                "l_dom_daily_increment": l_dom_total,
                "l_sat_daily_increment": l_sat_total,
                "c2d_cumulative": c2d_total,
                "l_dom_cumulative": l_dom_total,
                "l_sat_cumulative": l_sat_total,
                "cxx_cumulative": cxx_total,
            })
        print(l_dom_total)
        records.append({
            "Worker": worker,
            "c2d_total": c2d_total,
            "l_dom_total": l_dom_total,
            "l_sat_total": l_sat_total,
            "cxx_total": cxx_total,
        })

    return pd.DataFrame(records), pd.DataFrame(daily_counter_records)

def create_html_report(daily_counters_py, time_worked_py, daily_counters_r, time_worked_r, output_path, workbook_name,):
    """Compare Python and R schedule analysis in side-by-side graphs."""

    # --------------------------------------------------
    # Worker colors
    # --------------------------------------------------
    workers = sorted(
        set(daily_counters_py["Worker"]) |
        set(daily_counters_r["Worker"]),
        key=str,
    )

    color_palette = px.colors.qualitative.Plotly

    worker_colors = {
        worker: color_palette[i % len(color_palette)]
        for i, worker in enumerate(workers)
    }

    # --------------------------------------------------
    # Subplots
    # --------------------------------------------------
    fig = make_subplots(
        rows=5,
        cols=2,
        vertical_spacing=0.07,
        horizontal_spacing=0.08,
        subplot_titles=(
            "Necessities — Original", "Necessities — Fixed",
            "Cumulative c2d per Worker — Original", "Cumulative c2d per Worker — Fixed",
            "Cumulative l_dom per Worker — Original", "Cumulative l_dom per Worker — Fixed",
            "Cumulative l_sat Count per Worker — Original", "Cumulative l_sat Count per Worker — Fixed",
            "Cumulative CXX Count per Worker — Original", "Cumulative CXX Count per Worker — Fixed",
        ),
    )

    # --------------------------------------------------
    # Graph 1: Time worked — PY and R
    # --------------------------------------------------
    shift_specs = [
        ("Time_Worked_M", "Morning", "#3498DB"),
        ("Time_Worked_T", "Afternoon", "#E74C3C"),
    ]
    
    for col, time_worked, suffix in [
        (1, time_worked_py, "Original"),
        (2, time_worked_r, "Fixed"),
    ]:
        for shift, label, color in shift_specs:
            fig.add_trace(
                go.Scatter(
                    x=time_worked["Day"],
                    y=time_worked[shift],
                    mode="lines",
                    name=label,
                    legendgroup=label,
                    showlegend=(col == 1),
                    line={
                        "width": 1.5,
                        "color": color,
                    },
                    hovertemplate=(
                        f"{suffix} — Day %{{x}}<br>"
                        f"{label}: %{{y}}<extra></extra>"
                    ),
                ),
                row=1,
                col=col,
            )
    
        fig.add_hline(
            y=0,
            line_width=0.8,
            line_color="gray",
            row=1,
            col=col,
        )
    
    # Shared Y-axis range for PY and R.
    combined_time = pd.concat([
        time_worked_py[["Time_Worked_M", "Time_Worked_T"]].stack(),
        time_worked_r[["Time_Worked_M", "Time_Worked_T"]].stack(),
    ]).dropna()
    
    if not combined_time.empty:
        y_min = combined_time.min()
        y_max = combined_time.max()
        padding = max((y_max - y_min) * 0.05, 1)
    
        time_range = [
            min(0, y_min - padding),
            max(0, y_max + padding),
        ]
    
        for col in (1, 2):
            fig.update_yaxes(
                title_text="Time worked",
                range=time_range,
                row=1,
                col=col,
            )
    
    # --------------------------------------------------
    # Graphs 2–5: Cumulative counters
    # --------------------------------------------------
    counter_specs = [(2, "c2d", "c2d_cumulative"), (3, "l_dom", "l_dom_cumulative"), (4, "l_sat", "l_sat_cumulative"), (5, "cxx", "cxx_cumulative"),]

    for row_number, counter_name, value_column in counter_specs:

        # Shared Y-axis range between PY and R.
        combined_values = pd.concat([
            daily_counters_py[value_column],
            daily_counters_r[value_column],
        ]).dropna()

        if not combined_values.empty:
            y_min = min(0, combined_values.min())
            y_max = combined_values.max()
            padding = max((y_max - y_min) * 0.05, 1)

            y_range = [y_min, y_max + padding]

        else:
            y_range = [0, 1]

        for col, daily_counters, suffix in [(1, daily_counters_py, "Original"), (2, daily_counters_r, "Fixed"),]:
            for worker, group in daily_counters.groupby("Worker", sort=False):

                fig.add_trace(
                    go.Scatter(
                        x=group["Day"],
                        y=group[value_column],
                        mode="lines",
                        name=str(worker),
                        legendgroup=str(worker),
                        showlegend=(row_number == 2 and col == 1),
                        line={
                            "width": 1.2,
                            "color": worker_colors[worker],
                        },
                        hovertemplate=(
                            f"{suffix} — Worker {worker}<br>"
                            f"{counter_name.upper()}: "
                            "%{y}<extra></extra>"
                        ),
                    ),
                    row=row_number,
                    col=col,
                )

            fig.update_yaxes(
                title_text="Cumulative count",
                range=y_range,
                row=row_number,
                col=col,
            )

    # --------------------------------------------------
    # X-axis ranges
    # --------------------------------------------------
    for col in (1, 2):
        fig.update_xaxes(
            title_text="Day",
            range=[FIRST_DAY, LAST_DAY],
            row=1,
            col=col,
        )

        for row_number in (2, 3, 4, 5):
            fig.update_xaxes(
                title_text="Day",
                range=[COUNTER_FIRST_DAY, COUNTER_LAST_DAY],
                row=row_number,
                col=col,
            )

    # --------------------------------------------------
    # Layout
    # --------------------------------------------------
    fig.update_layout(
        title={
            "text": f"Schedule Analysis — Original vs Fixed — {workbook_name}",
            "x": 0.5,
        },
        height=2200,
        width=1800,
        template="plotly_white",
        hovermode="x",
        legend={
            "title": {"text": "Workers — toggle Original and Fixed together"},
            "groupclick": "togglegroup",
        },
        margin={
            "l": 75,
            "r": 180,
            "t": 130,
            "b": 60,
        },
        paper_bgcolor="#111111",
        plot_bgcolor="#111111",
        font=dict(color="grey"),
    )

    # --------------------------------------------------
    # Export HTML
    # --------------------------------------------------
    fig.write_html(
        str(output_path),
        full_html=True,
        include_plotlyjs=True,
        config={
            "responsive": True,
            "displaylogo": False,
        },
    )
    html = Path(output_path).read_text(encoding="utf-8")

    html = html.replace(
        "<body>",
        '<body style="background-color: #111111; margin: 0; padding: 0;">'
    )

    Path(output_path).write_text(html, encoding="utf-8")

def compare_counters(counters_df_py: pd.DataFrame, counters_df_r: pd.DataFrame) -> pd.DataFrame:
    counters = ["c2d_total", "l_dom_total", "l_sat_total", "cxx_total"]

    py = counters_df_py.set_index("Worker")
    r = counters_df_r.set_index("Worker")

    comparison = pd.DataFrame(index=py.index.union(r.index))
    comparison.index.name = "Worker"

    for counter in counters:
        py_values = py[counter].reindex(comparison.index)
        r_values = r[counter].reindex(comparison.index)

        comparison[counter] = [
            f"{p if pd.notna(p) else '-'} vs "
            f"{v if pd.notna(v) else '-'}"
            for p, v in zip(py_values, r_values)
        ]

    return comparison.reset_index()


def create_time_worked_rows(days, morning_row, tardes_row, day_columns):

    return pd.DataFrame({
            "Day": days,
            "Time_Worked_M": pd.to_numeric(morning_row[day_columns] ,errors="coerce",).to_numpy(),
            "Time_Worked_T": pd.to_numeric(tardes_row[day_columns], errors="coerce",).to_numpy(),
        })
    
def main() -> None:
    parser = argparse.ArgumentParser(description="Plot excess/deficit and worker counters from Excel.")
    parser.add_argument("excel_file_original", type=Path, help="Path to the original input .xlsx/.xls file")
    parser.add_argument("excel_file_altered", type=Path, help="Path to the altered input .xlsx/.xls file")
    parser.add_argument("--output-dir", type=Path, default=Path("schedule_analysis"), help="Directory for PNG and CSV outputs")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    _, label_column, worker_df_original, day_columns, morning_row_o, tardes_row_o = load_schedule(args.excel_file_original)
    _, label_column, worker_df_altered, day_columns, morning_row_a, tardes_row_a = load_schedule(args.excel_file_altered)

    counters_df_py, daily_counters_df_py = calculate_counters(worker_df_original, label_column, day_columns)
    counters_df_altered, daily_counters_df_altered = calculate_counters(worker_df_altered, label_column, day_columns)

    time_worked_df_original = create_time_worked_rows(list(range(FIRST_DAY, LAST_DAY + 1)), morning_row_o, tardes_row_o, day_columns)
    time_worked_df_altered = create_time_worked_rows(list(range(FIRST_DAY, LAST_DAY + 1)), morning_row_a, tardes_row_a, day_columns)

    html_path = (args.output_dir / "schedule_analysis.html")
    create_html_report(daily_counters_df_py, time_worked_df_original, daily_counters_df_altered, time_worked_df_altered, html_path, args.excel_file_original.name,)

    print(f"Analysis complete")
    print(f"Workers analyzed: {len(worker_df_original)}")
    print(f"Counter days: {COUNTER_FIRST_DAY}-{COUNTER_LAST_DAY} inclusive")
    print(f"Counter for algorithms: {BLUE}PY{RESET} vs {RED}R{RESET}")
    print(compare_counters(counters_df_py, counters_df_altered))
    webbrowser.open(html_path.resolve().as_uri())

if __name__ == "__main__":
    main()