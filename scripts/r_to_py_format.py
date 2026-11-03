
import csv
import re
import argparse
from pathlib import Path
import json
from pathlib import Path

import oracledb
import pandas as pd

import oracledb

oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient")

SCHEDULE_MAPPING = {
    ("R", ""): "F",
    ("F", "Q"): "LQ",
    ("F", ""): "L",
    ("T", "T"): "T",
    ("T", "M"): "M",
    ("F", "D"): "LD",
    ("T", "A"): "A",
    ("T", "TC"): "TC",
    ("T", "H"): "MoT",
}


def day_number(day_value):
    """Extract the numeric day from values such as Day_12."""
    match = re.fullmatch(r"Day_(\d+)", day_value.strip(), re.IGNORECASE)

    if not match:
        raise ValueError(f"Invalid DAY_OF_YEAR value: {day_value!r}")

    return int(match.group(1))


def convert_csv(data: pd.DataFrame) -> pd.DataFrame:
    required_columns = {
        "WORKER",
        "DAY_OF_YEAR",
        "SCHED_TYPE",
        "SCHED_SUBTYPE",
    }

    if not required_columns.issubset(data.columns):
        raise ValueError(f"Missing columns: {required_columns - set(data.columns)}")

    workers = {}
    all_days = set()

    for _, row in data.iterrows():
        worker = str(row["WORKER"]).strip()
        day = day_number(str(row["DAY_OF_YEAR"]))

        sched_type = "" if pd.isna(row["SCHED_TYPE"]) else str(row["SCHED_TYPE"]).strip()
        sched_subtype = "" if pd.isna(row["SCHED_SUBTYPE"]) else str(row["SCHED_SUBTYPE"]).strip()

        schedule_value = SCHEDULE_MAPPING.get((sched_type, sched_subtype), "?")

        if worker not in workers:
            workers[worker] = {}

        if day in workers[worker]:
            print(f"Duplicate schedule entry for worker {worker}, day {day}")
            continue

        workers[worker][day] = schedule_value
        all_days.add(day)

    if not all_days:
        raise ValueError("No schedule days found.")

    days = sorted(all_days)

    output_data = []

    for worker in sorted(
        workers,
        key=lambda value: (not value.isdigit(), int(value) if value.isdigit() else value),
    ):
        schedule = workers[worker]

        output_row = {"Worker": worker}
        output_row.update({
            f"Day_{day}": schedule.get(day, "-")
            for day in days
        })

        output_data.append(output_row)

    wide_df = pd.DataFrame(output_data)

    unknown_count = sum(
        value == "?"
        for schedule in workers.values()
        for value in schedule.values()
    )

    print(f"Converted {len(workers)} workers and {len(days)} days.")
    print(f"Unrecognized schedule combinations: {unknown_count}")

    return wide_df

def get_schedule(process_id: int) -> pd.DataFrame:
    # Paths relative to this Python file
    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / "src" / "settings" / "oracle_connection_parameters.json"
    sql_path = base_dir / "data" / "Queries" / "sql" / "get_R_output.sql"

    # Load database configuration
    with config_path.open("r", encoding="utf-8") as file:
        config = json.load(file)

    db = config["alcampo_tst"]

    # Read SQL query
    sql = sql_path.read_text(encoding="utf-8")

    # Connect and execute query
    dsn = oracledb.makedsn(
        db["host"],
        int(db["port"]),
        service_name=db["service_name"],
    )

    with oracledb.connect(
        user=db["username"],
        password=db["password"],
        dsn=dsn,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, process_id=process_id)

            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()

    return pd.DataFrame(rows, columns=columns)


def execute_from_r(process_id):
    schedule = get_schedule(process_id)

    output = convert_csv(schedule)

    return output

