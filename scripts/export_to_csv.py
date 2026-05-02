"""
Exports all five RecruitIQ tables from MySQL to CSV files in data/processed/.

Intended for use with Tableau Public, which requires local CSV files as its
data source. Output files are excluded from git (see .gitignore) and should
be regenerated from MySQL whenever the underlying data changes.

Run from the project root:
    python scripts/export_to_csv.py
"""

from __future__ import annotations

import os
import sys

import mysql.connector
import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_TABLES: list[str] = [
    "job_requisitions",
    "candidates",
    "applications",
    "pipeline_stages",
    "offers",
]

_OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "processed",
)


def load_config() -> dict:
    load_dotenv()
    return {
        "host":     os.getenv("DB_HOST", "localhost"),
        "port":     int(os.getenv("DB_PORT", 3306)),
        "user":     os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
    }


def export_table(table: str, connection: mysql.connector.MySQLConnection) -> str:
    """
    Read one table into a DataFrame and write it to data/processed/<table>.csv.

    Returns the absolute path of the written file.
    """
    df = pd.read_sql(f"SELECT * FROM {table}", con=connection)  # noqa: S608

    output_path = os.path.join(_OUTPUT_DIR, f"{table}.csv")
    df.to_csv(
        output_path,
        index=False,
        date_format="%Y-%m-%d %H:%M:%S",
    )

    print(f"  Exported {len(df):>7,} rows  →  {output_path}")
    return output_path


def main() -> None:
    db_config   = load_config()
    connection  = None

    os.makedirs(_OUTPUT_DIR, exist_ok=True)

    try:
        print(f"Connecting to MySQL ({db_config['host']}:{db_config['port']} / {db_config['database']})...")
        connection = mysql.connector.connect(**db_config)
        print(f"Connected.\n")

        exported_paths: list[str] = []
        for table in _TABLES:
            path = export_table(table, connection)
            exported_paths.append(path)

        print(f"\nFile sizes:")
        total_bytes = 0
        for path in exported_paths:
            size_bytes  = os.path.getsize(path)
            total_bytes += size_bytes
            print(f"  {os.path.basename(path):<30}  {size_bytes / 1_048_576:>6.2f} MB")

        print(f"  {'TOTAL':<30}  {total_bytes / 1_048_576:>6.2f} MB")
        print(f"\nAll tables exported to {_OUTPUT_DIR}")

    except Exception as e:
        print(f"Export failed: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        if connection is not None and connection.is_connected():
            connection.close()


if __name__ == "__main__":
    main()
