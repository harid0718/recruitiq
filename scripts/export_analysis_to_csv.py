"""
Runs every SQL query in sql/analysis/ against MySQL and exports each result
to data/processed/analysis/ as a named CSV file for use in Tableau Public.

Each SQL file contains multiple queries delimited by "-- =====" separator
lines. This script parses those boundaries, extracts the query title from the
"-- QUERY N: ..." comment, derives a CSV filename from the SQL filename prefix
and the title, and executes the query via pandas.read_sql().

Run from the project root:
    python scripts/export_analysis_to_csv.py
"""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import mysql.connector
import pandas as pd
from dotenv import load_dotenv

_SQL_DIR    = Path(__file__).parent.parent / "sql" / "analysis"
_OUTPUT_DIR = Path(__file__).parent.parent / "data" / "processed" / "analysis"

_SEPARATOR_RE  = re.compile(r"^-- ={10,}", re.MULTILINE)
_QUERY_TITLE_RE = re.compile(r"^--\s+QUERY\s+\d+[:\s]+(.+)$", re.MULTILINE)


# =============================================================================
# Data structures
# =============================================================================

@dataclass
class ParsedQuery:
    file_prefix:  str   # e.g. "01"
    title:        str   # e.g. "Overall funnel conversion"
    sql:          str   # executable SQL, comment lines stripped
    output_name:  str   # e.g. "01_overall_funnel_conversion.csv"


# =============================================================================
# Helpers
# =============================================================================

def _to_snake_case(text: str) -> str:
    """
    Convert a human-readable query title to a snake_case filename stem.
    "Overall funnel conversion (most important)" → "overall_funnel_conversion_most_important"
    """
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)   # punctuation → spaces
    text = re.sub(r"\s+", "_", text.strip())    # spaces → underscores
    text = re.sub(r"_+", "_", text)             # collapse consecutive underscores
    return text


def _strip_comments(sql_block: str) -> str:
    """
    Remove lines that begin with '--' and blank lines, leaving only
    executable SQL. Preserves meaningful whitespace within lines.
    """
    lines = [
        line for line in sql_block.splitlines()
        if not line.lstrip().startswith("--") and line.strip()
    ]
    return "\n".join(lines).strip()


# =============================================================================
# Core functions
# =============================================================================

def load_config() -> dict:
    load_dotenv()
    return {
        "host":     os.getenv("DB_HOST", "localhost"),
        "port":     int(os.getenv("DB_PORT", 3306)),
        "user":     os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
    }


def parse_sql_file(sql_path: Path) -> list[ParsedQuery]:
    """
    Parse a single SQL analysis file into a list of ParsedQuery objects.

    Each query's structure in the file is:

        -- =====...  ← opening separator  (start of comment block)
        -- QUERY N: <title>
        -- ...
        -- =====...  ← closing separator  (end of comment block)

        <actual SQL here>

        -- =====...  ← opening separator of the NEXT query's comment block

    Strategy:
      1. Find every "-- QUERY N: <title>" line via finditer over the full content.
      2. For each title match, search forward for the next separator — that is
         the closing line of the comment block.
      3. The SQL starts immediately after that closing separator.
      4. The SQL ends at the next separator after the SQL start (the opening
         line of the following query's comment block), or at end-of-file.
    """
    file_prefix = sql_path.stem.split("_")[0]   # "01" from "01_funnel_metrics"
    content     = sql_path.read_text(encoding="utf-8")

    queries: list[ParsedQuery] = []

    for title_match in _QUERY_TITLE_RE.finditer(content):
        title = title_match.group(1).strip()

        # Find the closing separator of this query's comment block —
        # the first "-- ===..." line that follows the title line.
        closing_sep = _SEPARATOR_RE.search(content, title_match.end())
        if not closing_sep:
            continue  # malformed file — no closing separator after title

        # SQL starts on the character immediately after the closing separator line.
        sql_start = closing_sep.end()

        # SQL ends at the next separator (opening of the next comment block)
        # or at end-of-file if this is the last query.
        next_sep = _SEPARATOR_RE.search(content, sql_start)
        sql_end  = next_sep.start() if next_sep else len(content)

        sql = _strip_comments(content[sql_start:sql_end])
        if not sql:
            continue  # title comment with no following SQL — skip

        snake_title = _to_snake_case(title)
        output_name = f"{file_prefix}_{snake_title}.csv"

        queries.append(ParsedQuery(
            file_prefix  = file_prefix,
            title        = title,
            sql          = sql,
            output_name  = output_name,
        ))

    return queries


def export_query(
    query:      ParsedQuery,
    connection: mysql.connector.MySQLConnection,
    output_dir: Path,
) -> bool:
    """
    Execute a single query and write the result to output_dir/<output_name>.

    Returns True on success, False on failure (error is printed but not raised
    so the caller can continue processing remaining queries).
    """
    output_path = output_dir / query.output_name
    try:
        df = pd.read_sql(query.sql, con=connection)
        df.to_csv(output_path, index=False, date_format="%Y-%m-%d %H:%M:%S")
        print(f"  [{len(df):>6,} rows]  →  analysis/{query.output_name}")
        return True
    except Exception as e:
        print(f"  [  FAILED]      analysis/{query.output_name}  —  {e}", file=sys.stderr)
        return False


def main() -> None:
    db_config = load_config()

    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    sql_files = sorted(_SQL_DIR.glob("*.sql"))
    if not sql_files:
        print(f"No SQL files found in {_SQL_DIR}", file=sys.stderr)
        sys.exit(1)

    # Parse all queries before opening the connection so parsing errors surface early
    all_queries: list[ParsedQuery] = []
    for sql_path in sql_files:
        parsed = parse_sql_file(sql_path)
        print(f"Parsed {sql_path.name}: {len(parsed)} quer{'y' if len(parsed) == 1 else 'ies'} found.")
        all_queries.extend(parsed)

    print(f"\n{len(all_queries)} total queries across {len(sql_files)} files.\n")

    connection = None
    n_succeeded = 0
    n_failed    = 0

    try:
        print(f"Connecting to MySQL ({db_config['host']}:{db_config['port']} / {db_config['database']})...")
        connection = mysql.connector.connect(**db_config)
        print("Connected.\n")

        for query in all_queries:
            success = export_query(query, connection, _OUTPUT_DIR)
            if success:
                n_succeeded += 1
            else:
                n_failed += 1

    except Exception as e:
        print(f"Fatal error during export: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        if connection is not None and connection.is_connected():
            connection.close()

    print(f"\nDone — {n_succeeded} succeeded, {n_failed} failed.")
    if n_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
