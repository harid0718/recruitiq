"""
Generates synthetic pipeline_stages rows and inserts them into MySQL.

For each application, builds an ordered sequence of hiring funnel stages with
realistic advancement rates, timing, and outcomes that are consistent with
the application's final status. Injects two categories of data-quality defects
for use in analytics cleaning exercises.

Run from the project root:
    python scripts/generate_pipeline_stages.py
"""

from __future__ import annotations

import os
import random
import sys
import time
from datetime import datetime, timedelta
from typing import NamedTuple

from dotenv import load_dotenv
from faker import Faker
import mysql.connector
from mysql.connector import Error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import (
    DATA_END_DATE,
    DATA_QUALITY_RATES,
    RANDOM_SEED,
)


class AppContext(NamedTuple):
    app_id:     int
    status:     str       # active | rejected | withdrawn | hired
    applied_at: datetime
    department: str | None
    seniority:  str | None


# =============================================================================
# Reference data
# =============================================================================

_TECH_DEPTS:      frozenset[str] = frozenset({"Engineering", "Manufacturing", "Design"})
_SENIOR_LEVELS:   frozenset[str] = frozenset({"senior", "staff", "manager",
                                               "director", "vp", "executive"})
# Stages that involve a live interviewer and generate a scorecard
_INTERVIEW_STAGES: frozenset[str] = frozenset({"recruiter_screen", "hiring_manager_screen",
                                                "technical_assessment", "panel_interview",
                                                "executive_interview"})

# Probability of advancing FROM each stage to the next stage in the sequence.
# Interpretation: "X% of candidates who reach this stage move on to the next one."
_ADVANCE_FROM: dict[str, float] = {
    "applied":               0.40,
    "recruiter_screen":      0.50,
    "hiring_manager_screen": 0.60,
    "technical_assessment":  0.60,
    "take_home_assignment":  0.40,
    "panel_interview":       0.50,
    "executive_interview":   0.70,
    "background_check":      0.90,
    "offer":                 0.95,
    # "hired" is terminal — no entry needed
}

# (min_days, max_days) a candidate spends inside each stage
_STAGE_DURATIONS: dict[str, tuple[int, int]] = {
    "applied":               (0,  2),
    "recruiter_screen":      (1,  7),
    "hiring_manager_screen": (3, 14),
    "technical_assessment":  (5, 14),
    "take_home_assignment":  (3, 10),
    "panel_interview":       (7, 21),
    "executive_interview":   (3, 14),
    "background_check":      (5, 14),
    "offer":                 (3, 14),
    "hired":                 (0,  0),  # instantaneous — accepted and done
}

_SCORECARD_RATINGS:  list[str] = ["strong_yes", "yes", "mixed", "no", "strong_no"]
_SCORECARD_WEIGHTS:  list[int] = [        15,    30,     25,    20,         10]

# Maps application.status to the outcome string for the terminal stage row
_TERMINAL_OUTCOME: dict[str, str] = {
    "active":    "pending",
    "rejected":  "rejected",
    "withdrawn": "withdrew",
}

_CHUNK_SIZE = 5_000

# Tuple column indices — referenced by inject_data_quality_issues
_STAGE_ORDER_IDX = 2
_ENTERED_AT_IDX  = 3

_INSERT_SQL = """
    INSERT INTO pipeline_stages
        (application_id, stage_name, stage_order, entered_at, exited_at,
         outcome, interviewer_name, scorecard_rating, notes)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

_END_DT: datetime = datetime.combine(DATA_END_DATE, datetime.min.time())


# =============================================================================
# Private helpers
# =============================================================================

def _cap_dt(dt: datetime) -> datetime:
    return min(dt, _END_DT)


def _stage_duration(stage_name: str, rng: random.Random) -> int:
    low, high = _STAGE_DURATIONS.get(stage_name, (1, 7))
    return rng.randint(low, high)


def _build_stage_sequence(
    dept: str | None,
    seniority: str | None,
    rng: random.Random,
) -> list[str]:
    """
    Build the ordered list of stages applicable to this application.

    - Technical stages (technical_assessment / take_home_assignment) are only
      included for Engineering, Manufacturing, and Design departments.
    - For Engineering specifically, take_home_assignment replaces
      technical_assessment 30% of the time.
    - executive_interview is only included for senior+ seniority levels.
    """
    seq: list[str] = ["applied", "recruiter_screen", "hiring_manager_screen"]

    if dept in _TECH_DEPTS:
        if dept == "Engineering" and rng.random() < 0.30:
            seq.append("take_home_assignment")
        else:
            seq.append("technical_assessment")

    seq.append("panel_interview")

    if seniority in _SENIOR_LEVELS:
        seq.append("executive_interview")

    seq.extend(["background_check", "offer", "hired"])
    return seq


def _make_stage_tuple(
    app_id:        int,
    stage_name:    str,
    stage_order:   int,
    entered_at:    datetime,
    exited_at:     datetime | None,
    outcome:       str,
    rng:           random.Random,
    faker_instance: Faker,
) -> tuple:
    if stage_name in _INTERVIEW_STAGES:
        interviewer   = faker_instance.name()
        scorecard     = rng.choices(_SCORECARD_RATINGS, weights=_SCORECARD_WEIGHTS, k=1)[0]
    else:
        interviewer   = None
        scorecard     = None

    return (
        app_id,
        stage_name,
        stage_order,
        entered_at,
        exited_at,
        outcome,
        interviewer,
        scorecard,
        None,   # notes — omitted to keep dataset size manageable
    )


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


def fetch_application_context(db_config: dict) -> list[AppContext]:
    """
    Query applications joined with job_requisitions to get the four fields
    needed for stage generation: status, applied_at, department, seniority_level.
    """
    connection = None
    cursor     = None
    try:
        connection = mysql.connector.connect(**db_config)
        cursor     = connection.cursor()

        cursor.execute("""
            SELECT
                a.id,
                a.status,
                a.applied_at,
                jr.department,
                jr.seniority_level
              FROM applications a
              JOIN job_requisitions jr ON a.requisition_id = jr.id
             ORDER BY a.id
        """)
        return [
            AppContext(
                app_id     = row[0],
                status     = row[1],
                applied_at = row[2],
                department = row[3],
                seniority  = row[4],
            )
            for row in cursor.fetchall()
        ]

    except Error as e:
        print(f"MySQL error while fetching application context: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()


def generate_stages_for_application(
    app: AppContext,
    rng: random.Random,
    faker_instance: Faker,
) -> list[tuple]:
    """
    Generate all pipeline stage rows for a single application.

    Hired applications are forced through every stage in the sequence.
    All other statuses advance probabilistically and terminate with an outcome
    that matches the application's final status (pending / rejected / withdrew).
    """
    stage_seq = _build_stage_sequence(app.department, app.seniority, rng)

    # Non-hired applications never reach the 'hired' stage name
    if app.status != "hired":
        stage_seq = [s for s in stage_seq if s != "hired"]

    generated:    list[tuple] = []
    current_time: datetime    = app.applied_at

    for i, stage_name in enumerate(stage_seq):
        entered_at  = current_time
        is_last_pos = (i == len(stage_seq) - 1)

        if app.status == "hired":
            if is_last_pos:  # 'hired' stage — instantaneous acceptance
                outcome   = "hired"
                exited_at: datetime | None = entered_at
            else:
                outcome   = "advanced"
                exited_at = _cap_dt(entered_at + timedelta(days=_stage_duration(stage_name, rng)))

            generated.append(_make_stage_tuple(
                app.app_id, stage_name, i + 1,
                entered_at, exited_at, outcome, rng, faker_instance,
            ))
            current_time = exited_at  # type: ignore[assignment]

        else:
            # Determine whether this is the terminal stage for this application.
            # It is terminal if: it's the last possible stage in the sequence, or
            # the random roll fails the advance probability for this stage.
            stops_here = is_last_pos or (rng.random() >= _ADVANCE_FROM.get(stage_name, 0.0))

            if stops_here:
                outcome = _TERMINAL_OUTCOME[app.status]
                if app.status == "active":
                    exited_at = None    # still in this stage; no exit timestamp
                else:
                    exited_at = _cap_dt(
                        entered_at + timedelta(days=_stage_duration(stage_name, rng))
                    )

                generated.append(_make_stage_tuple(
                    app.app_id, stage_name, i + 1,
                    entered_at, exited_at, outcome, rng, faker_instance,
                ))
                break  # no further stages for this application

            else:
                exited_at = _cap_dt(entered_at + timedelta(days=_stage_duration(stage_name, rng)))
                generated.append(_make_stage_tuple(
                    app.app_id, stage_name, i + 1,
                    entered_at, exited_at, "advanced", rng, faker_instance,
                ))
                current_time = exited_at

    return generated


def inject_data_quality_issues(
    all_rows: list[tuple],
    app_applied_ats: dict[int, datetime],
    rng: random.Random,
) -> None:
    """
    Inject two categories of data-quality defects in-place.

    1. invalid_interview_date_rate — set entered_at to BEFORE applied_at on a
       random sample of stage rows. Simulates a system clock error or retroactive
       data entry mistake; detectable by asserting entered_at >= applications.applied_at.

    2. out_of_order_stages_rate — shuffle stage_order within a random sample of
       applications while leaving entered_at unchanged. Detectable by checking
       that stage_order agrees with chronological entered_at ordering.
    """
    # --- Invalid interview dates (row-level injection) ---
    n_invalid    = round(len(all_rows) * DATA_QUALITY_RATES["invalid_interview_date_rate"])
    invalid_idxs = set(rng.sample(range(len(all_rows)), n_invalid))

    for i in invalid_idxs:
        row         = all_rows[i]
        applied_at  = app_applied_ats[row[0]]
        bad_time    = applied_at - timedelta(days=rng.randint(1, 30))
        all_rows[i] = row[:_ENTERED_AT_IDX] + (bad_time,) + row[_ENTERED_AT_IDX + 1:]

    # --- Out-of-order stage_order (application-level injection) ---
    # Build index: app_id → list of row positions in all_rows
    app_to_indices: dict[int, list[int]] = {}
    for i, row in enumerate(all_rows):
        app_to_indices.setdefault(row[0], []).append(i)

    all_app_ids = list(app_to_indices.keys())
    n_oos       = round(len(all_app_ids) * DATA_QUALITY_RATES["out_of_order_stages_rate"])

    for app_id in rng.sample(all_app_ids, n_oos):
        indices = app_to_indices[app_id]
        if len(indices) < 2:
            continue  # single-stage application — nothing to reorder

        orders   = [all_rows[i][_STAGE_ORDER_IDX] for i in indices]
        shuffled = orders[:]
        while shuffled == orders:          # guarantee a real permutation change
            rng.shuffle(shuffled)

        for j, idx in enumerate(indices):
            row           = all_rows[idx]
            all_rows[idx] = row[:_STAGE_ORDER_IDX] + (shuffled[j],) + row[_STAGE_ORDER_IDX + 1:]

    print(
        f"Data quality injected: {n_invalid} invalid entered_at timestamps, "
        f"{n_oos} applications with shuffled stage_order."
    )


def generate_all(
    app_contexts: list[AppContext],
    rng: random.Random,
    faker_instance: Faker,
) -> list[tuple]:
    app_applied_ats: dict[int, datetime] = {
        app.app_id: app.applied_at for app in app_contexts
    }

    all_rows:      list[tuple] = []
    last_progress: int         = 0

    for app in app_contexts:
        all_rows.extend(generate_stages_for_application(app, rng, faker_instance))

        if len(all_rows) >= last_progress + 10_000:
            print(f"  Generated {len(all_rows):,} stages...")
            last_progress = len(all_rows)

    print(
        f"Generated {len(all_rows):,} stage rows "
        f"across {len(app_contexts):,} applications."
    )

    inject_data_quality_issues(all_rows, app_applied_ats, rng)

    return all_rows


def insert_to_db(rows: list[tuple], db_config: dict) -> None:
    """Bulk-insert in chunks within a single atomic transaction."""
    connection = None
    cursor     = None
    try:
        connection = mysql.connector.connect(**db_config)
        cursor     = connection.cursor()

        t0             = time.perf_counter()
        total_inserted = 0

        for start in range(0, len(rows), _CHUNK_SIZE):
            chunk           = rows[start : start + _CHUNK_SIZE]
            cursor.executemany(_INSERT_SQL, chunk)
            total_inserted += len(chunk)
            print(f"  Inserted {total_inserted:,} / {len(rows):,} stages...")

        connection.commit()
        elapsed = time.perf_counter() - t0
        print(f"Inserted {len(rows):,} rows in {elapsed:.2f} seconds.")

    except Error as e:
        if connection is not None:
            connection.rollback()
        print(f"MySQL error during insert: {e}", file=sys.stderr)
        print("Transaction rolled back. No rows were committed.", file=sys.stderr)
        sys.exit(1)

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()


def main() -> None:
    db_config = load_config()

    print("Fetching application context from database...")
    app_contexts = fetch_application_context(db_config)
    print(f"  Loaded {len(app_contexts):,} applications.")

    rng            = random.Random(RANDOM_SEED)
    faker_instance = Faker()
    Faker.seed(RANDOM_SEED)

    print("Generating pipeline stages...")
    rows = generate_all(app_contexts, rng, faker_instance)

    print(f"\nInserting into '{db_config['database']}'...")
    insert_to_db(rows, db_config)


if __name__ == "__main__":
    main()
