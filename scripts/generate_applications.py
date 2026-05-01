"""
Generates synthetic applications rows and inserts them into MySQL.

Queries existing candidate and requisition IDs from the database, distributes
45,000 application pairs across them following a realistic re-application
frequency distribution, and enforces temporal constraints on applied_at.

Run from the project root:
    python scripts/generate_applications.py
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
    RANDOM_SEED,
    VOLUMES,
)


class ReqInfo(NamedTuple):
    req_id:     int
    opened_at:  datetime
    status:     str
    headcount:  int


# =============================================================================
# Reference data
# =============================================================================

_SOURCES: list[str]       = ["linkedin", "job_board", "employee_referral",
                              "company_website", "agency", "university", "event", "other"]
_SOURCE_WEIGHTS: list[int] = [35, 25, 15, 10, 7, 5, 2, 1]  # sums to 100

_APP_STATUSES: list[str]       = ["active", "rejected", "withdrawn", "hired"]
_APP_STATUS_WEIGHTS: list[int] = [25, 65, 5, 5]  # sums to 100

_CHUNK_SIZE = 5_000

_INSERT_SQL = """
    INSERT INTO applications
        (candidate_id, requisition_id, source, referral_employee_name, status, applied_at)
    VALUES
        (%s, %s, %s, %s, %s, %s)
"""

# Pre-compute the end-of-range ceiling for applied_at capping
_END_DT: datetime = datetime.combine(DATA_END_DATE, datetime.min.time())


# =============================================================================
# Private helpers
# =============================================================================

def _assign_counts(
    n_candidates: int,
    target: int,
    max_per_candidate: int,
    rng: random.Random,
) -> list[int]:
    """
    Assign a raw application count to each candidate drawn from the
    re-application distribution, then trim or extend the total to hit
    target exactly.

    Distribution:
        80% → 1 application   (one-time applicants)
        15% → 2 applications
         4% → 3–4 applications
         1% → 5–8 applications (chronic re-appliers)
    """
    counts: list[int] = []
    for _ in range(n_candidates):
        group = rng.choices(
            ["one", "two", "three_four", "five_plus"],
            weights=[80, 15, 4, 1],
            k=1,
        )[0]
        if group == "one":
            counts.append(1)
        elif group == "two":
            counts.append(2)
        elif group == "three_four":
            counts.append(rng.randint(3, 4))
        else:
            counts.append(rng.randint(5, 8))

    total = sum(counts)
    diff  = total - target

    if diff > 0:
        # Reduce some counts — floor is 1 application per candidate
        reducible = [i for i, c in enumerate(counts) if c > 1]
        rng.shuffle(reducible)
        for idx in reducible:
            if diff <= 0:
                break
            reduction    = min(diff, counts[idx] - 1)
            counts[idx] -= reduction
            diff         -= reduction

    elif diff < 0:
        # Increase some counts — ceiling is max_per_candidate
        diff    = -diff
        indices = list(range(n_candidates))
        rng.shuffle(indices)
        for idx in indices:
            if diff <= 0:
                break
            if counts[idx] < max_per_candidate:
                counts[idx] += 1
                diff         -= 1

    return counts


def _generate_applied_at(opened_at: datetime, rng: random.Random) -> datetime:
    """
    Return a datetime after opened_at with a front-loaded distribution:
      60% within the first 14 days  (high initial interest)
      30% within 14–45 days         (longer consideration period)
      10% within 45–90 days         (late applicants)

    Hard-capped at DATA_END_DATE so no applied_at leaks past the dataset window.
    """
    low_days, high_days = rng.choices(
        [(0, 14), (14, 45), (45, 90)],
        weights=[60, 30, 10],
        k=1,
    )[0]
    applied_at = (
        opened_at
        + timedelta(days=rng.uniform(low_days, high_days))
        + timedelta(seconds=rng.randint(0, 86_399))
    )
    return min(applied_at, _END_DT)



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


def fetch_candidates_and_reqs(
    db_config: dict,
) -> tuple[list[int], list[ReqInfo]]:
    """
    Query the database for all candidate IDs and all requisition metadata.

    Requisitions with NULL opened_at are excluded — applied_at cannot be
    derived without an anchor timestamp.

    Returns (candidate_ids, req_infos).
    """
    connection = None
    cursor     = None
    try:
        connection = mysql.connector.connect(**db_config)
        cursor     = connection.cursor()

        cursor.execute("SELECT id FROM candidates ORDER BY id")
        candidate_ids: list[int] = [row[0] for row in cursor.fetchall()]

        cursor.execute("""
            SELECT id, opened_at, status, headcount
              FROM job_requisitions
             WHERE opened_at IS NOT NULL
             ORDER BY id
        """)
        req_infos: list[ReqInfo] = [
            ReqInfo(req_id=row[0], opened_at=row[1], status=row[2], headcount=row[3])
            for row in cursor.fetchall()
        ]

        return candidate_ids, req_infos

    except Error as e:
        print(f"MySQL error while fetching data: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()


def assign_applications(
    candidate_ids: list[int],
    req_infos: list[ReqInfo],
    target: int,
    rng: random.Random,
) -> tuple[list[tuple[int, ReqInfo]], set[tuple[int, int]]]:
    """
    Assign target (candidate_id, ReqInfo) pairs following the re-application
    distribution, then earmark exactly headcount pairs per filled requisition
    as guaranteed hires.

    Requisitions are sampled *without replacement per candidate* so that the
    UNIQUE(candidate_id, requisition_id) constraint is satisfied by construction.

    Returns (pairs, must_hire_set) where must_hire_set is a set of
    (candidate_id, req_id) tuples that must be generated with status='hired'.
    """
    counts = _assign_counts(len(candidate_ids), target, len(req_infos), rng)

    pairs: list[tuple[int, ReqInfo]] = []
    for candidate_id, count in zip(candidate_ids, counts):
        count = min(count, len(req_infos))  # guard against edge-case where req pool < count
        for req in rng.sample(req_infos, count):
            pairs.append((candidate_id, req))

    rng.shuffle(pairs)

    # Guaranteed hire pass: collect candidate IDs per filled req, then earmark
    # exactly headcount of them so every filled req has the right number of hires.
    cands_by_req: dict[int, list[int]] = {
        req.req_id: [] for req in req_infos if req.status == "filled"
    }
    for cid, req in pairs:
        if req.req_id in cands_by_req:
            cands_by_req[req.req_id].append(cid)

    must_hire_set: set[tuple[int, int]] = set()
    for req in req_infos:
        if req.status != "filled":
            continue
        available    = cands_by_req.get(req.req_id, [])
        n_to_earmark = min(req.headcount, len(available))
        for cid in rng.sample(available, n_to_earmark):
            must_hire_set.add((cid, req.req_id))

    return pairs, must_hire_set


def generate_application_row(
    candidate_id: int,
    req: ReqInfo,
    rng: random.Random,
    faker_instance: Faker,
    force_hired: bool = False,
) -> tuple:
    """
    Build one application tuple aligned with _INSERT_SQL column order:
    (candidate_id, requisition_id, source, referral_employee_name, status, applied_at)

    referral_employee_name is only populated when source == 'employee_referral'.
    If force_hired is True, status is set to 'hired' unconditionally — used for
    pairs earmarked by the guaranteed hire pass in assign_applications().
    """
    source        = rng.choices(_SOURCES, weights=_SOURCE_WEIGHTS, k=1)[0]
    referral_name = faker_instance.name() if source == "employee_referral" else None
    status        = "hired" if force_hired else rng.choices(_APP_STATUSES, weights=_APP_STATUS_WEIGHTS, k=1)[0]
    applied_at    = _generate_applied_at(req.opened_at, rng)

    return (candidate_id, req.req_id, source, referral_name, status, applied_at)


def insert_to_db(rows: list[tuple], db_config: dict) -> None:
    """
    Bulk-insert rows in chunks of _CHUNK_SIZE.

    All chunks execute within a single transaction so the entire batch is
    committed atomically at the end — or rolled back in full on any error.
    Progress is reported after each chunk.
    """
    connection = None
    cursor     = None
    try:
        connection = mysql.connector.connect(**db_config)
        cursor     = connection.cursor()

        t0             = time.perf_counter()
        total_inserted = 0

        for start in range(0, len(rows), _CHUNK_SIZE):
            chunk = rows[start : start + _CHUNK_SIZE]
            cursor.executemany(_INSERT_SQL, chunk)
            total_inserted += len(chunk)
            print(f"  Inserted {total_inserted} / {len(rows)} applications...")

        connection.commit()
        elapsed = time.perf_counter() - t0
        print(f"Inserted {len(rows)} rows in {elapsed:.2f} seconds.")

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

    print("Fetching candidate IDs and requisition data from database...")
    candidate_ids, req_infos = fetch_candidates_and_reqs(db_config)
    print(
        f"  Candidates: {len(candidate_ids)}  |  "
        f"Requisitions: {len(req_infos)}"
    )

    target          = VOLUMES["applications"]
    rng             = random.Random(RANDOM_SEED)
    faker_instance  = Faker()
    Faker.seed(RANDOM_SEED)

    print(f"\nAssigning {target} applications across {len(candidate_ids)} candidates...")
    pairs, must_hire_set = assign_applications(candidate_ids, req_infos, target, rng)
    print(f"Assigned {len(pairs)} (candidate, requisition) pairs.")
    print(f"Guaranteed hire assignments: {len(must_hire_set)} (earmarked across filled requisitions).")

    print(f"Generating {len(pairs)} application rows...")
    rows: list[tuple] = []
    for i, (cid, req) in enumerate(pairs):
        rows.append(generate_application_row(cid, req, rng, faker_instance, force_hired=(cid, req.req_id) in must_hire_set))
        if (i + 1) % 5000 == 0:
            print(f"  Generated {i + 1} / {len(pairs)} rows...")

    print(f"\nInserting into '{db_config['database']}'...")
    insert_to_db(rows, db_config)


if __name__ == "__main__":
    main()
