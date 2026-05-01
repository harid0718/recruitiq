"""
Generates synthetic offers rows and inserts them into MySQL.

Covers all applications that reached the 'offer' pipeline stage, generates v1
offers with status and timing derived from the application's final outcome,
and adds v2 counter-offers for a subset of compensation-declined v1 offers.
A small number of orphan offers (no matching pipeline stage) are injected as a
data-quality defect.

Run from the project root:
    python scripts/generate_offers.py
"""

from __future__ import annotations

import os
import random
import sys
import time
from datetime import datetime, timedelta
from typing import NamedTuple

from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import (
    DATA_END_DATE,
    DATA_QUALITY_RATES,
    RANDOM_SEED,
)


class OfferContext(NamedTuple):
    """Application context for an app that reached the 'offer' pipeline stage."""
    app_id:           int
    app_status:       str          # active | rejected | withdrawn | hired
    seniority:        str | None
    salary_min:       float | None
    salary_max:       float | None
    offer_entered_at: datetime     # pipeline_stages.entered_at for stage_name='offer'
    offer_exited_at:  datetime | None  # NULL when app is still active


class OrphanAppInfo(NamedTuple):
    """Minimal context for applications that never reached the 'offer' stage."""
    app_id:     int
    seniority:  str | None
    salary_min: float | None
    salary_max: float | None
    applied_at: datetime


# =============================================================================
# Reference data
# =============================================================================

# Bonus target percentage band by seniority
_BONUS_RANGES: dict[str, tuple[float, float]] = {
    "intern":    ( 0.0,  5.0),
    "entry":     ( 5.0, 10.0),
    "mid":       ( 5.0, 10.0),
    "senior":    (10.0, 15.0),
    "staff":     (10.0, 15.0),
    "manager":   (15.0, 20.0),
    "director":  (15.0, 20.0),
    "vp":        (20.0, 30.0),
    "executive": (20.0, 30.0),
}

# Signing bonus range in dollars — generated with 30% probability
_SIGNING_RANGES: dict[str, tuple[int, int]] = {
    "intern":    (      0,  5_000),
    "entry":     (  5_000, 15_000),
    "mid":       (  5_000, 20_000),
    "senior":    ( 10_000, 30_000),
    "staff":     ( 10_000, 35_000),
    "manager":   ( 15_000, 40_000),
    "director":  ( 20_000, 50_000),
    "vp":        ( 25_000, 50_000),
    "executive": ( 30_000, 50_000),
}

# Equity grant value range in dollars — generated with 50% probability for non-interns
_EQUITY_RANGES: dict[str, tuple[int, int]] = {
    "entry":     ( 10_000,  50_000),
    "mid":       ( 25_000, 100_000),
    "senior":    ( 50_000, 200_000),
    "staff":     ( 75_000, 250_000),
    "manager":   ( 50_000, 200_000),
    "director":  (100_000, 400_000),
    "vp":        (150_000, 500_000),
    "executive": (200_000, 500_000),
}

_DECLINE_REASONS:  list[str] = ["compensation", "competing_offer", "location",
                                  "role_fit", "personal", "no_response", "other"]
_DECLINE_WEIGHTS:  list[int] = [35, 25, 10, 10, 10, 5, 5]

_INSERT_SQL = """
    INSERT INTO offers
        (application_id, offer_version, status, base_salary, bonus_target_pct,
         signing_bonus, equity_value, currency, proposed_start_date, offer_sent_at,
         offer_expires_at, offer_responded_at, decline_reason)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

_END_DT: datetime = datetime.combine(DATA_END_DATE, datetime.min.time())


# =============================================================================
# Private helpers
# =============================================================================

def _cap_dt(dt: datetime) -> datetime:
    return min(dt, _END_DT)


def _pick_salary(
    sal_min: float | None,
    sal_max: float | None,
    rng: random.Random,
) -> float:
    """
    Draw a base salary relative to the requisition's approved band.

    75% of offers fall within the band, 15% are stretch offers (5% above max),
    and 10% are tight-budget offers (5% below min). Result is rounded to the
    nearest $100.
    """
    min_v = sal_min if sal_min is not None else 60_000.0
    max_v = sal_max if sal_max is not None else 150_000.0

    strategy = rng.choices(["within", "stretch", "tight"], weights=[75, 15, 10], k=1)[0]
    if strategy == "within":
        raw = rng.uniform(min_v, max_v)
    elif strategy == "stretch":
        raw = max_v * 1.05
    else:  # tight
        raw = min_v * 0.95

    return float(round(raw / 100) * 100)


def _bonus_pct(seniority: str | None, rng: random.Random) -> float:
    low, high = _BONUS_RANGES.get(seniority or "mid", (5.0, 10.0))
    return round(rng.uniform(low, high), 1)


def _signing_bonus(seniority: str | None, rng: random.Random) -> float | None:
    if rng.random() >= 0.30:
        return None
    low, high = _SIGNING_RANGES.get(seniority or "mid", (5_000, 20_000))
    return float(round(rng.randint(low, high) / 1_000) * 1_000)


def _equity_value(seniority: str | None, rng: random.Random) -> float | None:
    if seniority == "intern" or rng.random() >= 0.50:
        return None
    low, high = _EQUITY_RANGES.get(seniority or "mid", (10_000, 100_000))
    return float(round(rng.randint(low, high) / 1_000) * 1_000)


# =============================================================================
# Core functions
# =============================================================================

def load_config() -> dict:
    from dotenv import load_dotenv
    load_dotenv()
    return {
        "host":     os.getenv("DB_HOST", "localhost"),
        "port":     int(os.getenv("DB_PORT", 3306)),
        "user":     os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
    }


def fetch_offer_candidates(
    db_config: dict,
) -> tuple[list[OfferContext], list[OrphanAppInfo]]:
    """
    Run two queries:

    1. OfferContext — applications with a 'offer' pipeline stage (includes all
       'hired' applications since they're forced through every stage).

    2. OrphanAppInfo — applications that never reached the 'offer' stage, used
       by inject_orphan_offers() to seed phantom offer rows.
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
                jr.seniority_level,
                jr.salary_range_min,
                jr.salary_range_max,
                ps.entered_at,
                ps.exited_at
              FROM applications a
              JOIN job_requisitions jr ON a.requisition_id = jr.id
              JOIN pipeline_stages  ps ON ps.application_id = a.id
                                      AND ps.stage_name = 'offer'
             ORDER BY a.id
        """)
        offer_contexts: list[OfferContext] = [
            OfferContext(
                app_id           = row[0],
                app_status       = row[1],
                seniority        = row[2],
                salary_min       = float(row[3]) if row[3] is not None else None,
                salary_max       = float(row[4]) if row[4] is not None else None,
                offer_entered_at = row[5],
                offer_exited_at  = row[6],
            )
            for row in cursor.fetchall()
        ]

        # Applications that never had an offer stage — candidates for orphan injection
        cursor.execute("""
            SELECT
                a.id,
                jr.seniority_level,
                jr.salary_range_min,
                jr.salary_range_max,
                a.applied_at
              FROM applications a
              JOIN job_requisitions jr ON a.requisition_id = jr.id
         LEFT JOIN pipeline_stages ps  ON ps.application_id = a.id
                                      AND ps.stage_name = 'offer'
             WHERE ps.id IS NULL
             ORDER BY a.id
        """)
        orphan_infos: list[OrphanAppInfo] = [
            OrphanAppInfo(
                app_id     = row[0],
                seniority  = row[1],
                salary_min = float(row[2]) if row[2] is not None else None,
                salary_max = float(row[3]) if row[3] is not None else None,
                applied_at = row[4],
            )
            for row in cursor.fetchall()
        ]

        return offer_contexts, orphan_infos

    except Error as e:
        print(f"MySQL error while fetching offer candidates: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()


def generate_v1_offer(ctx: OfferContext, rng: random.Random) -> tuple:
    """
    Build the v1 offer row for an application that reached the offer stage.

    Timing is anchored to the 'offer' pipeline stage:
      offer_sent_at      = pipeline_stages.entered_at for stage_name='offer'
      offer_responded_at = pipeline_stages.exited_at  (NULL for active apps)

    Offer status is derived from application status:
      hired     → accepted
      active    → sent  (still pending)
      rejected/withdrawn → declined (50%) | expired (30%) | rescinded (20%)
    """
    base_salary    = _pick_salary(ctx.salary_min, ctx.salary_max, rng)
    bonus_pct      = _bonus_pct(ctx.seniority, rng)
    signing        = _signing_bonus(ctx.seniority, rng)
    equity         = _equity_value(ctx.seniority, rng)

    offer_sent_at  = ctx.offer_entered_at
    offer_expires  = _cap_dt(offer_sent_at + timedelta(days=rng.randint(7, 14)))
    proposed_start = min(
        (offer_sent_at + timedelta(days=rng.randint(30, 60))).date(),
        DATA_END_DATE,
    )

    if ctx.app_status == "hired":
        status          = "accepted"
        offer_responded = ctx.offer_exited_at
        decline_reason  = None

    elif ctx.app_status == "active":
        status          = "sent"
        offer_responded = None
        decline_reason  = None

    else:  # rejected or withdrawn
        status = rng.choices(
            ["declined", "expired", "rescinded"],
            weights=[50, 30, 20],
            k=1,
        )[0]

        if status == "declined":
            offer_responded = (
                ctx.offer_exited_at
                or _cap_dt(offer_sent_at + timedelta(days=rng.randint(3, 14)))
            )
            decline_reason = rng.choices(
                _DECLINE_REASONS, weights=_DECLINE_WEIGHTS, k=1
            )[0]
        elif status == "expired":
            offer_responded = None   # no explicit response; offer just timed out
            decline_reason  = None
        else:  # rescinded
            offer_responded = None
            decline_reason  = None

    return (
        ctx.app_id,
        1,              # offer_version
        status,
        base_salary,
        bonus_pct,
        signing,
        equity,
        "USD",
        proposed_start,
        offer_sent_at,
        offer_expires,
        offer_responded,
        decline_reason,
    )


def generate_v2_offer(v1_row: tuple, rng: random.Random) -> tuple:
    """
    Build a v2 counter-offer for a v1 that was declined due to compensation.

    base_salary is increased 8–15% over v1. Timing starts 1–7 days after the
    v1 response. v2 can be accepted (70%), declined again (25%), or expire (5%).

    Data quality note: if v2 is accepted, the parent application.status remains
    'rejected' or 'withdrawn' in the database — an intentional discrepancy that
    represents an ATS sync failure.
    """
    v1_responded_at: datetime = v1_row[11]  # guaranteed non-NULL for declined v1

    v2_base    = float(round(v1_row[3] * (1 + rng.uniform(0.08, 0.15)) / 100) * 100)
    v2_sent_at = _cap_dt(v1_responded_at + timedelta(days=rng.randint(1, 7)))
    v2_expires = _cap_dt(v2_sent_at + timedelta(days=rng.randint(7, 14)))
    v2_start   = min(
        (v2_sent_at + timedelta(days=rng.randint(30, 60))).date(),
        DATA_END_DATE,
    )

    v2_status = rng.choices(
        ["accepted", "declined", "expired"],
        weights=[70, 25, 5],
        k=1,
    )[0]

    if v2_status == "accepted":
        v2_responded  = _cap_dt(v2_sent_at + timedelta(days=rng.randint(1, 7)))
        v2_decline    = None
    elif v2_status == "declined":
        v2_responded  = _cap_dt(v2_sent_at + timedelta(days=rng.randint(1, 10)))
        v2_decline    = rng.choices(_DECLINE_REASONS, weights=_DECLINE_WEIGHTS, k=1)[0]
    else:  # expired
        v2_responded  = None
        v2_decline    = None

    return (
        v1_row[0],   # application_id
        2,           # offer_version
        v2_status,
        v2_base,
        v1_row[4],   # reuse bonus_target_pct from v1
        v1_row[5],   # reuse signing_bonus from v1
        v1_row[6],   # reuse equity_value from v1
        "USD",
        v2_start,
        v2_sent_at,
        v2_expires,
        v2_responded,
        v2_decline,
    )


def inject_orphan_offers(
    all_rows: list[tuple],
    orphan_infos: list[OrphanAppInfo],
    rng: random.Random,
) -> None:
    """
    Append phantom offer rows for applications that never had an 'offer'
    pipeline stage. These represent an ATS data-sync failure where an offer
    record exists in the offers table but no corresponding pipeline stage row
    exists — detectable by a LEFT JOIN between the two tables.
    """
    n_orphans = round(len(all_rows) * DATA_QUALITY_RATES["orphan_offer_rate"])
    if not n_orphans or not orphan_infos:
        return

    selected = rng.sample(orphan_infos, min(n_orphans, len(orphan_infos)))

    for info in selected:
        offer_sent_at  = _cap_dt(info.applied_at + timedelta(days=rng.randint(30, 90)))
        offer_expires  = _cap_dt(offer_sent_at + timedelta(days=rng.randint(7, 14)))
        proposed_start = min(
            (offer_sent_at + timedelta(days=rng.randint(30, 60))).date(),
            DATA_END_DATE,
        )

        all_rows.append((
            info.app_id,
            1,          # offer_version
            "sent",     # phantom offer with no response — still "pending"
            _pick_salary(info.salary_min, info.salary_max, rng),
            _bonus_pct(info.seniority, rng),
            _signing_bonus(info.seniority, rng),
            _equity_value(info.seniority, rng),
            "USD",
            proposed_start,
            offer_sent_at,
            offer_expires,
            None,   # offer_responded_at
            None,   # decline_reason
        ))

    print(f"Injected {len(selected)} orphan offer rows (no matching 'offer' pipeline stage).")


def generate_all(
    offer_contexts: list[OfferContext],
    orphan_infos: list[OrphanAppInfo],
    rng: random.Random,
) -> list[tuple]:
    all_rows:      list[tuple] = []
    n_v2_accepted: int         = 0

    for ctx in offer_contexts:
        v1 = generate_v1_offer(ctx, rng)
        all_rows.append(v1)

        # Counter-offer: 30% of compensation-declined v1 offers get a v2
        if v1[2] == "declined" and v1[12] == "compensation" and rng.random() < 0.30:
            v2 = generate_v2_offer(v1, rng)
            all_rows.append(v2)
            if v2[2] == "accepted":
                # Data quality: application.status is still 'rejected'/'withdrawn'
                # but a v2 offer was accepted. Intentional discrepancy — do not fix.
                n_v2_accepted += 1

    n_v1  = len(offer_contexts)
    n_v2  = len(all_rows) - n_v1

    print(f"Generated {n_v1} v1 offers, {n_v2} v2 counter-offers.")

    if n_v2_accepted:
        print(
            f"  Data quality note: {n_v2_accepted} v2 offers accepted while "
            f"application.status remains 'rejected'/'withdrawn'. "
            f"Detectable via offer.status='accepted' JOIN application.status check."
        )

    inject_orphan_offers(all_rows, orphan_infos, rng)
    print(f"Total offer rows: {len(all_rows)}.")

    return all_rows


def insert_to_db(rows: list[tuple], db_config: dict) -> None:
    """Single atomic transaction — all rows committed or none."""
    connection = None
    cursor     = None
    try:
        connection = mysql.connector.connect(**db_config)
        cursor     = connection.cursor()

        t0 = time.perf_counter()
        cursor.executemany(_INSERT_SQL, rows)
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

    print("Fetching offer candidates from database...")
    offer_contexts, orphan_infos = fetch_offer_candidates(db_config)
    print(
        f"  Applications with offer stage: {len(offer_contexts):,}  |  "
        f"Applications without offer stage (orphan pool): {len(orphan_infos):,}"
    )

    rng = random.Random(RANDOM_SEED)

    print("Generating offers...")
    rows = generate_all(offer_contexts, orphan_infos, rng)

    print(f"\nInserting into '{db_config['database']}'...")
    insert_to_db(rows, db_config)


if __name__ == "__main__":
    main()
