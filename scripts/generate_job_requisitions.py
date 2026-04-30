"""
Generates synthetic job_requisitions rows and inserts them into MySQL.

Run from the project root:
    python scripts/generate_job_requisitions.py
"""

from __future__ import annotations

import os
import random
import sys
import time
from datetime import date, datetime, timedelta

from dotenv import load_dotenv
from faker import Faker
import mysql.connector
from mysql.connector import Error

# Allow sibling imports when run from project root (python scripts/...)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import (
    DATA_END_DATE,
    DATA_QUALITY_RATES,
    DATA_START_DATE,
    DEPARTMENTS,
    LOCATIONS,
    RANDOM_SEED,
    VOLUMES,
)

# =============================================================================
# Reference data
# =============================================================================

_TITLES_BY_DEPT: dict[str, list[str]] = {
    "Engineering": [
        "Software Engineer", "Senior Software Engineer", "Staff Software Engineer",
        "Backend Engineer", "Frontend Engineer", "Full Stack Engineer",
        "DevOps Engineer", "Site Reliability Engineer", "Data Engineer",
        "Machine Learning Engineer", "Engineering Manager",
    ],
    "Manufacturing": [
        "Manufacturing Engineer", "Process Engineer", "Quality Engineer",
        "Industrial Engineer", "Production Supervisor", "Plant Manager",
        "Maintenance Technician", "Operations Manager",
    ],
    "Sales": [
        "Account Executive", "Sales Development Representative",
        "Enterprise Account Manager", "Sales Engineer", "Regional Sales Manager",
        "Director of Sales", "VP of Sales", "Sales Operations Analyst",
    ],
    "Recruiting": [
        "Recruiter", "Senior Recruiter", "Technical Recruiter",
        "Recruiting Coordinator", "Sourcing Specialist", "Head of Recruiting",
    ],
    "People Operations": [
        "HR Business Partner", "HR Generalist", "Compensation Analyst",
        "Benefits Specialist", "People Operations Manager",
        "Director of People Operations", "HRIS Analyst",
    ],
    "Finance": [
        "Financial Analyst", "Senior Financial Analyst", "Staff Accountant",
        "Accounting Manager", "FP&A Analyst", "Controller",
        "Senior Manager of Finance", "VP of Finance",
    ],
    "Legal": [
        "Corporate Counsel", "Senior Corporate Counsel", "Paralegal",
        "Legal Operations Manager", "Associate General Counsel",
        "Chief Legal Officer",
    ],
    "Supply Chain": [
        "Supply Chain Analyst", "Logistics Coordinator", "Procurement Analyst",
        "Inventory Analyst", "Logistics Manager", "Procurement Manager",
        "Director of Supply Chain", "VP of Supply Chain",
    ],
    "Service": [
        "Customer Success Manager", "Technical Support Specialist",
        "Field Service Technician", "Support Engineer",
        "Service Manager", "Director of Customer Success",
    ],
    "Design": [
        "Product Designer", "Senior Product Designer", "UX Researcher",
        "Brand Designer", "Motion Designer", "Design Manager", "Head of Design",
    ],
}

_TEAMS_BY_DEPT: dict[str, list[str]] = {
    "Engineering":       ["Backend Platform", "Frontend", "Data Infrastructure", "Mobile",
                          "Security", "Developer Experience", "ML Platform", "API & Integrations"],
    "Manufacturing":     ["Assembly", "Quality Assurance", "Process Engineering", "Facilities"],
    "Sales":             ["Enterprise", "Mid-Market", "SMB", "Sales Operations", "Partnerships"],
    "Recruiting":        ["Technical Recruiting", "G&A Recruiting", "University Programs"],
    "People Operations": ["HR Business Partners", "Compensation & Benefits", "HRIS", "L&D"],
    "Finance":           ["FP&A", "Accounting", "Tax", "Treasury", "Internal Audit"],
    "Legal":             ["Corporate", "Employment", "Regulatory", "Privacy"],
    "Supply Chain":      ["Procurement", "Logistics", "Planning", "Inventory Management"],
    "Service":           ["Technical Support", "Customer Success", "Field Service"],
    "Design":            ["Product Design", "Brand", "Research", "Motion"],
}

# Salary band (floor, ceiling) per seniority level; used to derive realistic
# salary_range_min and salary_range_max for each requisition.
_SALARY_BY_SENIORITY: dict[str, tuple[int, int]] = {
    "intern":    ( 25_000,  60_000),
    "entry":     ( 60_000,  90_000),
    "mid":       ( 90_000, 130_000),
    "senior":    (130_000, 180_000),
    "staff":     (160_000, 220_000),
    "manager":   (140_000, 200_000),
    "director":  (180_000, 280_000),
    "vp":        (220_000, 350_000),
    "executive": (250_000, 400_000),
}

# Sampling weights aligned positionally with their respective list constants.
# Engineering (30%) + Manufacturing (20%) ≈ 50% of all reqs.
_DEPT_WEIGHTS: list[int] = [30, 20, 15, 5, 5, 8, 3, 7, 5, 2]

# Austin (25%) + Fremont (25%) ≈ 50% of all reqs.
_LOCATION_WEIGHTS: list[int] = [25, 25, 10, 10, 10, 10, 10]

_EMPLOYMENT_TYPES: list[str] = ["full_time", "part_time", "contract", "intern"]
_EMPLOYMENT_WEIGHTS: list[int] = [85, 5, 5, 5]

_SENIORITY_LEVELS: list[str] = ["entry", "mid", "senior", "staff", "manager", "director", "vp", "executive"]
_SENIORITY_WEIGHTS: list[int] = [15, 30, 25, 10, 10, 5, 3, 2]

_STATUSES: list[str] = ["open", "filled", "on_hold", "cancelled", "draft"]
_STATUS_WEIGHTS: list[int] = [40, 35, 10, 10, 5]

_INSERT_SQL = """
    INSERT INTO job_requisitions
        (req_code, title, department, team, location, employment_type,
         seniority_level, hiring_manager_name, recruiter_name, status,
         headcount, target_start_date, salary_range_min, salary_range_max,
         salary_currency, opened_at, closed_at)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# =============================================================================
# Helpers
# =============================================================================

def _format_location(loc: tuple[str, str, str]) -> str:
    city, state, country = loc
    if city == "Remote":
        return "Remote"
    if country == "US":
        return f"{city}, {state}"
    return f"{city}, {country}"


def _random_datetime(rng: random.Random, start: date, end: date) -> datetime:
    offset = timedelta(
        days=rng.randint(0, (end - start).days),
        seconds=rng.randint(0, 86_399),
    )
    return datetime.combine(start, datetime.min.time()) + offset


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


def generate_requisition(
    faker_instance: Faker,
    rng: random.Random,
    year_counters: dict[int, int],
) -> tuple:
    # Department — null out for missing_department_rate fraction of rows
    dept: str | None = rng.choices(DEPARTMENTS, weights=_DEPT_WEIGHTS, k=1)[0]
    if rng.random() < DATA_QUALITY_RATES["missing_department_rate"]:
        dept = None

    # Title and team fall back to generic values when dept is None
    title_pool = _TITLES_BY_DEPT.get(dept, ["Operations Specialist", "Business Analyst", "Program Manager"]) if dept else ["Operations Specialist", "Business Analyst", "Program Manager"]
    title = rng.choice(title_pool)

    team_pool = _TEAMS_BY_DEPT.get(dept, []) if dept else []
    team: str | None = rng.choice(team_pool) if team_pool else None

    location = _format_location(rng.choices(LOCATIONS, weights=_LOCATION_WEIGHTS, k=1)[0])

    employment_type = rng.choices(_EMPLOYMENT_TYPES, weights=_EMPLOYMENT_WEIGHTS, k=1)[0]
    seniority_level = rng.choices(_SENIORITY_LEVELS, weights=_SENIORITY_WEIGHTS, k=1)[0]

    # Hiring manager name — null out for null_hiring_manager_rate fraction of rows
    hiring_manager_name: str | None = faker_instance.name()
    if rng.random() < DATA_QUALITY_RATES["null_hiring_manager_rate"]:
        hiring_manager_name = None
    recruiter_name = faker_instance.name()

    status = rng.choices(_STATUSES, weights=_STATUS_WEIGHTS, k=1)[0]
    headcount = rng.choices([1, 2, 3], weights=[80, 15, 5], k=1)[0]

    opened_at = _random_datetime(rng, DATA_START_DATE, DATA_END_DATE)

    # Target start: 30–90 days after opening, hard-capped at DATA_END_DATE
    raw_start = (opened_at + timedelta(days=rng.randint(30, 90))).date()
    target_start_date = min(raw_start, DATA_END_DATE)

    # Salary range: min drawn from the lower half of the band,
    # max from the upper half, guaranteeing at least a $5k spread.
    sal_floor, sal_ceiling = _SALARY_BY_SENIORITY[seniority_level]
    midpoint = (sal_floor + sal_ceiling) // 2
    salary_range_min = rng.randint(sal_floor, midpoint)
    salary_range_max = rng.randint(midpoint, sal_ceiling)
    if salary_range_max <= salary_range_min:
        salary_range_max = salary_range_min + 5_000

    # Closed at: only set for terminal statuses; capped at DATA_END_DATE
    closed_at: datetime | None = None
    if status in ("filled", "cancelled"):
        raw_close = opened_at + timedelta(days=rng.randint(30, 180))
        end_dt = datetime.combine(DATA_END_DATE, datetime.min.time())
        closed_at = min(raw_close, end_dt)

    # req_code: REQ-YYYY-NNNN sequential within each calendar year
    year = opened_at.year
    year_counters[year] = year_counters.get(year, 0) + 1
    req_code = f"REQ-{year}-{year_counters[year]:04d}"

    return (
        req_code,
        title,
        dept,
        team,
        location,
        employment_type,
        seniority_level,
        hiring_manager_name,
        recruiter_name,
        status,
        headcount,
        target_start_date,
        salary_range_min,
        salary_range_max,
        "USD",
        opened_at,
        closed_at,
    )


def generate_all(n: int) -> list[tuple]:
    rng = random.Random(RANDOM_SEED)
    faker_instance = Faker()
    Faker.seed(RANDOM_SEED)
    year_counters: dict[int, int] = {}

    rows: list[tuple] = []
    for i in range(n):
        rows.append(generate_requisition(faker_instance, rng, year_counters))
        if (i + 1) % 100 == 0:
            print(f"  Generated {i + 1} / {n} requisitions...")

    print(f"Generated {n} requisitions.")
    return rows


def insert_to_db(rows: list[tuple], db_config: dict) -> None:
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

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
    n = VOLUMES["job_requisitions"]

    print(f"Generating {n} job requisitions...")
    rows = generate_all(n)

    print(f"Inserting into '{db_config['database']}'...")
    insert_to_db(rows, db_config)


if __name__ == "__main__":
    main()
