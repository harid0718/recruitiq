"""
Generates synthetic candidates rows and inserts them into MySQL.

Produces VOLUMES["candidates"] unique base records, then injects a small
percentage of near-duplicate records with mutated email addresses to support
downstream data-quality and fuzzy-matching analytics exercises.

Run from the project root:
    python scripts/generate_candidates.py
"""

from __future__ import annotations

import os
import random
import re
import sys
import time

from dotenv import load_dotenv
from faker import Faker
import mysql.connector
from mysql.connector import Error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import (
    DATA_QUALITY_RATES,
    RANDOM_SEED,
    VOLUMES,
)

# =============================================================================
# Reference data
# =============================================================================

# Email domains with sampling weights (out of 1000).
# Consumer 88%  |  .edu 7%  |  company domains 5%
_ALL_DOMAINS: list[str] = [
    "gmail.com", "outlook.com", "yahoo.com", "hotmail.com", "icloud.com",
    "mit.edu", "stanford.edu", "berkeley.edu", "umich.edu", "gatech.edu",
    "ucla.edu", "utexas.edu", "cornell.edu", "harvard.edu", "uw.edu",
    "acme-corp.com", "globaltech.io", "nexusworks.com", "brightpath.co", "stellargroup.net",
]
_DOMAIN_WEIGHTS: list[int] = [
    500, 150, 100, 80, 50,          # consumer: 880
     7,   7,   7,  7,  7,  7, 7, 7, 7, 7,  # edu: 70 (10 × 7)
    10,  10,  10, 10, 10,           # company: 50
]  # total = 1000

_US_CITIES: list[tuple[str, str]] = [
    ("Austin", "TX"), ("Seattle", "WA"), ("San Francisco", "CA"),
    ("New York", "NY"), ("Chicago", "IL"), ("Houston", "TX"),
    ("Los Angeles", "CA"), ("Boston", "MA"), ("Denver", "CO"),
    ("Atlanta", "GA"), ("Miami", "FL"), ("Phoenix", "AZ"),
    ("Portland", "OR"), ("Minneapolis", "MN"), ("Nashville", "TN"),
    ("Dallas", "TX"), ("San Diego", "CA"), ("Detroit", "MI"),
    ("Philadelphia", "PA"), ("Raleigh", "NC"),
]

# International cities grouped by ISO country code
_INTL_CITIES_BY_COUNTRY: dict[str, list[tuple[str, str]]] = {
    "CA": [("Toronto", "ON"), ("Vancouver", "BC"), ("Montreal", "QC"), ("Calgary", "AB")],
    "GB": [("London", ""), ("Manchester", ""), ("Edinburgh", ""), ("Bristol", "")],
    "DE": [("Berlin", ""), ("Munich", ""), ("Hamburg", ""), ("Frankfurt", "")],
    "IN": [("Bangalore", ""), ("Mumbai", ""), ("Hyderabad", ""), ("Pune", ""), ("Chennai", "")],
    "CN": [("Shanghai", ""), ("Beijing", ""), ("Shenzhen", ""), ("Guangzhou", "")],
}

# "Other" country pool — (city, state_or_empty, iso_country_code)
_OTHER_INTL_CITIES: list[tuple[str, str, str]] = [
    ("Sydney", "NSW", "AU"), ("Melbourne", "VIC", "AU"),
    ("Paris", "", "FR"), ("Amsterdam", "", "NL"),
    ("Singapore", "", "SG"), ("Tokyo", "", "JP"),
    ("São Paulo", "", "BR"), ("Mexico City", "", "MX"),
    ("Dublin", "", "IE"), ("Stockholm", "", "SE"),
]

_CURRENT_TITLES: list[str] = [
    # Tech
    "Software Engineer", "Senior Software Engineer", "Staff Software Engineer",
    "Data Scientist", "Machine Learning Engineer", "DevOps Engineer",
    "Product Manager", "UX Designer", "QA Engineer", "IT Manager",
    "Systems Administrator", "Cybersecurity Analyst", "Solutions Architect",
    # Business / Finance
    "Financial Analyst", "Senior Financial Analyst", "Accounting Manager",
    "Controller", "Business Analyst", "Operations Manager",
    # Sales / Marketing
    "Account Executive", "Sales Director", "Marketing Manager",
    "Brand Manager", "Content Strategist", "Growth Manager",
    "Customer Success Manager", "Sales Development Representative",
    # HR / Recruiting
    "Recruiter", "HR Business Partner", "Compensation Analyst",
    "People Operations Manager",
    # Supply Chain / Ops
    "Supply Chain Analyst", "Logistics Manager", "Procurement Specialist",
    "Project Manager", "Program Manager",
    # Design / Creative
    "Product Designer", "Brand Designer", "Creative Director",
    # Other professional
    "Consultant", "Research Scientist", "Legal Counsel", "Paralegal",
    "Nurse Practitioner", "Teacher", "Civil Engineer", "Mechanical Engineer",
]

# Years-of-experience bands and their relative sampling weights
_EXP_RANGES: list[tuple[int, int]] = [(0, 2), (2, 5), (5, 10), (10, 15), (15, 25)]
_EXP_WEIGHTS: list[int]            = [   20,    30,      25,       15,       10]

_EDUCATION_LEVELS:  list[str] = ["bachelor", "master", "associate", "mba", "phd", "high_school"]
_EDUCATION_WEIGHTS: list[int] = [        55,       25,          10,     5,     3,             2]

# Mutation strategies applied when injecting duplicate candidates
_DUP_STRATEGIES: list[str] = ["dot_removal", "case_change", "whitespace", "number_append"]

_INSERT_SQL = """
    INSERT INTO candidates
        (first_name, last_name, email, phone, current_title, current_company,
         location_city, location_state, location_country, linkedin_url,
         years_of_experience, highest_education_level)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


# =============================================================================
# Helpers
# =============================================================================

def _clean(name: str) -> str:
    """Lowercase and strip non-alphanumeric characters for use in email/URL slugs."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _generate_location(rng: random.Random) -> tuple[str, str, str]:
    """Return (city, state, country_code) drawn from the configured distribution."""
    if rng.random() < 0.70:
        city, state = rng.choice(_US_CITIES)
        return city, state, "US"

    # International breakdown (weights proportional to per-country share of all rows):
    # CA 5%, GB 5%, DE 5%, IN 5%, CN 3%, other 7% — sums to the 30% international share
    country = rng.choices(
        ["CA", "GB", "DE", "IN", "CN", "other"],
        weights=[5, 5, 5, 5, 3, 7],
        k=1,
    )[0]

    if country == "other":
        city, state, country_code = rng.choice(_OTHER_INTL_CITIES)
        return city, state, country_code

    city, state = rng.choice(_INTL_CITIES_BY_COUNTRY[country])
    return city, state, country


def _mutate_email(
    local: str,
    domain: str,
    strategy: str,
    rng: random.Random,
    used_lower: set[str],
) -> str:
    """
    Apply one mutation to produce a near-duplicate email address.

    Collision caveat: 'case_change' produces a string that is equal to the
    original under utf8mb4_unicode_ci (MySQL's case-insensitive default), so
    it is detected via used_lower and replaced with a number-append fallback.
    'whitespace' uses a leading space, which CI does not strip, so it passes.
    """
    if strategy == "dot_removal":
        new_local = local.replace(".", "", 1)   # john.smith  → johnsmith
    elif strategy == "case_change":
        new_local = local.title()               # john.smith  → John.Smith (falls back below)
    elif strategy == "whitespace":
        new_local = " " + local                 # john.smith  → " john.smith" (leading space)
    else:  # number_append
        new_local = f"{local}{rng.randint(2, 9)}"

    candidate = f"{new_local}@{domain}"

    # Fall back to number_append when: CI collision detected, or local part unchanged
    # (e.g. dot_removal on an email with no dots)
    if candidate.lower() in used_lower or new_local == local:
        counter = 2
        while f"{local}{counter}@{domain}".lower() in used_lower:
            counter += 1
        candidate = f"{local}{counter}@{domain}"

    return candidate


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


def generate_email(
    first: str,
    last: str,
    rng: random.Random,
    used_lower: set[str],
) -> str:
    """
    Build a firstname.lastname@domain email. Appends a random 1-99 suffix to
    ~30% of addresses to simulate natural name collisions. Resolves any
    remaining collisions with an incrementing counter.
    """
    f = _clean(first) or "user"
    l = _clean(last) or "anon"
    domain = rng.choices(_ALL_DOMAINS, weights=_DOMAIN_WEIGHTS, k=1)[0]

    local = f"{f}.{l}"
    if rng.random() < 0.30:
        local = f"{local}{rng.randint(1, 99)}"

    email = f"{local}@{domain}"

    if email.lower() in used_lower:
        counter = 2
        while f"{f}.{l}{counter}@{domain}".lower() in used_lower:
            counter += 1
        email = f"{f}.{l}{counter}@{domain}"

    used_lower.add(email.lower())
    return email


def generate_candidate(
    faker_instance: Faker,
    rng: random.Random,
    used_lower: set[str],
) -> tuple:
    first_name = faker_instance.first_name()
    last_name  = faker_instance.last_name()

    email          = generate_email(first_name, last_name, rng, used_lower)
    phone          = faker_instance.phone_number()
    current_title  = rng.choice(_CURRENT_TITLES)
    current_company = faker_instance.company()

    location_city, location_state, location_country = _generate_location(rng)

    # 80% of candidates have a LinkedIn profile
    if rng.random() < 0.80:
        f = _clean(first_name) or "user"
        l = _clean(last_name) or "anon"
        linkedin_url: str | None = f"https://linkedin.com/in/{f}-{l}-{rng.randint(1000, 9999)}"
    else:
        linkedin_url = None

    # Years of experience with 0.5-year precision
    low, high = rng.choices(_EXP_RANGES, weights=_EXP_WEIGHTS, k=1)[0]
    years_of_experience = round(rng.uniform(low, high) * 2) / 2

    education = rng.choices(_EDUCATION_LEVELS, weights=_EDUCATION_WEIGHTS, k=1)[0]

    return (
        first_name,
        last_name,
        email,
        phone,
        current_title,
        current_company,
        location_city,
        location_state,
        location_country,
        linkedin_url,
        years_of_experience,
        education,
    )


def inject_duplicates(
    base_rows: list[tuple],
    n_dupes: int,
    rng: random.Random,
    used_lower: set[str],
) -> list[tuple]:
    """
    Copy n_dupes random base candidates and mutate their email address.

    The result models the same real person submitted multiple times with
    slightly different contact details — a common ATS data quality problem
    that is detectable via fuzzy matching but not by a simple exact-match join.
    All other fields (name, phone, company) are preserved unchanged.
    """
    dupes: list[tuple] = []

    for _ in range(n_dupes):
        original = rng.choice(base_rows)
        original_email: str = original[2]  # email is at index 2

        local, domain = original_email.split("@", 1)
        strategy  = rng.choice(_DUP_STRATEGIES)
        new_email = _mutate_email(local, domain, strategy, rng, used_lower)

        dupe = original[:2] + (new_email,) + original[3:]
        dupes.append(dupe)
        used_lower.add(new_email.lower())

    return dupes


def generate_all(n: int) -> list[tuple]:
    n_dupes = round(n * DATA_QUALITY_RATES["duplicate_candidate_rate"])

    rng = random.Random(RANDOM_SEED)
    faker_instance = Faker()
    Faker.seed(RANDOM_SEED)
    used_lower: set[str] = set()

    rows: list[tuple] = []
    for i in range(n):
        rows.append(generate_candidate(faker_instance, rng, used_lower))
        if (i + 1) % 5000 == 0:
            print(f"  Generated {i + 1} / {n} base candidates...")

    print(f"Generated {n} base candidates.")

    dupes = inject_duplicates(rows, n_dupes, rng, used_lower)
    rows.extend(dupes)
    print(
        f"Injected {len(dupes)} duplicate rows "
        f"({DATA_QUALITY_RATES['duplicate_candidate_rate']:.0%} rate). "
        f"Total: {len(rows)} rows."
    )

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
    n = VOLUMES["candidates"]
    n_dupes = round(n * DATA_QUALITY_RATES["duplicate_candidate_rate"])

    print(f"Generating {n} base candidates + ~{n_dupes} duplicates...")
    rows = generate_all(n)

    print(f"Inserting into '{db_config['database']}'...")
    insert_to_db(rows, db_config)


if __name__ == "__main__":
    main()
