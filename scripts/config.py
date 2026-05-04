"""
Configuration constants for the RecruitIQ synthetic data generator.

All volumes, date ranges, reference lists, and data-quality injection rates are
defined here so that the generator scripts have a single source of truth.
Adjust these values to produce larger/smaller datasets or to tune the ratio of
intentional data-quality issues used for analytics exercises.
"""

from datetime import date


RANDOM_SEED: int = 42  # Fixed seed ensures identical output on every run

# =============================================================================
# Target row volumes
# =============================================================================
# pipeline_stages and offers are derived from applications during generation;
# the values below are expected approximations, not direct generation targets.

VOLUMES: dict[str, int] = {
    "job_requisitions": 500,
    "candidates": 35_000,
    "applications": 45_000,
    "expected_pipeline_stages": 110_000,  # ~2.4 stages per application on average
    "expected_offers": 1_800,             # ~4% of applications reach an offer
}

# =============================================================================
# Synthetic date range
# =============================================================================

DATA_START_DATE: date = date(2023, 1, 1)
DATA_END_DATE: date = date(2025, 10, 31)

# =============================================================================
# Reference lists
# =============================================================================

DEPARTMENTS: list[str] = [
    "Engineering",
    "Manufacturing",
    "Sales",
    "Recruiting",
    "People Operations",
    "Finance",
    "Legal",
    "Supply Chain",
    "Service",
    "Design",
]

# Each tuple: (city, state_or_province, iso_country_code)
# State is an empty string for non-US locations and for "Remote".
LOCATIONS: list[tuple[str, str, str]] = [
    ("Austin",   "TX", "US"),
    ("Fremont",  "CA", "US"),
    ("Reno",     "NV", "US"),
    ("Buffalo",  "NY", "US"),
    ("Berlin",   "",   "DE"),
    ("Shanghai", "",   "CN"),
    ("Remote",   "",   ""),
]

# =============================================================================
# Data-quality injection rates
# =============================================================================
# These rates control the proportion of rows that receive intentional defects.
# Defects are seeded so that downstream cleaning and analytics exercises have
# realistic noise to work with. Set any rate to 0.0 to disable that defect.

DATA_QUALITY_RATES: dict[str, float] = {
    # Candidates inserted with a near-duplicate email (e.g. extra whitespace)
    "duplicate_candidate_rate": 0.02,
    # job_requisitions rows where department is set to NULL
    "missing_department_rate": 0.01,
    # pipeline_stages rows where entered_at predates the application's applied_at
    "invalid_interview_date_rate": 0.005,
    # offers rows whose application_id references an application with status != 'active'
    "orphan_offer_rate": 0.003,
    # pipeline_stages rows where stage_order does not match the chronological sequence
    "out_of_order_stages_rate": 0.01,
    # job_requisitions rows where hiring_manager_name is set to NULL
    "null_hiring_manager_rate": 0.03,
}
