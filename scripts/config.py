"""Config for the data generators."""

from datetime import date

# seed for faker + random; bump if you want different fake data
RANDOM_SEED = 42

# target row counts. pipeline_stages and offers come out of the
# generator logic, so these are estimates not exact targets.
VOLUMES = {
    "job_requisitions": 500,
    "candidates": 35000,
    "applications": 45000,
    "expected_pipeline_stages": 110000,
    "expected_offers": 1800,
}

# synthetic data spans Jan 2023 - Oct 2025
DATA_START_DATE = date(2023, 1, 1)
DATA_END_DATE = date(2025, 10, 31)

DEPARTMENTS = [
    "Engineering", "Manufacturing", "Sales", "Recruiting",
    "People Operations", "Finance", "Legal", "Supply Chain",
    "Service", "Design",
]

# (city, state, country). state is "" for non-US locations.
LOCATIONS = [
    ("Austin", "TX", "US"),
    ("Fremont", "CA", "US"),
    ("Reno", "NV", "US"),
    ("Buffalo", "NY", "US"),
    ("Berlin", "", "DE"),
    ("Shanghai", "", "CN"),
    ("Remote", "", ""),
]

# rates of intentional data quality issues we inject into the
# generated data so the test suite has something to catch.
# bump to 0 to disable any of these.
DATA_QUALITY_RATES = {
    "duplicate_candidate": 0.02,
    "missing_department": 0.01,
    "invalid_interview_date": 0.005,
    "orphan_offer": 0.003,
    "out_of_order_stages": 0.01,
    "null_hiring_manager": 0.03,
}
