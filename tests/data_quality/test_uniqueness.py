"""
Uniqueness tests for the RecruitIQ database.

These tests detect duplicate and near-duplicate records across the schema.
Some duplicates are impossible by construction (database unique constraints
enforce them at the storage layer), but others require application-level
detection — particularly near-duplicate candidates created by the deliberate
duplicate_candidate_rate injection in generate_candidates.py.

Tests marked `critical` verify that hard unique constraints are intact (a
violation here means the constraint was dropped or bypassed). Tests marked
`warning` detect structural anomalies that would produce incorrect counts in
analysis. Tests marked `info` tolerate deliberately injected duplicates and
use `pytest.xfail` for counts within the expected injection range.
"""

import pytest


@pytest.mark.critical
def test_no_duplicate_candidate_emails_exact(db_cursor):
    """
    No two candidates should share the same email address (case-sensitive).

    The candidates table has a UNIQUE constraint on email enforced by the
    database, so this test verifies that constraint is still in place and was
    not bypassed during bulk load. An exact email duplicate would cause
    any lookup-by-email to return multiple rows, breaking deduplication and
    candidate-matching logic.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM (
              SELECT email
                FROM candidates
               GROUP BY email
              HAVING COUNT(*) > 1
          ) dupes
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} email value(s) appear more than once in the candidates table. "
        f"The UNIQUE constraint on candidates.email should prevent this."
    )


@pytest.mark.info
def test_no_duplicate_candidate_emails_normalized(db_cursor):
    """
    After normalizing emails (lowercase, trimmed, dots removed from local part),
    no two candidates should resolve to the same address.

    The duplicate_candidate_rate defect in generate_candidates.py deliberately
    injects ~700 near-duplicate candidates whose emails differ only by case,
    leading/trailing whitespace, or dot placement — all equivalent under
    utf8mb4_unicode_ci or Gmail-style normalization. These duplicates would
    inflate candidate-pipeline counts and produce double-counting in source
    attribution reports. Counts in the range 1-1500 are treated as expected
    (XFAIL).
    """
    db_cursor.execute("""
        WITH normalized AS (
            SELECT
                id,
                LOWER(TRIM(REPLACE(SUBSTRING_INDEX(email, '@', 1), '.', ''))) AS norm_local,
                LOWER(SUBSTRING_INDEX(email, '@', -1))                         AS norm_domain
              FROM candidates
        )
        SELECT COUNT(*)
          FROM (
              SELECT norm_local, norm_domain, COUNT(*) AS c
                FROM normalized
               GROUP BY norm_local, norm_domain
              HAVING c > 1
          ) dupes
    """)
    count = db_cursor.fetchone()[0]

    if 0 < count < 1500:
        pytest.xfail(
            f"Deliberate near-duplicate candidate injection — found {count} normalized "
            f"email group(s) with more than one candidate. Expected ~700 duplicates "
            f"from duplicate_candidate_rate=0.02."
        )

    assert count == 0, (
        f"Unexpected escalation: {count} normalized email group(s) contain duplicates. "
        f"The injected defect rate produces ~700; a count of 1500 or more suggests a "
        f"generation or load error."
    )


@pytest.mark.info
def test_no_duplicate_candidates_by_name_phone(db_cursor):
    """
    No two candidates should share the same (first_name, last_name, phone)
    combination where phone is not NULL.

    Injected duplicate candidates retain the same name and phone number as
    the original, making this a complementary signal to the email-normalization
    check. Name+phone duplicates that survive email deduplication would silently
    inflate candidate counts and skew source-of-hire attribution for any
    de-duplicated funnel analysis. Counts in the range 1-1500 are treated as
    expected (XFAIL).
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM (
              SELECT first_name, last_name, phone, COUNT(*) AS c
                FROM candidates
               WHERE phone IS NOT NULL
               GROUP BY first_name, last_name, phone
              HAVING c > 1
          ) dupes
    """)
    count = db_cursor.fetchone()[0]

    if 0 < count < 1500:
        pytest.xfail(
            f"Deliberate near-duplicate candidate injection — found {count} "
            f"(first_name, last_name, phone) group(s) with more than one candidate. "
            f"Expected from duplicate_candidate_rate=0.02; injected candidates "
            f"retain the original's name and phone."
        )

    assert count == 0, (
        f"Unexpected escalation: {count} (first_name, last_name, phone) group(s) "
        f"contain duplicates. The injected defect rate produces a similar count to "
        f"the email check; a count of 1500 or more suggests a generation or load error."
    )


@pytest.mark.critical
def test_no_duplicate_req_codes(db_cursor):
    """
    No two job_requisitions should share the same req_code.

    req_code (e.g. REQ-2024-0042) is the human-readable identifier used in
    hiring manager communications, ATS integrations, and offer letters. A
    duplicate req_code would cause lookups by code to return multiple rows,
    making it impossible to uniquely route applications or attribute offers to
    the correct requisition. The UNIQUE constraint on req_code should prevent
    this at the storage layer.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM (
              SELECT req_code
                FROM job_requisitions
               GROUP BY req_code
              HAVING COUNT(*) > 1
          ) dupes
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} req_code value(s) appear more than once in job_requisitions. "
        f"The UNIQUE constraint on job_requisitions.req_code should prevent this."
    )


@pytest.mark.critical
def test_no_duplicate_applications(db_cursor):
    """
    No candidate should have more than one application to the same requisition.

    The applications table has a UNIQUE constraint on (candidate_id, requisition_id).
    A duplicate application would mean a candidate appears twice in the pipeline
    for the same role, inflating stage-conversion counts and offer rates for
    that requisition and producing incorrect time-to-fill metrics.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM (
              SELECT candidate_id, requisition_id, COUNT(*) AS c
                FROM applications
               GROUP BY candidate_id, requisition_id
              HAVING c > 1
          ) dupes
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} (candidate_id, requisition_id) pair(s) appear more than once in "
        f"applications. The UNIQUE constraint on (candidate_id, requisition_id) "
        f"should prevent this."
    )


@pytest.mark.warning
def test_offer_versions_sequential_per_application(db_cursor):
    """
    For each application, offer version numbers must be contiguous starting at 1
    — i.e., an application with two offers must have versions 1 and 2, not 1 and 3.

    A gap in version numbers (e.g., v1 and v3 with no v2) indicates that an
    intermediate offer row was deleted or never inserted. Version gaps would
    cause counter-offer negotiation analysis to misrepresent the number of
    rounds and break any ordered iteration over offer history.
    """
    db_cursor.execute("""
        WITH ranked AS (
            SELECT
                application_id,
                offer_version,
                ROW_NUMBER() OVER (
                    PARTITION BY application_id
                    ORDER BY offer_version
                ) AS expected_version
              FROM offers
        )
        SELECT COUNT(DISTINCT application_id)
          FROM ranked
         WHERE offer_version != expected_version
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} application(s) have non-sequential offer version numbers."
    )


@pytest.mark.critical
def test_pipeline_stage_order_unique_per_application(db_cursor):
    """
    Within each application, no two pipeline stages should share the same
    stage_order value.

    stage_order is the positional index used to sequence the hiring funnel —
    duplicate values would make it impossible to determine which stage came
    first, breaking funnel conversion rate calculations and any ordered
    traversal of the hiring pipeline for a given candidate.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM (
              SELECT application_id, stage_order, COUNT(*) AS c
                FROM pipeline_stages
               GROUP BY application_id, stage_order
              HAVING c > 1
          ) dupes
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} (application_id, stage_order) pair(s) appear more than once in "
        f"pipeline_stages. Each stage_order value must be unique per application."
    )
