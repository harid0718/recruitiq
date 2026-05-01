"""
Referential integrity tests for the RecruitIQ database.

These tests verify that every foreign-key relationship in the schema is
satisfied — i.e., no child row references a parent that doesn't exist.
Because the schema uses ON DELETE RESTRICT, these violations cannot occur
through normal ORM usage, but they can appear after bulk data loads, manual
imports, or partial rollbacks. All tests in this module are marked `critical`:
a failure here means queries that assume FK integrity will produce silent
under-counts or incorrect joins.

One intentional exception is test_offers_have_pipeline_stage, which checks a
business-rule relationship (not a hard FK) and is expected to find a small
number of deliberately injected orphan offer rows.
"""

import pytest


@pytest.mark.critical
def test_no_orphan_applications_to_candidates(db_cursor):
    """
    Every applications.candidate_id must reference a row in candidates.

    A broken reference here would mean an application record exists for a
    person who is no longer (or never was) in the system — silently dropping
    that application from any candidate-joined query.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM applications a
          LEFT JOIN candidates c ON a.candidate_id = c.id
         WHERE c.id IS NULL
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} application(s) reference a candidate_id that does not exist "
        f"in the candidates table."
    )


@pytest.mark.critical
def test_no_orphan_applications_to_requisitions(db_cursor):
    """
    Every applications.requisition_id must reference a row in job_requisitions.

    A broken reference would silently exclude those applications from any
    analysis that joins to job_requisitions (e.g. time-to-fill, source mix
    by department), producing metrics calculated on an incomplete population.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM applications a
          LEFT JOIN job_requisitions jr ON a.requisition_id = jr.id
         WHERE jr.id IS NULL
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} application(s) reference a requisition_id that does not exist "
        f"in the job_requisitions table."
    )


@pytest.mark.critical
def test_no_orphan_pipeline_stages(db_cursor):
    """
    Every pipeline_stages.application_id must reference a row in applications.

    Orphan stage rows would make funnel conversion metrics unreliable: the
    stage count would include events that can never be joined back to a
    candidate, department, or requisition.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM pipeline_stages ps
          LEFT JOIN applications a ON ps.application_id = a.id
         WHERE a.id IS NULL
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} pipeline_stage row(s) reference an application_id that does "
        f"not exist in the applications table."
    )


@pytest.mark.critical
def test_no_orphan_offers_to_applications(db_cursor):
    """
    Every offers.application_id must reference a row in applications.

    An offer with no parent application cannot be attributed to a candidate,
    a requisition, or a department — making it invisible to any offer
    acceptance-rate or compensation analytics.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM offers o
          LEFT JOIN applications a ON o.application_id = a.id
         WHERE a.id IS NULL
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} offer row(s) reference an application_id that does not exist "
        f"in the applications table."
    )


@pytest.mark.info
def test_offers_have_pipeline_stage(db_cursor):
    """
    Offers should have a corresponding 'offer' stage row in pipeline_stages.

    This is not a hard foreign-key constraint but a business-rule invariant:
    an offer should only be created after the candidate entered the offer stage
    of the hiring pipeline. A small number of violations (~5) are deliberately
    injected by generate_offers.py (orphan_offer_rate) to simulate an ATS data
    sync failure. Counts in the range 1-19 are treated as expected (XFAIL).
    A count of 20 or more indicates something beyond the injected defects and
    is surfaced as a hard failure.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM offers o
         WHERE NOT EXISTS (
               SELECT 1
                 FROM pipeline_stages ps
                WHERE ps.application_id = o.application_id
                  AND ps.stage_name = 'offer'
         )
    """)
    count = db_cursor.fetchone()[0]

    if 0 < count < 20:
        pytest.xfail(
            f"Deliberate orphan offer injection — found {count} offer(s) with no "
            f"matching 'offer' pipeline stage. Expected ~5 from orphan_offer_rate."
        )

    assert count == 0, (
        f"Unexpected escalation: {count} offer(s) have no matching 'offer' pipeline "
        f"stage. The injected orphan_offer_rate produces ~5; a count this high "
        f"suggests a generation or load error."
    )


@pytest.mark.critical
def test_referral_applications_have_referrer_name(db_cursor):
    """
    Every application with source='employee_referral' must have a non-NULL
    referral_employee_name.

    A referral without a referrer name breaks the ability to calculate
    employee referral conversion rates and identify top referrers — two
    metrics that directly inform recruiting incentive programs.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          from applications
         WHERE source = 'employee_referral'
           AND referral_employee_name IS NULL
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} employee referral application(s) are missing a "
        f"referral_employee_name."
    )


@pytest.mark.critical
def test_non_referral_applications_have_no_referrer_name(db_cursor):
    """
    Applications whose source is not 'employee_referral' must have a NULL
    referral_employee_name.

    A non-referral application with a referrer name populated is a data entry
    error that would inflate referral counts and incorrectly credit an employee
    for a hire they did not refer.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM applications
         WHERE source != 'employee_referral'
           AND referral_employee_name IS NOT NULL
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} non-referral application(s) have a referral_employee_name "
        f"populated despite source != 'employee_referral'."
    )
