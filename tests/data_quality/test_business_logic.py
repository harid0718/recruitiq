"""
Business logic tests for the RecruitIQ database.

These tests validate cross-table invariants that cannot be enforced by foreign
keys or column constraints alone. They check that the state recorded in one
table is consistent with the state recorded in related tables — e.g., that an
application marked 'hired' actually progressed to the 'hired' pipeline stage,
or that a filled requisition has at least one corresponding hired application.

Tests marked `critical` assert invariants whose violation would produce silent
incorrect results in core recruiting metrics (funnel conversion, time-to-fill,
offer acceptance rate). Tests marked `warning` flag inconsistencies that degrade
analytics quality but do not break queries. Tests marked `info` tolerate
deliberately injected defects from the synthetic data generators.
"""

import pytest


@pytest.mark.info
def test_v2_accepted_matches_application_status(db_cursor):
    """
    A v2 offer with status='accepted' should have a parent application whose
    status is 'hired'. Rows where the offer says accepted but the application
    says 'rejected' or 'withdrawn' represent an ATS sync failure.

    generate_offers.py deliberately injects this discrepancy: when a v2 offer
    is accepted, the application's status is intentionally left as 'rejected'
    or 'withdrawn' to simulate a case where the offer tool and the ATS fell out
    of sync. This is detectable by joining offers back to applications and
    comparing statuses. Counts in the range 1-200 are treated as expected (XFAIL).
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM offers o
          JOIN applications a ON o.application_id = a.id
         WHERE o.offer_version = 2
           AND o.status = 'accepted'
           AND a.status IN ('rejected', 'withdrawn')
    """)
    count = db_cursor.fetchone()[0]

    if 0 < count < 200:
        pytest.xfail(
            f"Deliberate ATS sync discrepancy — found {count} v2 accepted offer(s) "
            f"whose application.status is still 'rejected' or 'withdrawn'. This is "
            f"an intentional data quality defect injected by generate_offers.py to "
            f"simulate an offer-tool / ATS sync failure."
        )

    assert count == 0, (
        f"Unexpected escalation: {count} v2 accepted offer(s) have an application "
        f"status of 'rejected' or 'withdrawn'. The intentional injection produces "
        f"~49; a count of 200 or more suggests a generation or load error."
    )


@pytest.mark.critical
def test_hired_application_has_hired_pipeline_stage(db_cursor):
    """
    Every application with status='hired' must have a pipeline_stages row with
    stage_name='hired'.

    The 'hired' stage is the terminal marker written at the end of every
    completed pipeline. An application flagged as hired without that stage row
    would be excluded from any stage-completion query, causing funnel reports
    to undercount successful hires and time-in-pipeline calculations to be
    uncalculable for those candidates.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM applications a
         WHERE a.status = 'hired'
           AND NOT EXISTS (
               SELECT 1
                 FROM pipeline_stages ps
                WHERE ps.application_id = a.id
                  AND ps.stage_name = 'hired'
           )
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} application(s) with status='hired' have no 'hired' pipeline stage row."
    )


@pytest.mark.warning
def test_rejected_application_has_rejected_outcome(db_cursor):
    """
    Every application with status='rejected' should have at least one
    pipeline_stages row with outcome='rejected'.

    A rejection recorded on the application but absent from the pipeline means
    the stage at which the candidate was rejected is unknown. This prevents
    funnel drop-off analysis from attributing rejections to the correct hiring
    stage, understating rejection rates at the actual decision point.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM applications a
         WHERE a.status = 'rejected'
           AND NOT EXISTS (
               SELECT 1
                 FROM pipeline_stages ps
                WHERE ps.application_id = a.id
                  AND ps.outcome = 'rejected'
           )
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} application(s) with status='rejected' have no pipeline stage row "
        f"with outcome='rejected'."
    )


@pytest.mark.warning
def test_withdrawn_application_has_withdrew_outcome(db_cursor):
    """
    Every application with status='withdrawn' should have at least one
    pipeline_stages row with outcome='withdrew'.

    A withdrawal on the application without a matching pipeline outcome makes
    it impossible to identify at which stage candidates self-select out. Stage-
    level withdrawal rates — a key candidate-experience signal — cannot be
    computed for applications missing this outcome.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM applications a
         WHERE a.status = 'withdrawn'
           AND NOT EXISTS (
               SELECT 1
                 FROM pipeline_stages ps
                WHERE ps.application_id = a.id
                  AND ps.outcome = 'withdrew'
           )
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} application(s) with status='withdrawn' have no pipeline stage row "
        f"with outcome='withdrew'."
    )


@pytest.mark.critical
def test_active_applications_have_no_terminal_outcome(db_cursor):
    """
    No pipeline stage belonging to an active application should carry a terminal
    outcome ('hired', 'rejected', or 'withdrew').

    An active application is by definition still in progress. A terminal outcome
    on any of its stages signals that the pipeline row and the application status
    are out of sync — likely a failed status update. If left uncorrected, the
    application would appear in both open-pipeline counts and closed-outcome
    reports simultaneously, inflating both.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM applications a
         WHERE a.status = 'active'
           AND EXISTS (
               SELECT 1
                 FROM pipeline_stages ps
                WHERE ps.application_id = a.id
                  AND ps.outcome IN ('hired', 'rejected', 'withdrew')
           )
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} active application(s) have at least one pipeline stage with a "
        f"terminal outcome ('hired', 'rejected', or 'withdrew')."
    )


@pytest.mark.warning
def test_offer_salary_within_requisition_band(db_cursor):
    """
    Every v1 offer's base_salary should fall within 10% of the requisition's
    approved salary band (salary_range_min * 0.9 to salary_range_max * 1.1).

    generate_offers.py intentionally generates stretch offers (5% above max)
    and tight offers (5% below min), both of which fall within the 10%
    tolerance window. A violation here therefore indicates a salary figure that
    is genuinely anomalous — likely a generation error or a data entry mistake
    that would distort compensation-band-compliance reporting.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM offers o
          JOIN applications a  ON a.id = o.application_id
          JOIN job_requisitions jr ON jr.id = a.requisition_id
         WHERE o.offer_version = 1
           AND jr.salary_range_min IS NOT NULL
           AND jr.salary_range_max IS NOT NULL
           AND (
               o.base_salary < jr.salary_range_min * 0.9
            OR o.base_salary > jr.salary_range_max * 1.1
           )
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} v1 offer(s) have a base_salary more than 10% outside the "
        f"requisition's approved salary band."
    )


@pytest.mark.critical
def test_filled_requisitions_have_at_least_one_hire(db_cursor):
    """
    Every requisition with status='filled' must have at least one application
    with status='hired'.

    A filled requisition with no hired application means headcount was closed
    without a corresponding hire record, breaking time-to-fill calculations
    (no anchor hire date) and causing the requisition to inflate open-headcount
    gap counts if it is later reprocessed.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM job_requisitions jr
         WHERE jr.status = 'filled'
           AND NOT EXISTS (
               SELECT 1
                 FROM applications a
                WHERE a.requisition_id = jr.id
                  AND a.status = 'hired'
           )
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} filled requisition(s) have no application with status='hired'."
    )


@pytest.mark.warning
def test_offer_seniority_consistency(db_cursor):
    """
    An offer's base_salary should not fall below the minimum credible floor for
    its seniority level. A salary far below the floor for that level almost
    certainly indicates a data entry or generation error rather than a legitimate
    comp decision, and would distort seniority-level compensation benchmarks.

    Floors used (USD):
      intern: 20K, entry: 40K, mid: 60K, senior: 100K, staff: 130K,
      manager: 110K, director: 150K, vp: 180K, executive: 200K
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM offers o
          JOIN applications a   ON a.id = o.application_id
          JOIN job_requisitions jr ON jr.id = a.requisition_id
         WHERE jr.seniority_level IS NOT NULL
           AND (
               (jr.seniority_level = 'intern'    AND o.base_salary <  20000)
            OR (jr.seniority_level = 'entry'     AND o.base_salary <  40000)
            OR (jr.seniority_level = 'mid'       AND o.base_salary <  60000)
            OR (jr.seniority_level = 'senior'    AND o.base_salary < 100000)
            OR (jr.seniority_level = 'staff'     AND o.base_salary < 130000)
            OR (jr.seniority_level = 'manager'   AND o.base_salary < 110000)
            OR (jr.seniority_level = 'director'  AND o.base_salary < 150000)
            OR (jr.seniority_level = 'vp'        AND o.base_salary < 180000)
            OR (jr.seniority_level = 'executive' AND o.base_salary < 200000)
           )
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} offer(s) have a base_salary below the minimum credible floor "
        f"for their seniority level."
    )
