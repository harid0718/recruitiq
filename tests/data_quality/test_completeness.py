"""
Completeness tests for the RecruitIQ database.

These tests verify that required fields are populated on rows where the
business process demands them. Missing values in these fields either break
downstream reporting (NULL department blocks dept-level funnel metrics),
violate process invariants (a hire must have an accepted offer), or indicate
a data entry gap (a declined offer with no reason cannot be used for
candidate-experience analysis).

Tests marked `critical` assert strict zero-violation invariants. Tests marked
`warning` flag gaps that degrade analytics quality without breaking queries.
Tests marked `info` tolerate deliberately injected defects (see DATA_QUALITY_RATES
in config.py) and use `pytest.xfail` for counts within the expected injection range.
"""

import pytest


@pytest.mark.info
def test_open_requisitions_have_department(db_cursor):
    """
    Every open job_requisition should have a non-NULL department.

    A NULL department on an open requisition silently excludes it from all
    department-level headcount and time-to-fill reports, making those metrics
    calculated on an incomplete population. The missing_department_rate defect
    in generate_job_requisitions.py deliberately sets ~1% of departments to NULL
    (~5 rows). Counts in the range 1-20 are treated as expected (XFAIL).
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM job_requisitions
         WHERE status = 'open'
           AND department IS NULL
    """)
    count = db_cursor.fetchone()[0]

    if 0 < count < 20:
        pytest.xfail(
            f"Deliberate missing-department injection — found {count} open "
            f"requisition(s) with NULL department. Expected ~5 from "
            f"missing_department_rate=0.01. Department null on open reqs blocks "
            f"dept-level reporting."
        )

    assert count == 0, (
        f"{count} open requisition(s) have a NULL department. The injected defect "
        f"rate produces ~5; a count of 20 or more suggests a generation or load error."
    )


@pytest.mark.info
def test_open_requisitions_have_hiring_manager(db_cursor):
    """
    Every open job_requisition should have a non-NULL hiring_manager_name.

    A requisition without a hiring manager cannot be assigned for recruiter
    routing or used in hiring-manager performance dashboards. The
    null_hiring_manager_rate defect in generate_job_requisitions.py deliberately
    nulls ~3% of hiring managers (~15 rows). Counts in the range 1-30 are
    treated as expected (XFAIL).
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM job_requisitions
         WHERE status = 'open'
           AND hiring_manager_name IS NULL
    """)
    count = db_cursor.fetchone()[0]

    if 0 < count < 30:
        pytest.xfail(
            f"Deliberate null-hiring-manager injection — found {count} open "
            f"requisition(s) with NULL hiring_manager_name. Expected ~15 from "
            f"null_hiring_manager_rate=0.03."
        )

    assert count == 0, (
        f"{count} open requisition(s) have a NULL hiring_manager_name. The injected "
        f"defect rate produces ~15; a count of 30 or more suggests a generation or "
        f"load error."
    )


@pytest.mark.critical
def test_filled_requisitions_have_closed_at(db_cursor):
    """
    Every requisition with status='filled' must have a non-NULL closed_at.

    closed_at is the anchor for time-to-fill calculations. A filled requisition
    without a close date makes it impossible to compute how long the role took
    to hire, rendering that requisition invisible to fill-rate SLA tracking.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM job_requisitions
         WHERE status = 'filled'
           AND closed_at IS NULL
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} filled requisition(s) have a NULL closed_at."
    )


@pytest.mark.critical
def test_cancelled_requisitions_have_closed_at(db_cursor):
    """
    Every requisition with status='cancelled' must have a non-NULL closed_at.

    A cancelled requisition without a close date cannot be excluded from
    open-headcount counts or attributed to the correct reporting period for
    budget and attrition analysis.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM job_requisitions
         WHERE status = 'cancelled'
           AND closed_at IS NULL
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} cancelled requisition(s) have a NULL closed_at."
    )


@pytest.mark.critical
def test_open_requisitions_have_no_closed_at(db_cursor):
    """
    Requisitions with status 'open', 'on_hold', or 'draft' must not have a
    closed_at populated.

    A close date on an active requisition signals a data entry error or a
    status rollback that wasn't accompanied by a date clear. Such rows would
    be misclassified as closed in any time-series analysis that uses closed_at
    to bound the active window.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM job_requisitions
         WHERE status IN ('open', 'on_hold', 'draft')
           AND closed_at IS NOT NULL
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} active requisition(s) (open/on_hold/draft) have a non-NULL closed_at."
    )


@pytest.mark.critical
def test_hired_applications_have_offer(db_cursor):
    """
    Every application with status='hired' must have at least one accepted offer.

    A hire without an accepted offer record is a process gap: the candidate
    cleared the pipeline but compensation was never formally tracked. Such rows
    would cause the hire to be excluded from offer-acceptance-rate and
    compensation-band-compliance analytics.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM applications a
         WHERE a.status = 'hired'
           AND NOT EXISTS (
               SELECT 1
                 FROM offers o
                WHERE o.application_id = a.id
                  AND o.status = 'accepted'
           )
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} hired application(s) have no corresponding accepted offer row."
    )


@pytest.mark.critical
def test_accepted_offers_have_response_date(db_cursor):
    """
    Every offer with status='accepted' must have a non-NULL offer_responded_at.

    offer_responded_at is the timestamp used to compute offer-response time and
    to anchor start-date lead-time calculations. An accepted offer without a
    response date makes both metrics uncalculable for that hire.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM offers
         WHERE status = 'accepted'
           AND offer_responded_at IS NULL
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} accepted offer(s) have a NULL offer_responded_at."
    )


@pytest.mark.warning
def test_declined_offers_have_decline_reason(db_cursor):
    """
    Every offer with status='declined' should have a non-NULL decline_reason.

    Decline reason is the primary input for candidate-experience analysis and
    offer-competitiveness benchmarking. A missing reason degrades those reports
    but does not break any query — rows without a reason are simply excluded
    from reason-breakdown charts, silently reducing sample size.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM offers
         WHERE status = 'declined'
           AND decline_reason IS NULL
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} declined offer(s) have a NULL decline_reason."
    )


@pytest.mark.critical
def test_candidate_email_format(db_cursor):
    """
    Every candidate's email must match a basic format: contains '@' followed
    by a domain with at least one '.'.

    An email that fails this check cannot be used for candidate outreach or
    deduplication. Because email is the primary natural key for candidate
    identity matching (separate from the surrogate id), a malformed email
    also undermines duplicate-candidate detection logic.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM candidates
         WHERE email NOT LIKE '%@%.%'
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} candidate(s) have an email that does not match the pattern '%@%.%'."
    )
