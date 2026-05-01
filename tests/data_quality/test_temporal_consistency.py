"""
Temporal consistency tests for the RecruitIQ database.

These tests verify that timestamps across tables obey the logical ordering
constraints of a hiring pipeline: a candidate cannot apply before a requisition
opens, a pipeline stage cannot be entered before the application was submitted,
stages must proceed in chronological order, and offer dates must follow their
pipeline prerequisites.

Tests marked `critical` assert a strict zero-violation invariant — a failure
indicates a load or generation error. Tests marked `warning` or `info` tolerate
a small number of deliberately injected defects (see DATA_QUALITY_RATES in
config.py) and use `pytest.xfail` for counts within the expected injection range.
"""

import pytest


@pytest.mark.critical
def test_application_after_requisition_opened(db_cursor):
    """
    Every application's applied_at must be >= the requisition's opened_at.

    An application dated before its requisition existed is temporally impossible
    and indicates either a bad data load or a clock skew issue. Any such row
    would distort time-to-fill calculations by inflating the apparent pipeline
    duration.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM applications a
          JOIN job_requisitions jr ON a.requisition_id = jr.id
         WHERE jr.opened_at IS NOT NULL
           AND a.applied_at < jr.opened_at
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} application(s) have an applied_at earlier than their "
        f"requisition's opened_at."
    )


@pytest.mark.info
def test_pipeline_stage_after_application(db_cursor):
    """
    Every pipeline_stages.entered_at must be >= the parent application's applied_at.

    The invalid_interview_date_rate defect in generate_pipeline_stages.py
    deliberately back-dates a small number of stage entries to before the
    application date (~0.5% of stages, roughly 550 rows). Counts in the range
    1-1000 are treated as expected (XFAIL). A count of 1000 or more suggests
    something beyond the injected defects.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM pipeline_stages ps
          JOIN applications a ON ps.application_id = a.id
         WHERE ps.entered_at < a.applied_at
    """)
    count = db_cursor.fetchone()[0]

    if 0 < count < 1000:
        pytest.xfail(
            f"Deliberate invalid-date injection — found {count} stage(s) entered "
            f"before their application's applied_at. Expected ~550 from "
            f"invalid_interview_date_rate."
        )

    assert count == 0, (
        f"Unexpected escalation: {count} pipeline stage(s) have an entered_at "
        f"earlier than their application's applied_at. The injected defect rate "
        f"produces ~550; a count this high suggests a generation or load error."
    )


@pytest.mark.info
def test_pipeline_stage_chronological_within_application(db_cursor):
    """
    Within each application, pipeline stages must be ordered chronologically
    by entered_at — i.e., stage_order must match the rank of entered_at.

    The out_of_order_stages_rate defect in generate_pipeline_stages.py
    deliberately shuffles stage_order for ~1% of applications (~363 apps).
    Counts in the range 1-1000 are treated as expected (XFAIL). A count of
    1000 or more indicates something beyond the injected defects.
    """
    db_cursor.execute("""
        WITH ranked AS (
            SELECT application_id,
                   stage_order,
                   ROW_NUMBER() OVER (
                       PARTITION BY application_id
                       ORDER BY entered_at
                   ) AS chrono_rank
              FROM pipeline_stages
        )
        SELECT COUNT(DISTINCT application_id)
          FROM ranked
         WHERE stage_order != chrono_rank
    """)
    count = db_cursor.fetchone()[0]

    if 0 < count < 1000:
        pytest.xfail(
            f"Deliberate out-of-order stage injection — found {count} application(s) "
            f"with stage_order mismatched to chronological entered_at rank. Expected "
            f"~363 from out_of_order_stages_rate."
        )

    assert count == 0, (
        f"Unexpected escalation: {count} application(s) have pipeline stages whose "
        f"stage_order does not match the chronological order of entered_at. The "
        f"injected defect rate produces ~363; a count this high suggests a generation "
        f"or load error."
    )


@pytest.mark.critical
def test_pipeline_exited_after_entered(db_cursor):
    """
    For every pipeline stage with a non-NULL exited_at, exited_at must be
    >= entered_at.

    An exit timestamp earlier than the entry timestamp is physically impossible
    and would produce negative duration values in funnel time-in-stage metrics.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM pipeline_stages
         WHERE exited_at IS NOT NULL
           AND exited_at < entered_at
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} pipeline stage(s) have an exited_at earlier than their entered_at."
    )


@pytest.mark.critical
def test_offer_responded_after_sent(db_cursor):
    """
    For every offer with non-NULL offer_responded_at and offer_sent_at,
    offer_responded_at must be >= offer_sent_at.

    A response timestamp before the offer was sent is impossible and would
    produce negative offer-response-time values in compensation analytics.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM offers
         WHERE offer_responded_at IS NOT NULL
           AND offer_sent_at IS NOT NULL
           AND offer_responded_at < offer_sent_at
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} offer(s) have an offer_responded_at earlier than offer_sent_at."
    )


@pytest.mark.warning
def test_offer_after_pipeline_offer_stage(db_cursor):
    """
    Every offer's offer_sent_at must be >= the entered_at of the matching
    'offer' pipeline stage.

    An offer sent before the candidate entered the offer stage suggests a
    process violation or a data sync gap. This is a warning-level check
    because minor clock skew between the ATS and the offer tool is tolerable,
    but systematic violations indicate a process or integration problem.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM offers o
          JOIN pipeline_stages ps
            ON ps.application_id = o.application_id
           AND ps.stage_name = 'offer'
         WHERE o.offer_sent_at IS NOT NULL
           AND o.offer_sent_at < ps.entered_at
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} offer(s) have an offer_sent_at earlier than the entered_at of "
        f"the corresponding 'offer' pipeline stage."
    )


@pytest.mark.critical
def test_v2_offer_sent_after_v1_responded(db_cursor):
    """
    For every version-2 offer, offer_sent_at must be > the version-1 offer's
    offer_responded_at.

    A second offer cannot logically be sent until the candidate has responded
    to (declined) the first. A violation here indicates a sequencing error in
    the offer generation logic.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM offers v2
          JOIN offers v1
            ON v1.application_id = v2.application_id
           AND v1.offer_version = 1
         WHERE v2.offer_version = 2
           AND v1.offer_responded_at IS NOT NULL
           AND v2.offer_sent_at <= v1.offer_responded_at
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} version-2 offer(s) have an offer_sent_at that is not after "
        f"the version-1 offer's offer_responded_at."
    )


@pytest.mark.critical
def test_no_future_dates(db_cursor):
    """
    No application's applied_at should be in the future relative to NOW().

    A future-dated application cannot represent real activity and indicates
    either a generation error or a clock misconfiguration. Such rows would
    skew time-series reports by adding phantom future volume.
    """
    db_cursor.execute("""
        SELECT COUNT(*)
          FROM applications
         WHERE applied_at > NOW()
    """)
    count = db_cursor.fetchone()[0]
    assert count == 0, (
        f"{count} application(s) have an applied_at date in the future."
    )
