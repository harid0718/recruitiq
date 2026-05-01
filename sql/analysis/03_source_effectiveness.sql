-- =============================================================================
-- File   : sql/analysis/03_source_effectiveness.sql
-- Purpose: Sourcing channel effectiveness analysis
--
-- Business question:
--   "Which sourcing channels (LinkedIn, employee referral, job boards, etc.)
--    produce the most hires per application submitted — and which channels
--    produce candidates who progress furthest through the pipeline?"
--
-- Why it matters:
--   Recruiting budgets are finite. Employee referral bonuses, staffing agency
--   contracts, LinkedIn Recruiter seats, and job board subscriptions all carry
--   material cost. Conversion rate and stage-progression data let recruiting
--   ops and finance compare cost-per-hire across channels and reallocate spend
--   toward channels with the best ROI. A channel with 2x the conversion rate
--   of another is worth 2x the investment at equivalent cost — and more if it
--   also fills roles faster.
--
-- Intended consumers:
--   - Recruiting leadership (channel strategy)
--   - Talent Acquisition Operations (spend reallocation, vendor negotiations)
--   - Finance (sourcing budget approval and reconciliation)
-- =============================================================================


-- =============================================================================
-- QUERY 1: Source-to-hire conversion rates
--
-- Business question:
--   Which sourcing channels produce the highest fraction of hires per
--   application, and which channels have the strongest offer acceptance rates?
--
-- Use case:
--   Headline channel comparison for recruiting leadership. Pair with cost-per-
--   source data (not in this database) to compute cost-per-hire by channel.
-- =============================================================================

SELECT
    a.source,
    COUNT(*)                                                              AS total_applications,
    COUNT(CASE WHEN a.status = 'hired' THEN 1 END)                       AS total_hires,
    ROUND(
        COUNT(CASE WHEN a.status = 'hired' THEN 1 END) * 100.0
        / NULLIF(COUNT(*), 0),
        1
    )                                                                     AS hire_rate_pct,
    COUNT(DISTINCT o.id)                                                  AS total_offers,
    COUNT(DISTINCT CASE WHEN o.status = 'accepted' THEN o.id END)        AS accepted_offers,
    ROUND(
        COUNT(DISTINCT CASE WHEN o.status = 'accepted' THEN o.id END) * 100.0
        / NULLIF(COUNT(DISTINCT o.id), 0),
        1
    )                                                                     AS offer_acceptance_rate_pct
  FROM applications a
  LEFT JOIN offers  o ON o.application_id = a.id
 GROUP BY a.source
 ORDER BY hire_rate_pct DESC;


-- =============================================================================
-- QUERY 2: Source effectiveness by department
--
-- Business question:
--   Does channel effectiveness vary by department? Some teams may rely heavily
--   on referrals while others fill primarily through job boards.
--
-- Use case:
--   Helps recruiting business partners tailor sourcing strategy per department
--   rather than applying a one-size-fits-all channel mix. Filtered to
--   ≥20 applications per (department, source) to suppress noisy small cells.
-- =============================================================================

SELECT
    jr.department,
    a.source,
    COUNT(*)                                                              AS total_applications,
    COUNT(CASE WHEN a.status = 'hired' THEN 1 END)                       AS total_hires,
    ROUND(
        COUNT(CASE WHEN a.status = 'hired' THEN 1 END) * 100.0
        / NULLIF(COUNT(*), 0),
        1
    )                                                                     AS hire_rate_pct
  FROM applications     a
  JOIN job_requisitions jr ON jr.id = a.requisition_id
 WHERE jr.department IS NOT NULL
 GROUP BY jr.department, a.source
HAVING COUNT(*) >= 20
 ORDER BY jr.department, hire_rate_pct DESC;


-- =============================================================================
-- QUERY 3: Time-to-hire by source
--
-- Business question:
--   Which channels produce hires fastest? A channel with a lower conversion
--   rate but shorter time-to-hire may still be preferable when roles are
--   urgent (e.g., backfills, headcount freezes with hard deadlines).
--
-- Use case:
--   Informs SLA conversations: if employee referrals fill roles in 25 days
--   vs. agencies at 55 days, the referral bonus program has both quality and
--   speed advantages that justify investment.
-- =============================================================================

WITH hired_ttf AS (
    SELECT
        a.source,
        TIMESTAMPDIFF(DAY, jr.opened_at,  a.applied_at)            AS days_to_hire
      FROM applications     a
      JOIN job_requisitions jr ON jr.id = a.requisition_id
     WHERE a.status = 'hired'
       AND jr.opened_at IS NOT NULL
),
ranked AS (
    SELECT
        source,
        days_to_hire,
        COUNT(*) OVER (PARTITION BY source)                         AS n,
        ROW_NUMBER() OVER (
            PARTITION BY source
            ORDER BY days_to_hire
        )                                                           AS rn
      FROM hired_ttf
)
SELECT
    source,
    n                                                               AS total_hires,
    MAX(CASE WHEN rn = FLOOR(n * 0.25) THEN days_to_hire END)     AS p25_days,
    MAX(CASE WHEN rn = FLOOR(n * 0.50) THEN days_to_hire END)     AS median_days,
    MAX(CASE WHEN rn = FLOOR(n * 0.75) THEN days_to_hire END)     AS p75_days,
    ROUND(AVG(days_to_hire), 1)                                    AS avg_days
  FROM ranked
 GROUP BY source, n
 ORDER BY median_days ASC;


-- =============================================================================
-- QUERY 4: Source quality by stage progression
--
-- Business question:
--   Does a high-volume source also produce high-quality candidates who advance
--   through the funnel? A channel that floods the pipeline but screens poorly
--   creates recruiter workload without proportional output.
--
-- Use case:
--   Distinguish "quantity" channels (high application volume, low stage
--   progression) from "quality" channels (lower volume, candidates advance
--   much further). Pair with Query 1 conversion rates to build a 2x2 matrix.
-- =============================================================================

SELECT
    a.source,
    COUNT(*)                                                              AS total_applications,
    ROUND(
        COUNT(DISTINCT CASE WHEN ps_screen.application_id IS NOT NULL
                            THEN a.id END) * 100.0
        / NULLIF(COUNT(*), 0),
        1
    )                                                                     AS pct_reach_recruiter_screen,
    ROUND(
        COUNT(DISTINCT CASE WHEN ps_panel.application_id IS NOT NULL
                            THEN a.id END) * 100.0
        / NULLIF(COUNT(*), 0),
        1
    )                                                                     AS pct_reach_panel_interview,
    ROUND(
        COUNT(DISTINCT CASE WHEN ps_offer.application_id IS NOT NULL
                            THEN a.id END) * 100.0
        / NULLIF(COUNT(*), 0),
        1
    )                                                                     AS pct_reach_offer,
    ROUND(
        COUNT(CASE WHEN a.status = 'hired' THEN 1 END) * 100.0
        / NULLIF(COUNT(*), 0),
        1
    )                                                                     AS pct_hired
  FROM applications a
  LEFT JOIN pipeline_stages ps_screen
         ON ps_screen.application_id = a.id
        AND ps_screen.stage_name     = 'recruiter_screen'
  LEFT JOIN pipeline_stages ps_panel
         ON ps_panel.application_id  = a.id
        AND ps_panel.stage_name      = 'panel_interview'
  LEFT JOIN pipeline_stages ps_offer
         ON ps_offer.application_id  = a.id
        AND ps_offer.stage_name      = 'offer'
 GROUP BY a.source
 ORDER BY pct_hired DESC;


-- =============================================================================
-- QUERY 5: Top employee referrers
--
-- Business question:
--   Which employees are the most productive referral sources, measured by
--   hires generated rather than just referrals submitted?
--
-- Use case:
--   Identifies employees who should receive recognition and potentially higher
--   referral bonuses as top-of-funnel contributors. Also surfaces employees
--   who refer many people but produce few hires — useful for coaching on
--   referral quality. Filtered to referrers with ≥3 referrals; limited to
--   top 20 by hires.
-- =============================================================================

SELECT
    a.referral_employee_name                                              AS referrer,
    COUNT(*)                                                              AS total_referrals,
    COUNT(CASE WHEN a.status = 'hired' THEN 1 END)                       AS hires,
    ROUND(
        COUNT(CASE WHEN a.status = 'hired' THEN 1 END) * 100.0
        / NULLIF(COUNT(*), 0),
        1
    )                                                                     AS hire_rate_pct
  FROM applications a
 WHERE a.source                  = 'employee_referral'
   AND a.referral_employee_name IS NOT NULL
 GROUP BY a.referral_employee_name
HAVING COUNT(*) >= 3
 ORDER BY hires DESC, total_referrals DESC
 LIMIT 20;
