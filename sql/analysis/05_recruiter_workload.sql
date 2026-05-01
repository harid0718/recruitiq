-- =============================================================================
-- File   : sql/analysis/05_recruiter_workload.sql
-- Purpose: Recruiter workload and throughput analysis
--
-- Business question:
--   "How is workload distributed across recruiters — and which recruiters
--    fill roles fastest, maintain the highest fill rates, and achieve the
--    best offer acceptance rates?"
--
-- Why it matters:
--   Recruiter capacity is the binding constraint on hiring throughput. A team
--   where two recruiters carry 60% of open reqs while others are under-loaded
--   will see burnout, slower TTF, and regrettable candidate experience on the
--   overloaded desks. Performance metrics across TTF, fill rate, and offer
--   acceptance rate inform coaching conversations, headcount planning for the
--   recruiting team itself, and recognition decisions.
--
-- Note on schema:
--   This analysis uses the recruiter_name text field on job_requisitions, which
--   is a denormalized string in the synthetic dataset. A production schema would
--   store a recruiter_id FK to an employees or users table to support renames,
--   terminations, and team hierarchy rollups. NULL recruiter_name rows are
--   excluded throughout — these are deliberately injected data quality defects.
--
-- Intended consumers:
--   - Recruiting Operations (capacity planning, queue balancing)
--   - Talent Acquisition leadership (performance reviews, coaching)
-- =============================================================================


-- =============================================================================
-- QUERY 1: Recruiter workload distribution
--
-- Business question:
--   How many requisitions has each recruiter been assigned, and how many are
--   currently open vs. closed?
--
-- Use case:
--   Queue visibility for recruiting operations. Recruiters with a high open
--   count relative to their peers may need relief; recruiters who primarily
--   show filled/cancelled reqs may be available for new assignments.
-- =============================================================================

SELECT
    jr.recruiter_name,
    COUNT(*)                                                              AS total_reqs,
    COUNT(CASE WHEN jr.status = 'open'      THEN 1 END)                  AS open_reqs,
    COUNT(CASE WHEN jr.status = 'on_hold'   THEN 1 END)                  AS on_hold_reqs,
    COUNT(CASE WHEN jr.status = 'filled'    THEN 1 END)                  AS filled_reqs,
    COUNT(CASE WHEN jr.status = 'cancelled' THEN 1 END)                  AS cancelled_reqs
  FROM job_requisitions jr
 WHERE jr.recruiter_name IS NOT NULL
 GROUP BY jr.recruiter_name
 ORDER BY total_reqs DESC
 LIMIT 20;


-- =============================================================================
-- QUERY 2: Recruiter fill rate
--
-- Business question:
--   Of the requisitions a recruiter closed (filled or cancelled), what
--   fraction resulted in a hire?
--
-- Use case:
--   Fill rate distinguishes recruiters who successfully close roles from those
--   whose reqs frequently get cancelled. A low fill rate may indicate that a
--   recruiter's reqs are often cancelled for business reasons (not their fault)
--   or that roles stall without resolution. Filtered to ≥5 closed reqs to
--   avoid noise from recruiters with minimal history.
-- =============================================================================

SELECT
    jr.recruiter_name,
    COUNT(CASE WHEN jr.status IN ('filled', 'cancelled') THEN 1 END)     AS closed_reqs,
    COUNT(CASE WHEN jr.status = 'filled'                 THEN 1 END)     AS filled_reqs,
    COUNT(CASE WHEN jr.status = 'cancelled'              THEN 1 END)     AS cancelled_reqs,
    ROUND(
        COUNT(CASE WHEN jr.status = 'filled' THEN 1 END) * 100.0
        / NULLIF(COUNT(CASE WHEN jr.status IN ('filled', 'cancelled') THEN 1 END), 0),
        1
    )                                                                     AS fill_rate_pct
  FROM job_requisitions jr
 WHERE jr.recruiter_name IS NOT NULL
 GROUP BY jr.recruiter_name
HAVING COUNT(CASE WHEN jr.status IN ('filled', 'cancelled') THEN 1 END) >= 5
 ORDER BY fill_rate_pct DESC;


-- =============================================================================
-- QUERY 3: Recruiter time-to-fill performance
--
-- Business question:
--   Which recruiters fill roles fastest, and which are consistently slower
--   than the org median?
--
-- Use case:
--   Paired with fill rate to form a 2x2 performance view. A recruiter with
--   both high fill rate and low TTF is performing well across both dimensions.
--   A recruiter with low TTF but also low fill rate may be rushing to closure
--   at the cost of quality. Filtered to ≥5 filled reqs for stable percentiles.
-- =============================================================================

WITH recruiter_ttf AS (
    SELECT
        jr.recruiter_name,
        TIMESTAMPDIFF(DAY, jr.opened_at, a.applied_at)                   AS days_to_fill
      FROM job_requisitions jr
      JOIN applications      a  ON a.requisition_id = jr.id
                               AND a.status = 'hired'
     WHERE jr.status         = 'filled'
       AND jr.opened_at      IS NOT NULL
       AND jr.recruiter_name IS NOT NULL
),
recruiter_counts AS (
    SELECT recruiter_name, COUNT(*) AS n
      FROM recruiter_ttf
     GROUP BY recruiter_name
    HAVING COUNT(*) >= 5
),
ranked AS (
    SELECT
        rt.recruiter_name,
        rt.days_to_fill,
        rc.n,
        ROW_NUMBER() OVER (
            PARTITION BY rt.recruiter_name
            ORDER BY rt.days_to_fill
        )                                                                  AS rn
      FROM recruiter_ttf    rt
      JOIN recruiter_counts rc ON rc.recruiter_name = rt.recruiter_name
)
SELECT
    recruiter_name,
    n                                                                      AS filled_reqs,
    ROUND(AVG(days_to_fill), 1)                                           AS avg_days_to_fill,
    MAX(CASE WHEN rn = FLOOR(n * 0.25) THEN days_to_fill END)            AS p25_days,
    MAX(CASE WHEN rn = FLOOR(n * 0.50) THEN days_to_fill END)            AS median_days,
    MAX(CASE WHEN rn = FLOOR(n * 0.75) THEN days_to_fill END)            AS p75_days
  FROM ranked
 GROUP BY recruiter_name, n
 ORDER BY median_days ASC;


-- =============================================================================
-- QUERY 4: Recruiter offer acceptance rate
--
-- Business question:
--   Which recruiters achieve the highest offer acceptance rates on the roles
--   they manage?
--
-- Use case:
--   Offer acceptance often reflects how well the recruiter managed candidate
--   expectations around compensation, role scope, and timeline throughout the
--   process. A recruiter with a consistently low acceptance rate may benefit
--   from coaching on expectation-setting or salary conversation techniques.
--   Filtered to ≥10 v1 offers for statistical relevance.
-- =============================================================================

SELECT
    jr.recruiter_name,
    COUNT(*)                                                              AS total_offers,
    COUNT(CASE WHEN o.status = 'accepted'  THEN 1 END)                   AS accepted,
    COUNT(CASE WHEN o.status = 'declined'  THEN 1 END)                   AS declined,
    ROUND(
        COUNT(CASE WHEN o.status = 'accepted' THEN 1 END) * 100.0
        / NULLIF(COUNT(*), 0),
        1
    )                                                                     AS acceptance_rate_pct
  FROM offers          o
  JOIN applications    a  ON a.id  = o.application_id
  JOIN job_requisitions jr ON jr.id = a.requisition_id
 WHERE o.offer_version    = 1
   AND jr.recruiter_name IS NOT NULL
 GROUP BY jr.recruiter_name
HAVING COUNT(*) >= 10
 ORDER BY acceptance_rate_pct DESC;


-- =============================================================================
-- QUERY 5: Recruiter workload by department
--
-- Business question:
--   Which recruiters are covering which departments, and is coverage well
--   distributed or concentrated?
--
-- Use case:
--   Recruiting ops uses this to ensure that specialized knowledge (e.g., a
--   recruiter who understands engineering roles) is matched to the right
--   departments. It also reveals single points of failure: if one recruiter
--   owns 90% of Sales reqs and goes on leave, pipeline stalls.
-- =============================================================================

SELECT
    jr.recruiter_name,
    jr.department,
    COUNT(*)                                                              AS total_reqs,
    COUNT(CASE WHEN jr.status = 'open'   THEN 1 END)                     AS open_reqs,
    COUNT(CASE WHEN jr.status = 'filled' THEN 1 END)                     AS filled_reqs
  FROM job_requisitions jr
 WHERE jr.recruiter_name IS NOT NULL
   AND jr.department     IS NOT NULL
 GROUP BY jr.recruiter_name, jr.department
 ORDER BY total_reqs DESC
 LIMIT 30;
