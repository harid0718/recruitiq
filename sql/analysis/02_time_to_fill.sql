-- =============================================================================
-- File   : sql/analysis/02_time_to_fill.sql
-- Purpose: Time-to-fill (TTF) analysis across requisitions
--
-- Business question:
--   "How long does it take from a requisition opening to a candidate being
--    hired — and which departments, seniority levels, or time periods are
--    consistently slow?"
--
-- Why it matters:
--   TTF is the #1 SLA metric for recruiting teams. Every day an approved role
--   stays open represents lost productivity, delayed project delivery, and
--   additional recruiting cost. Executive teams typically set TTF targets
--   (e.g., 45 days for IC roles, 60 days for management) and recruiting
--   leadership is held accountable to them.
--
-- Key definitions:
--   TTF = TIMESTAMPDIFF(DAY, job_requisitions.opened_at, applications.applied_at)
--         for the application that resulted in a hire on that requisition.
--
--   Some organizations measure TTF to offer_accepted_at (when the candidate
--   signed) rather than applied_at (when they were hired in the ATS). This
--   file uses applied_at as the hire anchor because it is the most consistently
--   populated field. Offer-based TTF would add the offer-response lag on top
--   and would be slightly longer on average.
--
-- Intended consumers:
--   - Recruiting leadership (SLA compliance tracking)
--   - Hiring managers (benchmark your team's fill time against the org)
--   - HR Business Partners (workforce planning input)
-- =============================================================================


-- =============================================================================
-- QUERY 1: Overall TTF headline metrics
--
-- Business question:
--   Across all filled requisitions, what is the typical time to fill?
--
-- Use case:
--   Single headline row for executive dashboards. Compare median against
--   internal SLA targets (e.g., "target: 45 days") to report compliance.
-- =============================================================================

WITH ttf_base AS (
    SELECT
        jr.id                                                       AS req_id,
        TIMESTAMPDIFF(DAY, jr.opened_at, a.applied_at)             AS days_to_fill
      FROM job_requisitions jr
      JOIN applications      a  ON a.requisition_id = jr.id
                               AND a.status = 'hired'
     WHERE jr.status = 'filled'
       AND jr.opened_at IS NOT NULL
),
ranked AS (
    SELECT
        days_to_fill,
        COUNT(*) OVER ()                                            AS n,
        ROW_NUMBER() OVER (ORDER BY days_to_fill)                  AS rn
      FROM ttf_base
)
SELECT
    COUNT(*)                                                        AS filled_requisitions,
    ROUND(AVG(days_to_fill), 1)                                    AS avg_days_to_fill,
    MAX(CASE WHEN rn = FLOOR(n * 0.25) THEN days_to_fill END)     AS p25_days,
    MAX(CASE WHEN rn = FLOOR(n * 0.50) THEN days_to_fill END)     AS median_days,
    MAX(CASE WHEN rn = FLOOR(n * 0.75) THEN days_to_fill END)     AS p75_days
  FROM ranked;


-- =============================================================================
-- QUERY 2: TTF by department
--
-- Business question:
--   Which departments fill roles fastest, and which are the slowest?
--
-- Use case:
--   Surfaced to recruiting business partners for each department. Sorted
--   slowest-first so the problem areas appear at the top. Filtered to ≥10
--   filled reqs to avoid misleading percentiles from small samples.
-- =============================================================================

WITH ttf_dept AS (
    SELECT
        jr.department,
        TIMESTAMPDIFF(DAY, jr.opened_at, a.applied_at)             AS days_to_fill
      FROM job_requisitions jr
      JOIN applications      a  ON a.requisition_id = jr.id
                               AND a.status = 'hired'
     WHERE jr.status    = 'filled'
       AND jr.opened_at IS NOT NULL
       AND jr.department IS NOT NULL
),
dept_counts AS (
    SELECT department, COUNT(*) AS n
      FROM ttf_dept
     GROUP BY department
    HAVING COUNT(*) >= 10
),
ranked AS (
    SELECT
        td.department,
        td.days_to_fill,
        dc.n,
        ROW_NUMBER() OVER (PARTITION BY td.department ORDER BY td.days_to_fill) AS rn
      FROM ttf_dept td
      JOIN dept_counts dc ON dc.department = td.department
)
SELECT
    department,
    n                                                               AS filled_requisitions,
    ROUND(AVG(days_to_fill), 1)                                    AS avg_days_to_fill,
    MAX(CASE WHEN rn = FLOOR(n * 0.25) THEN days_to_fill END)     AS p25_days,
    MAX(CASE WHEN rn = FLOOR(n * 0.50) THEN days_to_fill END)     AS median_days,
    MAX(CASE WHEN rn = FLOOR(n * 0.75) THEN days_to_fill END)     AS p75_days
  FROM ranked
 GROUP BY department, n
 ORDER BY median_days DESC;


-- =============================================================================
-- QUERY 3: TTF by seniority level
--
-- Business question:
--   Do senior and executive roles take meaningfully longer to fill than
--   individual contributor and intern roles?
--
-- Use case:
--   Informs headcount planning lead times. If executive roles take 90+ days,
--   workforce planning needs to open those reqs further in advance to meet
--   start-date targets.
-- =============================================================================

WITH ttf_seniority AS (
    SELECT
        jr.seniority_level,
        TIMESTAMPDIFF(DAY, jr.opened_at, a.applied_at)             AS days_to_fill
      FROM job_requisitions jr
      JOIN applications      a  ON a.requisition_id = jr.id
                               AND a.status = 'hired'
     WHERE jr.status         = 'filled'
       AND jr.opened_at      IS NOT NULL
       AND jr.seniority_level IS NOT NULL
),
ranked AS (
    SELECT
        seniority_level,
        days_to_fill,
        COUNT(*) OVER (PARTITION BY seniority_level)               AS n,
        ROW_NUMBER() OVER (
            PARTITION BY seniority_level
            ORDER BY days_to_fill
        )                                                           AS rn
      FROM ttf_seniority
)
SELECT
    FIELD(
        seniority_level,
        'intern', 'entry', 'mid', 'senior', 'staff',
        'manager', 'director', 'vp', 'executive'
    )                                                               AS seniority_order,
    seniority_level,
    n                                                               AS filled_requisitions,
    ROUND(AVG(days_to_fill), 1)                                    AS avg_days_to_fill,
    MAX(CASE WHEN rn = FLOOR(n * 0.25) THEN days_to_fill END)     AS p25_days,
    MAX(CASE WHEN rn = FLOOR(n * 0.50) THEN days_to_fill END)     AS median_days,
    MAX(CASE WHEN rn = FLOOR(n * 0.75) THEN days_to_fill END)     AS p75_days
  FROM ranked
 GROUP BY seniority_level, n
 ORDER BY seniority_order;


-- =============================================================================
-- QUERY 4: TTF trend by quarter
--
-- Business question:
--   Is the recruiting team filling roles faster or slower over time? Are there
--   seasonal patterns (e.g., slower fills in Q4 due to holiday hiring freezes)?
--
-- Use case:
--   Time-series chart for recruiting QBRs. A rising median signals process
--   slowdown or increased role complexity; a falling median signals improved
--   efficiency or easing of hiring difficulty.
-- =============================================================================

WITH ttf_quarterly AS (
    SELECT
        YEAR(jr.opened_at)                                         AS yr,
        QUARTER(jr.opened_at)                                      AS qtr,
        TIMESTAMPDIFF(DAY, jr.opened_at, a.applied_at)            AS days_to_fill
      FROM job_requisitions jr
      JOIN applications      a  ON a.requisition_id = jr.id
                               AND a.status = 'hired'
     WHERE jr.status    = 'filled'
       AND jr.opened_at IS NOT NULL
),
ranked AS (
    SELECT
        yr,
        qtr,
        days_to_fill,
        COUNT(*) OVER (PARTITION BY yr, qtr)                       AS n,
        ROW_NUMBER() OVER (
            PARTITION BY yr, qtr
            ORDER BY days_to_fill
        )                                                           AS rn
      FROM ttf_quarterly
)
SELECT
    yr                                                              AS year,
    qtr                                                             AS quarter,
    CONCAT(yr, '-Q', qtr)                                          AS period,
    n                                                               AS filled_requisitions,
    ROUND(AVG(days_to_fill), 1)                                    AS avg_days_to_fill,
    MAX(CASE WHEN rn = FLOOR(n * 0.50) THEN days_to_fill END)     AS median_days
  FROM ranked
 GROUP BY yr, qtr, n
 ORDER BY yr, qtr;


-- =============================================================================
-- QUERY 5: Slowest 10 filled requisitions
--
-- Business question:
--   Which specific requisitions took the longest to fill, and what do they
--   have in common (department, seniority, location)?
--
-- Use case:
--   Ad-hoc investigation starting point. Recruiters and hiring managers can
--   use req_code to pull the full application history from the ATS and
--   identify root causes (e.g., role was paused, interview panel unavailable,
--   compensation band was too narrow).
-- =============================================================================

SELECT
    jr.req_code,
    jr.title,
    jr.department,
    jr.seniority_level,
    jr.location,
    DATE(jr.opened_at)                                             AS opened_at,
    DATE(a.applied_at)                                             AS hired_at,
    TIMESTAMPDIFF(DAY, jr.opened_at, a.applied_at)                AS days_to_fill
  FROM job_requisitions jr
  JOIN applications      a  ON a.requisition_id = jr.id
                           AND a.status = 'hired'
 WHERE jr.status    = 'filled'
   AND jr.opened_at IS NOT NULL
 ORDER BY days_to_fill DESC
 LIMIT 10;
