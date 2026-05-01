-- =============================================================================
-- File   : sql/analysis/04_offer_analytics.sql
-- Purpose: Offer acceptance, decline, and compensation analysis
--
-- Business question:
--   "What are our offer acceptance rates, why do candidates decline, and how
--    does compensation compare across departments, seniority levels, and
--    locations?"
--
-- Why it matters:
--   Offer outcomes are the final gate between pipeline work and headcount
--   achievement. A decline at the offer stage wastes the entire upstream
--   investment. Decline reasons — especially compensation — are the primary
--   driver of comp band adjustments. Counter-offer data reveals whether salary
--   flexibility actually rescues hires or just delays an inevitable decline.
--   Geographic and seniority compensation patterns inform band design and
--   competitive positioning against market benchmarks.
--
-- Intended consumers:
--   - Compensation team (band setting, competitive analysis)
--   - Recruiting leadership (offer strategy, acceptance rate SLAs)
--   - HR Business Partners (headcount achievement forecasting)
-- =============================================================================


-- =============================================================================
-- QUERY 1: Overall offer outcomes
--
-- Business question:
--   What fraction of first-round offers are accepted, declined, expired, or
--   rescinded?
--
-- Use case:
--   Headline acceptance rate for recruiting QBRs. An acceptance rate below
--   ~80% is a signal that comp bands, role clarity, or candidate experience
--   at the offer stage needs attention.
-- =============================================================================

SELECT
    COUNT(*)                                                              AS total_v1_offers,
    COUNT(CASE WHEN status = 'accepted'  THEN 1 END)                     AS accepted,
    COUNT(CASE WHEN status = 'declined'  THEN 1 END)                     AS declined,
    COUNT(CASE WHEN status = 'expired'   THEN 1 END)                     AS expired,
    COUNT(CASE WHEN status = 'rescinded' THEN 1 END)                     AS rescinded,
    COUNT(CASE WHEN status = 'sent'      THEN 1 END)                     AS pending,
    ROUND(
        COUNT(CASE WHEN status = 'accepted' THEN 1 END) * 100.0
        / NULLIF(COUNT(*), 0),
        1
    )                                                                     AS acceptance_rate_pct,
    ROUND(
        COUNT(CASE WHEN status = 'declined' THEN 1 END) * 100.0
        / NULLIF(COUNT(*), 0),
        1
    )                                                                     AS decline_rate_pct
  FROM offers
 WHERE offer_version = 1;


-- =============================================================================
-- QUERY 2: Decline reason breakdown
--
-- Business question:
--   Of the offers candidates decline, what are the stated reasons — and is
--   compensation the dominant factor?
--
-- Use case:
--   Primary input for comp band review cycles. If "compensation" accounts for
--   >40% of declines, the band is likely below market. "Competing offer" often
--   implies speed issues as much as comp. "Location" may signal a need to
--   expand remote flexibility.
-- =============================================================================

SELECT
    COALESCE(decline_reason, 'not_specified')                            AS decline_reason,
    COUNT(*)                                                              AS decline_count,
    ROUND(
        COUNT(*) * 100.0
        / NULLIF(SUM(COUNT(*)) OVER (), 0),
        1
    )                                                                     AS pct_of_all_declines
  FROM offers
 WHERE status = 'declined'
 GROUP BY decline_reason
 ORDER BY decline_count DESC;


-- =============================================================================
-- QUERY 3: Offer acceptance rate by department
--
-- Business question:
--   Which departments struggle most with offer acceptance? A department with
--   a low acceptance rate may have a comp band below market for its specific
--   talent pool, or a candidate experience issue late in the process.
--
-- Use case:
--   Delivered to department heads and recruiting business partners alongside
--   their TTF and funnel data. Filtered to ≥10 offers to avoid noise.
-- =============================================================================

SELECT
    jr.department,
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
 WHERE o.offer_version = 1
   AND jr.department  IS NOT NULL
 GROUP BY jr.department
HAVING COUNT(*) >= 10
 ORDER BY acceptance_rate_pct DESC;


-- =============================================================================
-- QUERY 4: Compensation by seniority level
--
-- Business question:
--   What does the company actually pay at each seniority level, and how wide
--   is the spread within each band?
--
-- Use case:
--   Compensation team uses this to validate whether approved salary bands
--   match actual offer outcomes. Total comp adds signing bonus and equity to
--   surface the full economic picture, since candidates evaluate total package.
-- =============================================================================

WITH accepted_offers AS (
    SELECT
        jr.seniority_level,
        o.base_salary,
        COALESCE(o.signing_bonus, 0)                                      AS signing,
        COALESCE(o.equity_value,  0)                                      AS equity,
        o.base_salary
            + COALESCE(o.signing_bonus, 0)
            + COALESCE(o.equity_value,  0)                                AS total_comp
      FROM offers          o
      JOIN applications    a  ON a.id  = o.application_id
      JOIN job_requisitions jr ON jr.id = a.requisition_id
     WHERE o.offer_version   = 1
       AND o.status          = 'accepted'
       AND jr.seniority_level IS NOT NULL
),
ranked AS (
    SELECT
        seniority_level,
        base_salary,
        total_comp,
        COUNT(*) OVER (PARTITION BY seniority_level)                      AS n,
        ROW_NUMBER() OVER (
            PARTITION BY seniority_level
            ORDER BY base_salary
        )                                                                  AS rn_base
      FROM accepted_offers
)
SELECT
    FIELD(
        seniority_level,
        'intern', 'entry', 'mid', 'senior', 'staff',
        'manager', 'director', 'vp', 'executive'
    )                                                                      AS seniority_order,
    seniority_level,
    n                                                                      AS accepted_offers,
    ROUND(AVG(base_salary), 0)                                            AS avg_base_salary,
    MAX(CASE WHEN rn_base = FLOOR(n * 0.25) THEN base_salary END)        AS p25_base_salary,
    MAX(CASE WHEN rn_base = FLOOR(n * 0.50) THEN base_salary END)        AS median_base_salary,
    MAX(CASE WHEN rn_base = FLOOR(n * 0.75) THEN base_salary END)        AS p75_base_salary,
    ROUND(AVG(total_comp), 0)                                             AS avg_total_comp
  FROM ranked
 GROUP BY seniority_level, n
 ORDER BY seniority_order;


-- =============================================================================
-- QUERY 5: Counter-offer success analysis
--
-- Business question:
--   When a candidate declines due to compensation and we send a counter-offer,
--   how often does it succeed — and how much salary increase does it take?
--
-- Use case:
--   Informs the "make counter-offer" decision. If counter-offers succeed only
--   30% of the time, the salary spend may be better directed toward raising
--   initial bands. If they succeed 70% of the time at a modest bump, the
--   counter-offer strategy is justified.
-- =============================================================================

WITH v1_comp_declines AS (
    SELECT
        o.application_id,
        o.base_salary                                                      AS v1_base,
        o.offer_responded_at                                               AS v1_responded_at
      FROM offers o
     WHERE o.offer_version   = 1
       AND o.status          = 'declined'
       AND o.decline_reason  = 'compensation'
),
v2_offers AS (
    SELECT
        o.application_id,
        o.base_salary                                                      AS v2_base,
        o.status                                                           AS v2_status
      FROM offers o
     WHERE o.offer_version = 2
),
combined AS (
    SELECT
        v1.v1_base,
        v2.v2_base,
        v2.v2_status,
        v2.v2_base - v1.v1_base                                           AS salary_increase_dollars,
        ROUND((v2.v2_base - v1.v1_base) * 100.0 / NULLIF(v1.v1_base, 0), 1)
                                                                           AS salary_increase_pct
      FROM v1_comp_declines v1
      JOIN v2_offers         v2 ON v2.application_id = v1.application_id
)
SELECT
    COUNT(*)                                                              AS total_counter_offers,
    COUNT(CASE WHEN v2_status = 'accepted' THEN 1 END)                   AS v2_accepted,
    COUNT(CASE WHEN v2_status = 'declined' THEN 1 END)                   AS v2_declined,
    COUNT(CASE WHEN v2_status = 'expired'  THEN 1 END)                   AS v2_expired,
    ROUND(
        COUNT(CASE WHEN v2_status = 'accepted' THEN 1 END) * 100.0
        / NULLIF(COUNT(*), 0),
        1
    )                                                                     AS counter_offer_acceptance_rate_pct,
    ROUND(AVG(salary_increase_dollars), 0)                               AS avg_salary_increase_dollars,
    ROUND(AVG(salary_increase_pct), 1)                                   AS avg_salary_increase_pct
  FROM combined;


-- =============================================================================
-- QUERY 6: Geographic compensation comparison
--
-- Business question:
--   Do we pay materially different salaries in different locations, and does
--   the spread reflect expected geographic cost-of-labor differentials?
--
-- Use case:
--   Compensation team uses this to calibrate location-based pay tiers. A
--   location showing unexpectedly low median pay relative to peers may indicate
--   an under-indexed band or a seniority mix issue (more junior hires there).
--   Filtered to ≥10 accepted offers per location to suppress noise.
-- =============================================================================

WITH accepted_geo AS (
    SELECT
        jr.location,
        o.base_salary
      FROM offers          o
      JOIN applications    a  ON a.id  = o.application_id
      JOIN job_requisitions jr ON jr.id = a.requisition_id
     WHERE o.offer_version = 1
       AND o.status        = 'accepted'
       AND jr.location    IS NOT NULL
),
ranked AS (
    SELECT
        location,
        base_salary,
        COUNT(*) OVER (PARTITION BY location)                              AS n,
        ROW_NUMBER() OVER (
            PARTITION BY location
            ORDER BY base_salary
        )                                                                   AS rn
      FROM accepted_geo
)
SELECT
    location,
    n                                                                       AS accepted_offers,
    ROUND(AVG(base_salary), 0)                                             AS avg_base_salary,
    MAX(CASE WHEN rn = FLOOR(n * 0.25) THEN base_salary END)             AS p25_base_salary,
    MAX(CASE WHEN rn = FLOOR(n * 0.50) THEN base_salary END)             AS median_base_salary,
    MAX(CASE WHEN rn = FLOOR(n * 0.75) THEN base_salary END)             AS p75_base_salary
  FROM ranked
 WHERE n >= 10
 GROUP BY location, n
 ORDER BY median_base_salary DESC;
