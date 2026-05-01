-- =============================================================================
-- File   : sql/analysis/06_pipeline_diversity.sql
-- Purpose: Geographic diversity analysis through the recruiting funnel
--
-- Business question:
--   "Does our pipeline reflect geographic diversity — and are we losing
--    candidates from specific countries at particular stages of the funnel
--    at a higher rate than others?"
--
-- Why geography as proxy:
--   This synthetic dataset deliberately excludes demographic PII (gender,
--   ethnicity, disability status). In a real People Analytics environment,
--   demographic data is analyzed alongside recruiting outcomes — with
--   candidate consent, proper data governance controls, and legal review for
--   each jurisdiction. Geography is one of several legitimate, lower-risk
--   signals that does not require PII collection. The query architecture
--   demonstrated here — funnel progression rates, representation lift,
--   experience-band analysis — extends identically to demographic dimensions
--   when that data is available and appropriately governed.
--
-- Intended consumers:
--   - People Analytics (representation modeling, funnel equity analysis)
--   - Diversity & Inclusion team (sourcing channel audits)
--   - Recruiting leadership (geographic coverage of sourcing programs)
-- =============================================================================


-- =============================================================================
-- QUERY 1: Application distribution by candidate country
--
-- Business question:
--   Where are our applicants coming from, and which countries are most
--   represented in the pipeline?
--
-- Use case:
--   Baseline denominator for all downstream diversity queries. A pipeline
--   dominated by one country may reflect sourcing channel geography rather
--   than genuine interest distribution.
-- =============================================================================

SELECT
    c.location_country,
    COUNT(*)                                                              AS total_applications,
    ROUND(
        COUNT(*) * 100.0
        / NULLIF(SUM(COUNT(*)) OVER (), 0),
        1
    )                                                                     AS pct_of_pipeline
  FROM applications a
  JOIN candidates   c ON c.id = a.candidate_id
 WHERE c.location_country IS NOT NULL
 GROUP BY c.location_country
 ORDER BY total_applications DESC;


-- =============================================================================
-- QUERY 2: Pipeline progression by candidate country
--
-- Business question:
--   Do candidates from certain countries advance through the funnel at
--   materially different rates than the overall population?
--
-- Use case:
--   If candidates from a particular country show significantly lower
--   panel_interview or offer progression rates, that may indicate screening
--   criteria that inadvertently filter for geography-correlated proxies
--   (timezone availability, location requirements, language of résumé) rather
--   than role-relevant skills. Filtered to ≥100 applications per country
--   for statistical stability.
-- =============================================================================

SELECT
    c.location_country,
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
  JOIN candidates   c ON c.id = a.candidate_id
  LEFT JOIN pipeline_stages ps_screen
         ON ps_screen.application_id = a.id
        AND ps_screen.stage_name     = 'recruiter_screen'
  LEFT JOIN pipeline_stages ps_panel
         ON ps_panel.application_id  = a.id
        AND ps_panel.stage_name      = 'panel_interview'
  LEFT JOIN pipeline_stages ps_offer
         ON ps_offer.application_id  = a.id
        AND ps_offer.stage_name      = 'offer'
 WHERE c.location_country IS NOT NULL
 GROUP BY c.location_country
HAVING COUNT(*) >= 100
 ORDER BY pct_hired DESC;


-- =============================================================================
-- QUERY 3: Education distribution — all applicants vs. hired
--
-- Business question:
--   Is a particular education level over- or under-represented among hires
--   relative to the applicant pool?
--
-- Use case:
--   Lift > 1.0 means that education level is over-represented in hires vs.
--   the full applicant pool. If "bachelor" has a lift of 1.3 and "no_degree"
--   has a lift of 0.5, the process may be filtering on credential rather than
--   demonstrated skill — a signal worth investigating with hiring managers.
-- =============================================================================

WITH all_apps AS (
    SELECT
        c.highest_education_level,
        COUNT(*)                                                           AS app_count
      FROM applications a
      JOIN candidates   c ON c.id = a.candidate_id
     WHERE c.highest_education_level IS NOT NULL
     GROUP BY c.highest_education_level
),
hired_apps AS (
    SELECT
        c.highest_education_level,
        COUNT(*)                                                           AS hired_count
      FROM applications a
      JOIN candidates   c ON c.id = a.candidate_id
     WHERE a.status                    = 'hired'
       AND c.highest_education_level  IS NOT NULL
     GROUP BY c.highest_education_level
),
totals AS (
    SELECT
        SUM(app_count)    AS total_apps,
        (SELECT SUM(hired_count) FROM hired_apps) AS total_hires
      FROM all_apps
)
SELECT
    aa.highest_education_level,
    aa.app_count                                                           AS all_applicants,
    ROUND(aa.app_count * 100.0 / NULLIF(t.total_apps, 0), 1)             AS all_applicants_pct,
    COALESCE(ha.hired_count, 0)                                           AS hires,
    ROUND(COALESCE(ha.hired_count, 0) * 100.0 / NULLIF(t.total_hires, 0), 1)
                                                                           AS hired_pct,
    ROUND(
        (COALESCE(ha.hired_count, 0) * 100.0 / NULLIF(t.total_hires, 0))
        / NULLIF(aa.app_count * 100.0 / NULLIF(t.total_apps, 0), 0),
        2
    )                                                                      AS lift
  FROM all_apps    aa
  LEFT JOIN hired_apps ha ON ha.highest_education_level = aa.highest_education_level
  CROSS JOIN totals t
 ORDER BY lift DESC;


-- =============================================================================
-- QUERY 4: Years of experience — pipeline vs. hired
--
-- Business question:
--   Are candidates with certain experience levels hired at a higher rate
--   than their representation in the applicant pool would predict?
--
-- Use case:
--   Experience bands reveal whether the company is successfully hiring the
--   seniority mix it targets. If "15+" experience is over-represented in
--   hires relative to applicants, the pipeline may be over-indexed on senior
--   candidates. If "0-2" has very low hire lift, entry-level hiring may be
--   structurally deprioritized regardless of stated intent.
-- =============================================================================

WITH banded AS (
    SELECT
        a.id                                                               AS application_id,
        a.status,
        CASE
            WHEN c.years_of_experience <  2  THEN '0-2'
            WHEN c.years_of_experience <  5  THEN '2-5'
            WHEN c.years_of_experience < 10  THEN '5-10'
            WHEN c.years_of_experience < 15  THEN '10-15'
            ELSE                                  '15+'
        END                                                                AS experience_band,
        CASE
            WHEN c.years_of_experience <  2  THEN 1
            WHEN c.years_of_experience <  5  THEN 2
            WHEN c.years_of_experience < 10  THEN 3
            WHEN c.years_of_experience < 15  THEN 4
            ELSE                                  5
        END                                                                AS band_order
      FROM applications a
      JOIN candidates   c ON c.id = a.candidate_id
     WHERE c.years_of_experience IS NOT NULL
),
totals AS (
    SELECT
        COUNT(*)                                                           AS total_apps,
        COUNT(CASE WHEN status = 'hired' THEN 1 END)                      AS total_hires
      FROM banded
)
SELECT
    b.experience_band,
    COUNT(*)                                                              AS total_applications,
    ROUND(COUNT(*) * 100.0 / NULLIF(t.total_apps, 0), 1)                AS pct_of_applicants,
    COUNT(CASE WHEN b.status = 'hired' THEN 1 END)                       AS total_hires,
    ROUND(
        COUNT(CASE WHEN b.status = 'hired' THEN 1 END) * 100.0
        / NULLIF(t.total_hires, 0),
        1
    )                                                                     AS pct_of_hires,
    ROUND(
        COUNT(CASE WHEN b.status = 'hired' THEN 1 END) * 100.0
        / NULLIF(COUNT(*), 0),
        1
    )                                                                     AS hire_rate_pct
  FROM banded   b
  CROSS JOIN totals t
 GROUP BY b.experience_band, b.band_order, t.total_apps, t.total_hires
 ORDER BY b.band_order;


-- =============================================================================
-- QUERY 5: Offer acceptance rate by candidate country
--
-- Business question:
--   Are our compensation packages competitive in all the markets we recruit
--   from, or do candidates from certain countries decline at higher rates?
--
-- Use case:
--   A high decline rate from a specific country may indicate the base salary
--   is below local market rates, or that benefits packages do not transfer
--   well internationally (e.g., US-centric equity or healthcare benefits).
--   Filtered to countries with ≥10 v1 offers to avoid small-sample noise.
-- =============================================================================

SELECT
    c.location_country,
    COUNT(*)                                                              AS total_offers,
    COUNT(CASE WHEN o.status = 'accepted'  THEN 1 END)                   AS accepted,
    COUNT(CASE WHEN o.status = 'declined'  THEN 1 END)                   AS declined,
    ROUND(
        COUNT(CASE WHEN o.status = 'accepted' THEN 1 END) * 100.0
        / NULLIF(COUNT(*), 0),
        1
    )                                                                     AS acceptance_rate_pct
  FROM offers       o
  JOIN applications a  ON a.id  = o.application_id
  JOIN candidates   c  ON c.id  = a.candidate_id
 WHERE o.offer_version          = 1
   AND c.location_country      IS NOT NULL
 GROUP BY c.location_country
HAVING COUNT(*) >= 10
 ORDER BY acceptance_rate_pct DESC;


-- =============================================================================
-- QUERY 6: Source channel geographic representation
--
-- Business question:
--   Does each sourcing channel reach a geographically diverse candidate pool,
--   or do some channels skew heavily toward a single country?
--
-- Use case:
--   A sourcing channel that delivers 85% of its candidates from one country
--   may be underperforming as a diversity lever even if its conversion rate
--   is strong. D&I teams use this to prioritize which channels to expand
--   (e.g., invest more in university partnerships in underrepresented markets)
--   and which to complement with targeted outreach programs.
-- =============================================================================

SELECT
    a.source,
    c.location_country,
    COUNT(*)                                                              AS applications,
    ROUND(
        COUNT(*) * 100.0
        / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY a.source), 0),
        1
    )                                                                     AS pct_of_source
  FROM applications a
  JOIN candidates   c ON c.id = a.candidate_id
 WHERE c.location_country IS NOT NULL
 GROUP BY a.source, c.location_country
 ORDER BY a.source, applications DESC;
