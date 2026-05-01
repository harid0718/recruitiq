-- =============================================================================
-- File   : sql/analysis/01_funnel_metrics.sql
-- Purpose: Stage-by-stage conversion rate analysis of the recruiting pipeline
--
-- Business question:
--   "At each stage of the hiring funnel, what percentage of candidates advance
--    to the next stage — and where do the biggest drop-offs occur?"
--
-- Intended consumers:
--   - Recruiting leadership (executive funnel health view)
--   - Talent Acquisition team (stage-level bottleneck detection)
--   - Hiring managers (comparison across departments and seniority levels)
--
-- How to interpret:
--   Lower conversion at a stage indicates a bottleneck or filter. A recruiter
--   screen with 30% conversion means 70% of applicants are screened out there.
--   Trends over time signal process changes (e.g., stricter take-home rubric).
--   Compare across departments and seniority levels to identify structural
--   differences rather than uniform pipeline problems.
-- =============================================================================


-- =============================================================================
-- QUERY 1: Overall funnel conversion
--
-- Business question:
--   Of all applications that entered the pipeline, how many reached each stage,
--   and what fraction advanced from that stage to the next?
--
-- Use case:
--   Single-pane-of-glass funnel view for recruiting leadership. Highlight
--   any stage where advance_rate drops sharply — that is the primary bottleneck.
-- =============================================================================

WITH stage_counts AS (
    SELECT
        ps.stage_name,
        COUNT(DISTINCT ps.application_id)                       AS applications_at_stage,
        COUNT(DISTINCT CASE WHEN ps.outcome = 'advanced'
                            THEN ps.application_id END)         AS applications_advanced
      FROM pipeline_stages ps
     GROUP BY ps.stage_name
),
total_applied AS (
    SELECT COUNT(*) AS n FROM applications
),
ordered AS (
    SELECT
        sc.stage_name,
        sc.applications_at_stage,
        sc.applications_advanced,
        ta.n                                                     AS total_applications,
        FIELD(
            sc.stage_name,
            'applied',
            'recruiter_screen',
            'hiring_manager_screen',
            'technical_assessment',
            'take_home_assignment',
            'panel_interview',
            'executive_interview',
            'background_check',
            'offer',
            'hired'
        )                                                        AS stage_order
      FROM stage_counts sc
     CROSS JOIN total_applied ta
)
SELECT
    stage_order                                                                          AS `#`,
    stage_name,
    applications_at_stage,
    ROUND(applications_at_stage * 100.0 / total_applications, 1)                        AS pct_of_all_applicants,
    applications_advanced,
    ROUND(applications_advanced * 100.0 / NULLIF(applications_at_stage, 0), 1)          AS advance_rate_pct,
    ROUND(
        100.0 - applications_advanced * 100.0 / NULLIF(applications_at_stage, 0),
        1
    )                                                                                    AS drop_off_rate_pct
  FROM ordered
 WHERE stage_order > 0
 ORDER BY stage_order;


-- =============================================================================
-- QUERY 2: Funnel by department
--
-- Business question:
--   Do certain departments have significantly tighter or wider funnels than
--   the company average? Which department loses the most candidates at screen?
--
-- Use case:
--   Surfaced to department heads and recruiting business partners to benchmark
--   their funnel against the org. Filter to ≥10 applications per department
--   to avoid small-sample noise.
-- =============================================================================

WITH dept_stage_counts AS (
    SELECT
        jr.department,
        ps.stage_name,
        COUNT(DISTINCT ps.application_id)                       AS applications_at_stage,
        COUNT(DISTINCT CASE WHEN ps.outcome = 'advanced'
                            THEN ps.application_id END)         AS applications_advanced
      FROM pipeline_stages ps
      JOIN applications    a  ON a.id  = ps.application_id
      JOIN job_requisitions jr ON jr.id = a.requisition_id
     WHERE jr.department IS NOT NULL
     GROUP BY jr.department, ps.stage_name
),
dept_totals AS (
    SELECT
        jr.department,
        COUNT(DISTINCT a.id) AS total_dept_applications
      FROM applications     a
      JOIN job_requisitions jr ON jr.id = a.requisition_id
     WHERE jr.department IS NOT NULL
     GROUP BY jr.department
    HAVING COUNT(DISTINCT a.id) >= 10
)
SELECT
    dsc.department,
    FIELD(
        dsc.stage_name,
        'applied',
        'recruiter_screen',
        'hiring_manager_screen',
        'technical_assessment',
        'take_home_assignment',
        'panel_interview',
        'executive_interview',
        'background_check',
        'offer',
        'hired'
    )                                                                                    AS stage_order,
    dsc.stage_name,
    dt.total_dept_applications,
    dsc.applications_at_stage,
    ROUND(dsc.applications_at_stage * 100.0 / dt.total_dept_applications, 1)           AS pct_of_dept_applicants,
    ROUND(dsc.applications_advanced * 100.0 / NULLIF(dsc.applications_at_stage, 0), 1) AS advance_rate_pct
  FROM dept_stage_counts dsc
  JOIN dept_totals        dt  ON dt.department = dsc.department
 WHERE FIELD(
           dsc.stage_name,
           'applied', 'recruiter_screen', 'hiring_manager_screen',
           'technical_assessment', 'take_home_assignment', 'panel_interview',
           'executive_interview', 'background_check', 'offer', 'hired'
       ) > 0
 ORDER BY dsc.department, stage_order;


-- =============================================================================
-- QUERY 3: Funnel by seniority level
--
-- Business question:
--   Are senior and executive roles harder to fill because candidates drop off
--   at specific stages (e.g., executive interview), or do they track similarly
--   to junior pipelines?
--
-- Use case:
--   Informs recruiter specialization and interview panel design. If executive
--   roles show very low panel_interview advance rates, the panel rubric may
--   need calibration.
-- =============================================================================

WITH seniority_stage_counts AS (
    SELECT
        jr.seniority_level,
        ps.stage_name,
        COUNT(DISTINCT ps.application_id)                       AS applications_at_stage,
        COUNT(DISTINCT CASE WHEN ps.outcome = 'advanced'
                            THEN ps.application_id END)         AS applications_advanced
      FROM pipeline_stages ps
      JOIN applications     a  ON a.id  = ps.application_id
      JOIN job_requisitions jr ON jr.id = a.requisition_id
     WHERE jr.seniority_level IS NOT NULL
     GROUP BY jr.seniority_level, ps.stage_name
),
seniority_totals AS (
    SELECT
        jr.seniority_level,
        COUNT(DISTINCT a.id) AS total_seniority_applications
      FROM applications     a
      JOIN job_requisitions jr ON jr.id = a.requisition_id
     WHERE jr.seniority_level IS NOT NULL
     GROUP BY jr.seniority_level
)
SELECT
    ssc.seniority_level,
    FIELD(
        ssc.stage_name,
        'applied',
        'recruiter_screen',
        'hiring_manager_screen',
        'technical_assessment',
        'take_home_assignment',
        'panel_interview',
        'executive_interview',
        'background_check',
        'offer',
        'hired'
    )                                                                                    AS stage_order,
    ssc.stage_name,
    st.total_seniority_applications,
    ssc.applications_at_stage,
    ROUND(ssc.applications_at_stage * 100.0 / st.total_seniority_applications, 1)      AS pct_of_seniority_applicants,
    ROUND(ssc.applications_advanced * 100.0 / NULLIF(ssc.applications_at_stage, 0), 1) AS advance_rate_pct
  FROM seniority_stage_counts ssc
  JOIN seniority_totals        st  ON st.seniority_level = ssc.seniority_level
 WHERE FIELD(
           ssc.stage_name,
           'applied', 'recruiter_screen', 'hiring_manager_screen',
           'technical_assessment', 'take_home_assignment', 'panel_interview',
           'executive_interview', 'background_check', 'offer', 'hired'
       ) > 0
 ORDER BY
    FIELD(
        ssc.seniority_level,
        'intern', 'entry', 'mid', 'senior', 'staff', 'manager', 'director', 'vp', 'executive'
    ),
    stage_order;


-- =============================================================================
-- QUERY 4: Time spent in each stage (median, p25, p75)
--
-- Business question:
--   Where in the pipeline do candidates wait the longest before hearing back?
--   Are some stages consistently slow (scheduling bottleneck, committee delay)?
--
-- Use case:
--   Candidate experience and process efficiency analysis. Stages with high p75
--   indicate the tail of slow responses is pulling down offer acceptance rates.
--   MySQL does not have a native PERCENTILE_CONT function; approximate using
--   the standard ROW_NUMBER / COUNT approach.
-- =============================================================================

WITH stage_durations AS (
    SELECT
        stage_name,
        TIMESTAMPDIFF(DAY, entered_at, exited_at) AS days_in_stage,
        COUNT(*) OVER (PARTITION BY stage_name)   AS stage_n
      FROM pipeline_stages
     WHERE exited_at IS NOT NULL
       AND exited_at >= entered_at
),
ranked AS (
    SELECT
        stage_name,
        days_in_stage,
        stage_n,
        ROW_NUMBER() OVER (PARTITION BY stage_name ORDER BY days_in_stage) AS rn
      FROM stage_durations
)
SELECT
    FIELD(
        stage_name,
        'applied',
        'recruiter_screen',
        'hiring_manager_screen',
        'technical_assessment',
        'take_home_assignment',
        'panel_interview',
        'executive_interview',
        'background_check',
        'offer',
        'hired'
    )                                                              AS stage_order,
    stage_name,
    stage_n                                                        AS completed_stages,
    MAX(CASE WHEN rn = FLOOR(stage_n * 0.25) THEN days_in_stage END) AS p25_days,
    MAX(CASE WHEN rn = FLOOR(stage_n * 0.50) THEN days_in_stage END) AS median_days,
    MAX(CASE WHEN rn = FLOOR(stage_n * 0.75) THEN days_in_stage END) AS p75_days
  FROM ranked
 WHERE FIELD(
           stage_name,
           'applied', 'recruiter_screen', 'hiring_manager_screen',
           'technical_assessment', 'take_home_assignment', 'panel_interview',
           'executive_interview', 'background_check', 'offer', 'hired'
       ) > 0
 GROUP BY stage_name, stage_n
 ORDER BY stage_order;


-- =============================================================================
-- QUERY 5: Stage drop-off reasons
--
-- Business question:
--   At each stage, are candidates primarily rejected by the company, or are
--   they withdrawing themselves? A high withdrew count signals candidate
--   experience friction at that specific point.
--
-- Use case:
--   Distinguish between company-driven filtering (rejected) and candidate-
--   driven drop-off (withdrew). If withdrew spikes at panel_interview, the
--   process may be too long or poorly communicated at that point. If rejected
--   spikes at recruiter_screen, screening criteria may need recalibration.
-- =============================================================================

SELECT
    FIELD(
        ps.stage_name,
        'applied',
        'recruiter_screen',
        'hiring_manager_screen',
        'technical_assessment',
        'take_home_assignment',
        'panel_interview',
        'executive_interview',
        'background_check',
        'offer',
        'hired'
    )                                                  AS stage_order,
    ps.stage_name,
    ps.outcome,
    COUNT(*)                                           AS count,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY ps.stage_name),
        1
    )                                                  AS pct_of_stage
  FROM pipeline_stages ps
 WHERE ps.outcome IS NOT NULL
   AND FIELD(
           ps.stage_name,
           'applied', 'recruiter_screen', 'hiring_manager_screen',
           'technical_assessment', 'take_home_assignment', 'panel_interview',
           'executive_interview', 'background_check', 'offer', 'hired'
       ) > 0
 GROUP BY ps.stage_name, ps.outcome
 ORDER BY stage_order, ps.outcome;
