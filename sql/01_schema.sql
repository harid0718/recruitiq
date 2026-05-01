-- =============================================================================
-- RecruitIQ: Recruiting Analytics Schema
-- =============================================================================
-- Simulates the internal data infrastructure of a People Analytics team.
--
-- Design notes:
--   - Surrogate integer PKs on every table
--   - Audit fields (created_at, updated_at) on every table
--   - InnoDB engine throughout for FK enforcement and ACID transactions
--   - All FKs use ON DELETE RESTRICT to prevent accidental orphaning
--   - Indexes on every FK column for join and filter performance
--
-- Production trade-offs (intentionally simplified for this portfolio project):
--   - hiring_manager_name / recruiter_name are denormalized strings;
--     production would FK to an employees / identity table
--   - pipeline_stages.stage_name is an ENUM; production would reference a
--     configurable stage_types lookup table keyed to each requisition template
--   - applications.source is an ENUM; production would reference a
--     sourcing_channels table for richer UTM / campaign attribution
-- =============================================================================

-- Drop in reverse dependency order for clean re-runs during development
DROP TABLE IF EXISTS offers;
DROP TABLE IF EXISTS pipeline_stages;
DROP TABLE IF EXISTS applications;
DROP TABLE IF EXISTS candidates;
DROP TABLE IF EXISTS job_requisitions;


-- -----------------------------------------------------------------------------
-- job_requisitions
-- Represents an approved headcount request for a single role. One requisition
-- can result in multiple hires if headcount > 1, but each hire is tracked
-- through its own application record.
-- -----------------------------------------------------------------------------
CREATE TABLE job_requisitions (
    id                  INT UNSIGNED        NOT NULL AUTO_INCREMENT,
    req_code            VARCHAR(20)         NOT NULL COMMENT 'Human-readable business key, e.g. REQ-2024-042',
    title               VARCHAR(150)        NOT NULL,
    department          VARCHAR(100)                 COMMENT 'Nullable to allow synthetic data quality issues; production would enforce NOT NULL with form validation',
    team                VARCHAR(100)                 COMMENT 'Sub-team within the department',
    location            VARCHAR(150)                 COMMENT 'City name or "Remote"',
    employment_type     ENUM(
                            'full_time',
                            'part_time',
                            'contract',
                            'intern'
                        )                   NOT NULL,
    seniority_level     ENUM(
                            'intern',
                            'entry',
                            'mid',
                            'senior',
                            'staff',
                            'manager',
                            'director',
                            'vp',
                            'executive'
                        )                   NOT NULL,
    -- Denormalized strings; production would FK to an HR / employees table
    hiring_manager_name VARCHAR(150)                 COMMENT 'Denormalized; production would FK to employees table',
    recruiter_name      VARCHAR(150)                 COMMENT 'Denormalized; production would FK to employees table',
    status              ENUM(
                            'draft',
                            'open',
                            'on_hold',
                            'filled',
                            'cancelled'
                        )                   NOT NULL DEFAULT 'draft',
    headcount           TINYINT UNSIGNED    NOT NULL DEFAULT 1 COMMENT 'Number of approved seats to fill',
    target_start_date   DATE                         COMMENT 'Requested start date from the hiring manager',
    salary_range_min    DECIMAL(10,2)                COMMENT 'Approved budget floor for this role',
    salary_range_max    DECIMAL(10,2)                COMMENT 'Approved budget ceiling for this role',
    salary_currency     CHAR(3)             NOT NULL DEFAULT 'USD' COMMENT 'ISO 4217 currency code',
    -- Business state timestamps (distinct from audit fields)
    opened_at           DATETIME                     COMMENT 'When the req transitioned to open status; used for time-to-fill calculations',
    closed_at           DATETIME                     COMMENT 'When the req was filled or cancelled',
    -- Audit
    created_at          DATETIME            NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME            NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    UNIQUE KEY uq_req_code (req_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Approved headcount requests; one row per open role';


-- -----------------------------------------------------------------------------
-- candidates
-- A person who has applied to at least one role. Deduplicated by email address.
-- A candidate may have multiple applications (one per requisition).
-- -----------------------------------------------------------------------------
CREATE TABLE candidates (
    id                      INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    first_name              VARCHAR(100)    NOT NULL,
    last_name               VARCHAR(100)    NOT NULL,
    email                   VARCHAR(254)    NOT NULL COMMENT 'Primary deduplication key; max length per RFC 5321',
    phone                   VARCHAR(30)              COMMENT 'Stored as string to preserve formatting and international prefixes',
    current_title           VARCHAR(150),
    current_company         VARCHAR(150),
    location_city           VARCHAR(100),
    location_state          VARCHAR(100),
    location_country        CHAR(2)         NOT NULL DEFAULT 'US' COMMENT 'ISO 3166-1 alpha-2 country code',
    linkedin_url            VARCHAR(255),
    years_of_experience     DECIMAL(4,1)             COMMENT 'Allows half-year precision, e.g. 3.5',
    highest_education_level ENUM(
                                'high_school',
                                'associate',
                                'bachelor',
                                'master',
                                'mba',
                                'phd',
                                'other'
                            ),
    -- Audit
    created_at              DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    UNIQUE KEY uq_candidate_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='People who have applied for one or more roles; deduplicated by email';


-- -----------------------------------------------------------------------------
-- applications
-- Junction table linking a candidate to a specific job requisition.
-- Represents the act of applying. Source is recorded here (not on candidates)
-- because the same person can be sourced differently for different roles.
-- -----------------------------------------------------------------------------
CREATE TABLE applications (
    id                      INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    candidate_id            INT UNSIGNED    NOT NULL,
    requisition_id          INT UNSIGNED    NOT NULL,
    source                  ENUM(
                                'job_board',
                                'company_website',
                                'linkedin',
                                'employee_referral',
                                'agency',
                                'university',
                                'event',
                                'other'
                            )               NOT NULL COMMENT 'How this specific application originated; intentionally on applications not candidates',
    referral_employee_name  VARCHAR(150)             COMMENT 'Populated when source = employee_referral; production would FK to employees table',
    status                  ENUM(
                                'active',
                                'withdrawn',
                                'rejected',
                                'hired'
                            )               NOT NULL DEFAULT 'active' COMMENT 'Rolled-up status; granular history is in pipeline_stages',
    applied_at              DATETIME        NOT NULL COMMENT 'When the candidate submitted; may differ from created_at if logged retroactively',
    -- Audit
    created_at              DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    -- A candidate may only have one application per requisition
    UNIQUE KEY uq_application (candidate_id, requisition_id),
    -- FK indexes
    KEY idx_applications_candidate_id   (candidate_id),
    KEY idx_applications_requisition_id (requisition_id),

    CONSTRAINT fk_applications_candidate
        FOREIGN KEY (candidate_id)
        REFERENCES candidates (id)
        ON DELETE RESTRICT,

    CONSTRAINT fk_applications_requisition
        FOREIGN KEY (requisition_id)
        REFERENCES job_requisitions (id)
        ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='One row per candidate-requisition pair; source attribution lives here, not on candidates';


-- -----------------------------------------------------------------------------
-- pipeline_stages
-- Append-only log of every stage a candidate passes through per application.
-- Each row records entry into a stage, exit from it, and the outcome.
-- This models the hiring funnel as an event history rather than a single state,
-- enabling per-stage conversion rates and time-in-stage analytics.
-- -----------------------------------------------------------------------------
CREATE TABLE pipeline_stages (
    id                  INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    application_id      INT UNSIGNED    NOT NULL,
    stage_name          ENUM(
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
                        )               NOT NULL COMMENT 'Production would reference a configurable stage_types lookup table',
    stage_order         TINYINT UNSIGNED NOT NULL COMMENT 'Numeric position in the funnel; used for time-between-stages calculations without relying solely on entered_at ordering',
    entered_at          DATETIME        NOT NULL COMMENT 'When the candidate moved into this stage',
    exited_at           DATETIME                 COMMENT 'When the candidate left this stage; NULL means currently active in this stage',
    outcome             ENUM(
                            'advanced',
                            'rejected',
                            'withdrew',
                            'hired',
                            'pending'
                        )               NOT NULL DEFAULT 'pending',
    interviewer_name    VARCHAR(150)             COMMENT 'Who conducted this stage; production would FK to employees table',
    scorecard_rating    ENUM(
                            'strong_no',
                            'no',
                            'mixed',
                            'yes',
                            'strong_yes'
                        )                        COMMENT 'Structured evaluation summary; mirrors rubrics used in Greenhouse / Lever',
    notes               TEXT,
    -- Audit
    created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    -- FK index
    KEY idx_pipeline_stages_application_id (application_id),

    CONSTRAINT fk_pipeline_stages_application
        FOREIGN KEY (application_id)
        REFERENCES applications (id)
        ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Event log of every funnel stage per application; enables conversion rate and time-in-stage analytics';


-- -----------------------------------------------------------------------------
-- offers
-- Formal compensation offers extended to a candidate for a specific application.
-- offer_version supports iterative negotiation (v1 declined → v2 accepted)
-- without overwriting history. decline_reason is a structured enum (not free
-- text) so it is directly queryable for offer close-rate analysis.
-- -----------------------------------------------------------------------------
CREATE TABLE offers (
    id                  INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    application_id      INT UNSIGNED    NOT NULL,
    offer_version       TINYINT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'Increments with each revised offer; enables negotiation history without overwriting',
    status              ENUM(
                            'draft',
                            'sent',
                            'accepted',
                            'declined',
                            'rescinded',
                            'expired'
                        )               NOT NULL DEFAULT 'draft',
    base_salary         DECIMAL(10,2)   NOT NULL,
    bonus_target_pct    DECIMAL(5,2)             COMMENT 'Target annual bonus as a percentage of base salary',
    signing_bonus       DECIMAL(10,2),
    equity_value        DECIMAL(12,2)            COMMENT 'Total grant value at the time of offer; does not track vesting schedule',
    currency            CHAR(3)         NOT NULL DEFAULT 'USD' COMMENT 'ISO 4217 currency code',
    proposed_start_date DATE,
    offer_sent_at       DATETIME                 COMMENT 'When the offer letter was delivered to the candidate',
    offer_expires_at    DATETIME                 COMMENT 'Deadline for candidate response',
    offer_responded_at  DATETIME                 COMMENT 'When the candidate accepted or declined',
    decline_reason      ENUM(
                            'compensation',
                            'competing_offer',
                            'location',
                            'role_fit',
                            'personal',
                            'no_response',
                            'other'
                        )                        COMMENT 'Structured enum (not free text) to enable offer close-rate queries by decline category',
    -- Audit
    created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    -- FK index
    KEY idx_offers_application_id (application_id),

    CONSTRAINT fk_offers_application
        FOREIGN KEY (application_id)
        REFERENCES applications (id)
        ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Compensation offers per application; versioned to support negotiation history without data loss';


ALTER TABLE job_requisitions MODIFY department VARCHAR(100) NULL 
  COMMENT 'Nullable to allow synthetic data quality issues; production would enforce NOT NULL with form validation';
  
  
  
  -- 1. Total count
SELECT COUNT(*) AS total_reqs FROM job_requisitions;

-- 2. Department distribution (Engineering and Manufacturing should dominate)
SELECT department, COUNT(*) AS count
FROM job_requisitions
GROUP BY department
ORDER BY count DESC;

-- 3. Status distribution
SELECT status, COUNT(*) AS count
FROM job_requisitions
GROUP BY status
ORDER BY count DESC;

-- 4. Confirm deliberate data quality issues injected
SELECT 
  COUNT(*) AS total,
  SUM(department IS NULL) AS missing_dept,
  SUM(hiring_manager_name IS NULL) AS missing_hm
FROM job_requisitions;


-- 5. Visual check of 5 random rows
SELECT req_code, title, department, location, status, salary_range_min, salary_range_max
FROM job_requisitions
ORDER BY RAND()
LIMIT 5;


-- 1. Total count
SELECT COUNT(*) AS total_candidates FROM candidates;

-- 2. Country distribution (US should be ~70%)
SELECT location_country, COUNT(*) AS count, 
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM candidates), 1) AS pct
FROM candidates
GROUP BY location_country
ORDER BY count DESC;

-- 3. Education distribution
SELECT highest_education_level, COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM candidates), 1) AS pct
FROM candidates
GROUP BY highest_education_level
ORDER BY count DESC;


-- 4. Years of experience distribution (should be smooth)
SELECT 
  CASE 
    WHEN years_of_experience < 2 THEN '0-2 yrs'
    WHEN years_of_experience < 5 THEN '2-5 yrs'
    WHEN years_of_experience < 10 THEN '5-10 yrs'
    WHEN years_of_experience < 15 THEN '10-15 yrs'
    ELSE '15+ yrs'
  END AS exp_band,
  COUNT(*) AS count
FROM candidates
GROUP BY exp_band
ORDER BY MIN(years_of_experience);


-- 5. Confirm LinkedIn URL distribution (~80% should have one)
SELECT 
  COUNT(*) AS total,
  SUM(linkedin_url IS NOT NULL) AS has_linkedin,
  ROUND(SUM(linkedin_url IS NOT NULL) * 100.0 / COUNT(*), 1) AS pct_with_linkedin
FROM candidates;


-- 6. Detect potential duplicates with same name and company (the injected dupes)
SELECT first_name, last_name, current_company, COUNT(*) AS appearances
FROM candidates
GROUP BY first_name, last_name, current_company
HAVING COUNT(*) > 1
ORDER BY appearances DESC
LIMIT 10;


-- 7. Look at one example of a duplicate side-by-side
WITH dupes AS (
  SELECT first_name, last_name, current_company
  FROM candidates
  GROUP BY first_name, last_name, current_company
  HAVING COUNT(*) > 1
  LIMIT 1
)
SELECT c.id, c.first_name, c.last_name, c.email, c.phone, c.current_company
FROM candidates c
JOIN dupes d 
  ON c.first_name = d.first_name 
  AND c.last_name = d.last_name 
  AND c.current_company = d.current_company
ORDER BY c.id;



-- 1. Total count
SELECT COUNT(*) AS total_applications FROM applications;


-- 2. Applications per candidate distribution
SELECT 
  app_count,
  COUNT(*) AS num_candidates,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT candidate_id) FROM applications), 1) AS pct
FROM (
  SELECT candidate_id, COUNT(*) AS app_count
  FROM applications
  GROUP BY candidate_id
) sub
GROUP BY app_count
ORDER BY app_count;


-- 3. Source channel distribution
SELECT source, COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM applications), 1) AS pct
FROM applications
GROUP BY source
ORDER BY count DESC;


-- 4. Status distribution
SELECT status, COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM applications), 1) AS pct
FROM applications
GROUP BY status
ORDER BY count DESC;


-- 5. Verify referral_employee_name only populated when source = 'employee_referral'
SELECT 
  source,
  SUM(referral_employee_name IS NOT NULL) AS with_referral,
  SUM(referral_employee_name IS NULL) AS without_referral
FROM applications
GROUP BY source
ORDER BY source;


-- 6. CRITICAL: Verify no applied_at is before its requisition's opened_at
SELECT COUNT(*) AS bad_dates
FROM applications a
JOIN job_requisitions r ON a.requisition_id = r.id
WHERE a.applied_at < r.opened_at;

-- 7. Verify hired cap was respected
SELECT 
  (SELECT SUM(headcount) FROM job_requisitions WHERE status = 'filled') AS max_allowed_hires,
  (SELECT COUNT(*) FROM applications WHERE status = 'hired') AS actual_hires;
  
  
-- 8. Sample multi-applicant candidate
WITH multi_appliers AS (
  SELECT candidate_id 
  FROM applications 
  GROUP BY candidate_id 
  HAVING COUNT(*) >= 3 
  LIMIT 1
)
SELECT 
  c.first_name, c.last_name, c.email,
  r.req_code, r.title, r.department,
  a.source, a.status, a.applied_at
FROM applications a
JOIN candidates c ON a.candidate_id = c.id
JOIN job_requisitions r ON a.requisition_id = r.id
WHERE a.candidate_id = (SELECT candidate_id FROM multi_appliers)
ORDER BY a.applied_at;



-- 1. Total count
SELECT COUNT(*) AS total_stages FROM pipeline_stages;

-- 2. Stage funnel — count of applications that REACHED each stage
SELECT stage_name, COUNT(*) AS reached
FROM pipeline_stages
GROUP BY stage_name
ORDER BY 
  FIELD(stage_name, 'applied', 'recruiter_screen', 'hiring_manager_screen',
        'technical_assessment', 'take_home_assignment', 'panel_interview',
        'executive_interview', 'background_check', 'offer', 'hired');
        
        
-- 3. Average stages per application by final outcome
SELECT 
  a.status,
  COUNT(DISTINCT a.id) AS applications,
  COUNT(ps.id) AS total_stages,
  ROUND(COUNT(ps.id) * 1.0 / COUNT(DISTINCT a.id), 2) AS avg_stages_per_app
FROM applications a
LEFT JOIN pipeline_stages ps ON a.id = ps.application_id
GROUP BY a.status
ORDER BY avg_stages_per_app DESC;


-- 4. Verify ALL hired applications reached the 'hired' stage
SELECT 
  COUNT(DISTINCT a.id) AS hired_apps,
  COUNT(DISTINCT CASE WHEN ps.stage_name = 'hired' THEN a.id END) AS reached_hired_stage
FROM applications a
LEFT JOIN pipeline_stages ps ON a.id = ps.application_id
WHERE a.status = 'hired';



-- 5. Verify active applications have NULL exited_at on their final (most recent) stage
WITH latest_stages AS (
  SELECT 
    application_id,
    MAX(stage_order) AS max_order
  FROM pipeline_stages
  GROUP BY application_id
)
SELECT 
  a.status,
  COUNT(*) AS total_final_stages,
  SUM(ps.exited_at IS NULL) AS final_stages_null_exit
FROM applications a
JOIN latest_stages ls ON a.id = ls.application_id
JOIN pipeline_stages ps ON ps.application_id = ls.application_id 
  AND ps.stage_order = ls.max_order
GROUP BY a.status;


-- 6. Verify scorecard ratings only on interview stages
SELECT 
  stage_name,
  SUM(scorecard_rating IS NOT NULL) AS with_scorecard,
  SUM(scorecard_rating IS NULL) AS without_scorecard
FROM pipeline_stages
GROUP BY stage_name
ORDER BY stage_name;


-- 7. CRITICAL DATA QUALITY: count stages with entered_at before applied_at (injected)
SELECT COUNT(*) AS bad_dates_detected
FROM pipeline_stages ps
JOIN applications a ON ps.application_id = a.id
WHERE ps.entered_at < a.applied_at;


-- 8. CRITICAL DATA QUALITY: count applications with stage_order out of chronological sync
WITH stage_check AS (
  SELECT 
    application_id,
    stage_order,
    entered_at,
    ROW_NUMBER() OVER (PARTITION BY application_id ORDER BY entered_at) AS chronological_order
  FROM pipeline_stages
)
SELECT COUNT(DISTINCT application_id) AS apps_with_order_mismatch
FROM stage_check
WHERE stage_order != chronological_order;



-- 9. Sample one hired candidate's full journey
WITH one_hired AS (
  SELECT id FROM applications WHERE status = 'hired' LIMIT 1
)
SELECT ps.stage_order, ps.stage_name, ps.entered_at, ps.exited_at, ps.outcome, ps.scorecard_rating
FROM pipeline_stages ps
WHERE ps.application_id = (SELECT id FROM one_hired)
ORDER BY ps.stage_order;




-- 1. Total count and status breakdown
SELECT status, offer_version, COUNT(*) AS count
FROM offers
GROUP BY status, offer_version
ORDER BY offer_version, status;



-- 2. Verify all hired applications have an accepted offer
SELECT 
  COUNT(DISTINCT a.id) AS hired_apps,
  COUNT(DISTINCT CASE WHEN o.status = 'accepted' AND o.offer_version = 1 THEN a.id END) AS with_v1_accepted
FROM applications a
LEFT JOIN offers o ON a.id = o.application_id
WHERE a.status = 'hired';



-- 3. Salary distribution by seniority
SELECT 
  jr.seniority_level,
  COUNT(o.id) AS num_offers,
  ROUND(AVG(o.base_salary), 0) AS avg_salary,
  ROUND(MIN(o.base_salary), 0) AS min_salary,
  ROUND(MAX(o.base_salary), 0) AS max_salary
FROM offers o
JOIN applications a ON o.application_id = a.id
JOIN job_requisitions jr ON a.requisition_id = jr.id
WHERE o.offer_version = 1
GROUP BY jr.seniority_level
ORDER BY avg_salary;


-- 4. Decline reasons distribution
SELECT decline_reason, COUNT(*) AS count
FROM offers
WHERE decline_reason IS NOT NULL
GROUP BY decline_reason
ORDER BY count DESC;


-- 5. Counter-offer success rate (v1 declined → v2 outcome)
SELECT 
  v2.status AS v2_status,
  COUNT(*) AS count,
  ROUND(AVG(v2.base_salary - v1.base_salary), 0) AS avg_salary_increase,
  ROUND(AVG((v2.base_salary - v1.base_salary) / v1.base_salary * 100), 1) AS avg_pct_increase
FROM offers v2
JOIN offers v1 ON v2.application_id = v1.application_id 
  AND v2.offer_version = 2 AND v1.offer_version = 1
GROUP BY v2.status;

-- 6. CRITICAL DATA QUALITY: orphan offers (no matching pipeline stage)
SELECT COUNT(*) AS orphan_offers
FROM offers o
LEFT JOIN pipeline_stages ps 
  ON ps.application_id = o.application_id AND ps.stage_name = 'offer'
WHERE ps.id IS NULL;


-- 7. CRITICAL DATA QUALITY: v2 offers accepted while application status mismatches
SELECT COUNT(*) AS sync_issues
FROM offers o
JOIN applications a ON o.application_id = a.id
WHERE o.offer_version = 2 
  AND o.status = 'accepted'
  AND a.status IN ('rejected', 'withdrawn');
  
  
  
-- 8. Sample a complete negotiation story
WITH neg AS (
  SELECT application_id 
  FROM offers 
  WHERE offer_version = 2 AND status = 'accepted'
  LIMIT 1
)
SELECT 
  c.first_name, c.last_name, 
  jr.title, jr.department,
  o.offer_version, o.status, o.base_salary, o.decline_reason,
  o.offer_sent_at, o.offer_responded_at
FROM offers o
JOIN applications a ON o.application_id = a.id
JOIN candidates c ON a.candidate_id = c.id
JOIN job_requisitions jr ON a.requisition_id = jr.id
WHERE o.application_id = (SELECT application_id FROM neg)
ORDER BY o.offer_version;