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
