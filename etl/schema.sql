-- =================================================================
-- Cleveland Clinics, OH — Data Warehouse Schema
-- Target: PostgreSQL 13+ (also compatible with MySQL 8 with minor
-- syntax tweaks noted inline)
--
-- IMPORTANT NOTE ON DESIGN:
-- The source CSVs' patient_id / doctor_id / branch_id / case_id
-- values in the fact tables (appointments, billing, lab_reports,
-- prescriptions, surgeries) do NOT reliably match the IDs in the
-- dimension tables (patients, doctors, branches) — see the
-- "Data Quality & Challenges" section of the project report for
-- the full referential-integrity findings. Because of this, FK
-- columns are stored but NOT enforced with FOREIGN KEY constraints
-- here, so no data is silently rejected on load. The ETL pipeline
-- instead computes and logs match-rate statistics into
-- etl_data_quality_log for every load.
-- =================================================================

CREATE SCHEMA IF NOT EXISTS clinic;
SET search_path TO clinic;

-- ---------------- Dimension tables ----------------

CREATE TABLE IF NOT EXISTS patients (
    patient_id                   VARCHAR(20)  PRIMARY KEY,
    date_of_registration         DATE,
    date_of_first_consultation   DATE,
    date_of_latest_consultation  DATE,
    overall_satisfaction_rating  SMALLINT,
    admission                    BOOLEAN,
    surgery_underwent            BOOLEAN,
    total_number_of_visits       INTEGER,
    source_row                   INTEGER,
    load_timestamp               TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS doctors (
    doctor_id                VARCHAR(20)  PRIMARY KEY,
    doctor_name               TEXT,
    specialization             TEXT,
    qualification               TEXT,
    surgery_experience_years   INTEGER,
    practicing_branch          VARCHAR(20),
    other_branches_visited     TEXT,       -- stored as-is (source is a stringified list)
    source_row                 INTEGER,
    load_timestamp             TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS branches (
    branch_id               VARCHAR(20)  PRIMARY KEY,
    location                 TEXT,
    ot_available              BOOLEAN,
    inhouse_anesthetician     BOOLEAN,
    num_ots                   INTEGER,
    inhouse_pharmacy          BOOLEAN,
    inhouse_nutritionist      BOOLEAN,
    inpatient_rooms           INTEGER,
    luxury_suite_rooms        BOOLEAN,
    prayer_room               BOOLEAN,
    kids_play_area            BOOLEAN,
    source_row                INTEGER,
    load_timestamp            TIMESTAMP DEFAULT now()
);

-- ---------------- Fact tables ----------------

CREATE TABLE IF NOT EXISTS appointments (
    serial_no                  INTEGER,
    case_id                    BIGINT,
    patient_id                 VARCHAR(20),   -- not FK-enforced, see note above
    branch_id                  VARCHAR(20),
    date_of_consultation       DATE,
    requested_doctor_id        VARCHAR(20),
    consulted_doctor_id        VARCHAR(20),
    reason_for_visit           TEXT,
    diagnosis                  TEXT,
    prescribed_medication      TEXT,
    followup_required          BOOLEAN,
    nutritionist_recommended   BOOLEAN,
    load_timestamp             TIMESTAMP DEFAULT now(),
    PRIMARY KEY (case_id)
);

CREATE TABLE IF NOT EXISTS billing (
    serial_no                  INTEGER,
    claim_id                   BIGINT PRIMARY KEY,
    patient_id                 VARCHAR(20),
    case_id                    BIGINT,
    branch_id                  VARCHAR(20),
    total_bill                 NUMERIC(12,2),
    insurance_provider         TEXT,
    amount_covered              NUMERIC(12,2),
    out_of_pocket_expenses     NUMERIC(12,2),
    claim_status                TEXT,
    load_timestamp             TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lab_reports (
    serial_no        INTEGER,
    report_id         BIGINT PRIMARY KEY,
    case_id            BIGINT,
    patient_id         VARCHAR(20),
    branch_id          VARCHAR(20),
    test_name           TEXT,
    test_date           DATE,
    result              TEXT,
    load_timestamp     TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS prescriptions (
    serial_no               INTEGER,
    bill_id                  BIGINT PRIMARY KEY,
    patient_id               VARCHAR(20),
    doctor_id                VARCHAR(20),
    medication_name          TEXT,
    dosage                   TEXT,
    quantity                 INTEGER,
    pharmacy_availability    BOOLEAN,
    issued_date              DATE,
    load_timestamp           TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS surgeries (
    serial_no                  INTEGER,
    case_id                    BIGINT PRIMARY KEY,
    patient_id                 VARCHAR(20),
    doctor_id                  VARCHAR(20),
    branch_id                  VARCHAR(20),
    surgery_type               TEXT,
    surgery_date               DATE,
    anesthesia_type            TEXT,
    surgery_outcome            TEXT,
    surgery_duration_hours     NUMERIC(5,2),
    recovery_period_days       INTEGER,
    load_timestamp             TIMESTAMP DEFAULT now()
);

-- ---------------- ETL metadata / observability tables ----------------

CREATE TABLE IF NOT EXISTS etl_load_log (
    load_id         SERIAL PRIMARY KEY,
    table_name      TEXT,
    rows_extracted  INTEGER,
    rows_loaded     INTEGER,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    status          TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS etl_data_quality_log (
    check_id            SERIAL PRIMARY KEY,
    run_timestamp        TIMESTAMP DEFAULT now(),
    fact_table            TEXT,
    fk_column              TEXT,
    references_table       TEXT,
    total_rows              INTEGER,
    matched_rows             INTEGER,
    match_rate_pct           NUMERIC(5,2)
);

-- ---------------- Helpful indexes for reporting ----------------

CREATE INDEX IF NOT EXISTS idx_appt_date ON appointments (date_of_consultation);
CREATE INDEX IF NOT EXISTS idx_appt_branch ON appointments (branch_id);
CREATE INDEX IF NOT EXISTS idx_appt_diagnosis ON appointments (diagnosis);
CREATE INDEX IF NOT EXISTS idx_billing_date ON billing (case_id);
CREATE INDEX IF NOT EXISTS idx_billing_branch ON billing (branch_id);
CREATE INDEX IF NOT EXISTS idx_surgery_date ON surgeries (surgery_date);
