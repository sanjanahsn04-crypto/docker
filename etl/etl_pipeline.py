#!/usr/bin/env python3
"""
etl_pipeline.py
================
Cleveland Clinics, OH — Data Pipeline Construction (Task 1)

Extracts hospital data from the source CSV files, transforms it into a
clean relational structure, and loads it into PostgreSQL (works with
MySQL too — see README for the small connection-string change needed).

Usage:
    python etl_pipeline.py --data-dir ./data --db-url postgresql+psycopg2://postgres:postgres@localhost:5432/cleveland_clinics_oh

Design notes:
- Column names are standardized to snake_case to match schema.sql.
- Dates are parsed from the source's M/D/YYYY text format into real DATE columns.
- Yes/No text fields are converted to proper booleans.
- Duplicate rows (if any) are dropped defensively; the source data was
  found to have none, but production data won't be this clean forever.
- Referential integrity between fact tables (appointments, billing,
  lab_reports, prescriptions, surgeries) and dimension tables (patients,
  doctors, branches) is NOT enforced with hard foreign keys, because the
  match rate in this dataset is extremely low (see README /
  Challenges report). Enforcing FKs would silently drop the vast
  majority of rows. Instead every load computes and stores match-rate
  statistics in etl_data_quality_log so the gap is visible and
  measurable rather than hidden.
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("etl")

YES_NO_TRUE = {"yes", "y", "true", "1"}


def to_bool(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin(YES_NO_TRUE)


def to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, format="%m/%d/%Y", errors="coerce").dt.date


def clean_str(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()


# ---------------------------------------------------------------------
# Per-table transform functions
# ---------------------------------------------------------------------

def transform_patients(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["patient_id"] = clean_str(df["Patient ID"])
    out["date_of_registration"] = to_date(df["Date of Registration"])
    out["date_of_first_consultation"] = to_date(df["Date of First Consultation"])
    out["date_of_latest_consultation"] = to_date(df["Date of Latest Consultation"])
    out["overall_satisfaction_rating"] = pd.to_numeric(df["Overall Satisfaction Rating"], errors="coerce")
    out["admission"] = to_bool(df["Admission"])
    out["surgery_underwent"] = to_bool(df["Surgery Underwent"])
    out["total_number_of_visits"] = pd.to_numeric(df["Total Number of Visits"], errors="coerce")
    out["source_row"] = df["Sl. No"]
    return out.drop_duplicates(subset=["patient_id"])


def transform_doctors(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["doctor_id"] = clean_str(df["Doctor ID"])
    out["doctor_name"] = clean_str(df["Doctor Name"])
    out["specialization"] = clean_str(df["Specialization"])
    out["qualification"] = clean_str(df["Qualification"])
    out["surgery_experience_years"] = pd.to_numeric(df["Surgery Experience"], errors="coerce")
    out["practicing_branch"] = clean_str(df["Practicing Branch"])
    out["other_branches_visited"] = clean_str(df["Other Branches Visited"])
    out["source_row"] = df["Sl. No"]
    return out.drop_duplicates(subset=["doctor_id"])


def transform_branches(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["branch_id"] = clean_str(df["Branch ID"])
    out["location"] = clean_str(df["Location"])
    out["ot_available"] = to_bool(df["OT_Available"])
    out["inhouse_anesthetician"] = to_bool(df["Inhouse Anesthetician"])
    out["num_ots"] = pd.to_numeric(df["No. of OTs"], errors="coerce")
    out["inhouse_pharmacy"] = to_bool(df["Inhouse Pharmacy"])
    out["inhouse_nutritionist"] = to_bool(df["Inhouse Nutritionist"])
    out["inpatient_rooms"] = pd.to_numeric(df["Inpatient Rooms"], errors="coerce")
    out["luxury_suite_rooms"] = to_bool(df["Luxury Suite Rooms"])
    out["prayer_room"] = to_bool(df["Prayer Room"])
    out["kids_play_area"] = to_bool(df["Kids Play Area"])
    out["source_row"] = df["Sl. No"]
    return out.drop_duplicates(subset=["branch_id"])


def transform_appointments(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["serial_no"] = df["Serial No"]
    out["case_id"] = pd.to_numeric(df["Case ID"], errors="coerce")
    out["patient_id"] = clean_str(df["Patient ID"])
    out["branch_id"] = clean_str(df["Branch ID"])
    out["date_of_consultation"] = to_date(df["Date of Consultation"])
    out["requested_doctor_id"] = clean_str(df["Requested Doctor ID"])
    out["consulted_doctor_id"] = clean_str(df["Consulted Doctor ID"])
    out["reason_for_visit"] = clean_str(df["Reason for Visit"])
    out["diagnosis"] = clean_str(df["Diagnosis"])
    out["prescribed_medication"] = clean_str(df["Prescribed Medication"])
    out["followup_required"] = to_bool(df["Followup Required"])
    out["nutritionist_recommended"] = to_bool(df["Nutritionist Recommended"])
    return out.drop_duplicates(subset=["case_id"])


def transform_billing(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["serial_no"] = df["Serial No"]
    out["claim_id"] = pd.to_numeric(df["Claim ID"], errors="coerce")
    out["patient_id"] = clean_str(df["Patient ID"])
    out["case_id"] = pd.to_numeric(df["Case ID"], errors="coerce")
    out["branch_id"] = clean_str(df["Branch ID"])
    out["total_bill"] = pd.to_numeric(df["Total Bill"], errors="coerce")
    out["insurance_provider"] = clean_str(df["Insurance Provider"])
    out["amount_covered"] = pd.to_numeric(df["Amount Covered"], errors="coerce")
    out["out_of_pocket_expenses"] = pd.to_numeric(df["Out-of-Pocket Expenses"], errors="coerce")
    out["claim_status"] = clean_str(df["Claim Status"])
    return out.drop_duplicates(subset=["claim_id"])


def transform_lab_reports(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["serial_no"] = df["Serial No"]
    out["report_id"] = pd.to_numeric(df["Report ID"], errors="coerce")
    out["case_id"] = pd.to_numeric(df["Case ID"], errors="coerce")
    out["patient_id"] = clean_str(df["Patient ID"])
    out["branch_id"] = clean_str(df["Branch ID"])
    out["test_name"] = clean_str(df["Test Name"])
    out["test_date"] = to_date(df["Test Date"])
    out["result"] = clean_str(df["Result"])
    return out.drop_duplicates(subset=["report_id"])


def transform_prescriptions(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["serial_no"] = df["Serial No"]
    out["bill_id"] = pd.to_numeric(df["Bill ID"], errors="coerce")
    out["patient_id"] = clean_str(df["Patient ID"])
    out["doctor_id"] = clean_str(df["Doctor ID"])
    out["medication_name"] = clean_str(df["Medication Name"])
    out["dosage"] = clean_str(df["Dosage"])
    out["quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    out["pharmacy_availability"] = to_bool(df["Pharmacy Availability"])
    out["issued_date"] = to_date(df["Issued Date"])
    return out.drop_duplicates(subset=["bill_id"])


def transform_surgeries(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["serial_no"] = df["Serial No"]
    out["case_id"] = pd.to_numeric(df["Case ID"], errors="coerce")
    out["patient_id"] = clean_str(df["Patient ID"])
    out["doctor_id"] = clean_str(df["Doctor ID"])
    out["branch_id"] = clean_str(df["Branch ID"])
    out["surgery_type"] = clean_str(df["Surgery Type"])
    out["surgery_date"] = to_date(df["Surgery Date"])
    out["anesthesia_type"] = clean_str(df["Anesthesia Type"])
    out["surgery_outcome"] = clean_str(df["Surgery Outcome"])
    out["surgery_duration_hours"] = pd.to_numeric(df["Surgery Duration (hours)"], errors="coerce")
    out["recovery_period_days"] = pd.to_numeric(df["Recovery Period (days)"], errors="coerce")
    return out.drop_duplicates(subset=["case_id"])


TABLES = {
    # table_name: (source_csv, transform_fn)
    "patients": ("patients.csv", transform_patients),
    "doctors": ("doctors.csv", transform_doctors),
    "branches": ("branches.csv", transform_branches),
    "appointments": ("appointments.csv", transform_appointments),
    "billing": ("billing.csv", transform_billing),
    "lab_reports": ("lab_reports.csv", transform_lab_reports),
    "prescriptions": ("prescriptions.csv", transform_prescriptions),
    "surgeries": ("surgeries.csv", transform_surgeries),
}

# (fact_table, fk_column, dimension_table, dimension_key) pairs to check
FK_CHECKS = [
    ("appointments", "patient_id", "patients", "patient_id"),
    ("appointments", "branch_id", "branches", "branch_id"),
    ("appointments", "consulted_doctor_id", "doctors", "doctor_id"),
    ("billing", "patient_id", "patients", "patient_id"),
    ("billing", "branch_id", "branches", "branch_id"),
    ("lab_reports", "patient_id", "patients", "patient_id"),
    ("lab_reports", "branch_id", "branches", "branch_id"),
    ("prescriptions", "patient_id", "patients", "patient_id"),
    ("prescriptions", "doctor_id", "doctors", "doctor_id"),
    ("surgeries", "patient_id", "patients", "patient_id"),
    ("surgeries", "doctor_id", "doctors", "doctor_id"),
    ("surgeries", "branch_id", "branches", "branch_id"),
]


def run_etl(data_dir: Path, db_url: str, schema_file: Path):
    engine = create_engine(db_url)

    log.info("Applying schema (%s)...", schema_file)
    with engine.begin() as conn:
        for statement in schema_file.read_text().split(";"):
            s = statement.strip()
            if s:
                conn.execute(text(s))

    log.info("Clearing existing rows for a clean, idempotent reload ...")
    is_postgres = engine.dialect.name == "postgresql"
    with engine.begin() as conn:
        for table_name in TABLES:
            truncate_sql = f"TRUNCATE TABLE clinic.{table_name}"
            if is_postgres:
                truncate_sql += " CASCADE"  # no-op today (no FKs defined) but harmless/future-proof on Postgres
            conn.execute(text(truncate_sql))

    loaded = {}
    for table_name, (csv_name, transform_fn) in TABLES.items():
        started = datetime.now()
        src_path = data_dir / csv_name
        log.info("Extracting %s ...", src_path)
        raw = pd.read_csv(src_path)
        rows_extracted = len(raw)

        log.info("Transforming -> %s ...", table_name)
        clean = transform_fn(raw)
        rows_loaded = len(clean)

        log.info("Loading -> clinic.%s (%d rows) ...", table_name, rows_loaded)
        clean.to_sql(
            table_name, engine, schema="clinic",
            if_exists="append", index=False, method="multi", chunksize=1000,
        )
        loaded[table_name] = clean

        finished = datetime.now()
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO clinic.etl_load_log
                        (table_name, rows_extracted, rows_loaded, started_at, finished_at, status, notes)
                    VALUES (:t, :re, :rl, :s, :f, 'SUCCESS', :n)
                """),
                {
                    "t": table_name, "re": rows_extracted, "rl": rows_loaded,
                    "s": started, "f": finished,
                    "n": f"loaded from {csv_name}",
                },
            )
        log.info("Done: %s (%d -> %d rows) in %.2fs", table_name, rows_extracted, rows_loaded,
                  (finished - started).total_seconds())

    log.info("Running referential-integrity / data-quality checks ...")
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM clinic.etl_data_quality_log"))
        for fact_table, fk_col, dim_table, dim_key in FK_CHECKS:
            total = int(loaded[fact_table][fk_col].notna().sum())
            matched = int(loaded[fact_table][fk_col].isin(loaded[dim_table][dim_key]).sum())
            rate = round(100.0 * matched / total, 2) if total else 0.0
            conn.execute(
                text("""
                    INSERT INTO clinic.etl_data_quality_log
                        (fact_table, fk_column, references_table, total_rows, matched_rows, match_rate_pct)
                    VALUES (:ft, :fk, :rt, :tot, :m, :rate)
                """),
                {"ft": fact_table, "fk": fk_col, "rt": dim_table, "tot": int(total),
                 "m": int(matched), "rate": rate},
            )
            log.info("  %s.%s -> %s.%s : %s / %s matched (%.2f%%)",
                      fact_table, fk_col, dim_table, dim_key, matched, total, rate)

    log.info("ETL run complete.")


def main():
    parser = argparse.ArgumentParser(description="Cleveland Clinics ETL pipeline")
    parser.add_argument("--data-dir", default="./data", help="Folder containing the source CSV files")
    parser.add_argument(
        "--db-url",
        default="postgresql+psycopg2://postgres:postgres@localhost:5432/cleveland_clinics_oh",
        help="SQLAlchemy database URL (Postgres by default; see README for MySQL)",
    )
    parser.add_argument("--schema-file", default="./schema.sql", help="DDL file to apply before loading")
    args = parser.parse_args()

    run_etl(Path(args.data_dir), args.db_url, Path(args.schema_file))


if __name__ == "__main__":
    main()
