#!/usr/bin/env python3
"""
report_generator.py
=====================
Cleveland Clinics, OH — Automated Report Generation (Task 2)

Connects to the warehouse loaded by etl_pipeline.py and generates a
report covering:
    1. Patient visits (appointments)
    2. Revenue (billing)
    3. Most common diagnoses

Designed to be run on a schedule (see README for cron / Task Scheduler /
systemd timer examples) so it can act as a genuine "daily report" job.

Two modes:
    --date YYYY-MM-DD   Report scoped to a single day (the normal daily-run mode).
    --summary           Report scoped to the entire loaded dataset (useful for
                         historical/demo data where "today" has no rows —
                         which is the case for this project's synthetic data).

Output: a timestamped .txt report written to --output-dir (default ./sample_output),
and the same content printed to stdout / the log for whatever's supervising the job
(cron mail, CloudWatch Logs, etc).
"""

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("report")


def fetch_df(engine, sql: str, params: dict = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def build_report(engine, report_date: date | None) -> str:
    scope_clause = "WHERE date_of_consultation = :d" if report_date else ""
    billing_scope_clause = (
        "WHERE case_id IN (SELECT case_id FROM clinic.appointments WHERE date_of_consultation = :d)"
        if report_date else ""
    )
    params = {"d": report_date} if report_date else {}

    # 1. Patient visits
    visits_total = fetch_df(
        engine,
        f"SELECT COUNT(*) AS total_visits FROM clinic.appointments {scope_clause}",
        params,
    )["total_visits"][0]

    visits_by_branch = fetch_df(
        engine,
        f"""
        SELECT branch_id, COUNT(*) AS visits
        FROM clinic.appointments
        {scope_clause}
        GROUP BY branch_id
        ORDER BY visits DESC
        LIMIT 10
        """,
        params,
    )

    followups = fetch_df(
        engine,
        f"""
        SELECT
            SUM(CASE WHEN followup_required THEN 1 ELSE 0 END) AS followups_required,
            COUNT(*) AS total
        FROM clinic.appointments
        {scope_clause}
        """,
        params,
    )

    # 2. Revenue
    revenue = fetch_df(
        engine,
        f"""
        SELECT
            COUNT(*) AS num_claims,
            COALESCE(SUM(total_bill), 0) AS total_billed,
            COALESCE(SUM(amount_covered), 0) AS total_covered,
            COALESCE(SUM(out_of_pocket_expenses), 0) AS total_out_of_pocket
        FROM clinic.billing
        {billing_scope_clause}
        """,
        params,
    )

    revenue_by_status = fetch_df(
        engine,
        f"""
        SELECT claim_status, COUNT(*) AS num_claims, COALESCE(SUM(total_bill), 0) AS total_billed
        FROM clinic.billing
        {billing_scope_clause}
        GROUP BY claim_status
        ORDER BY total_billed DESC
        """,
        params,
    )

    # 3. Common diagnoses
    top_diagnoses = fetch_df(
        engine,
        f"""
        SELECT diagnosis, COUNT(*) AS num_cases
        FROM clinic.appointments
        {scope_clause}
        GROUP BY diagnosis
        ORDER BY num_cases DESC
        LIMIT 10
        """,
        params,
    )

    # ---------------- Render as plain text ----------------
    lines = []
    scope_label = report_date.isoformat() if report_date else "ALL LOADED DATA (summary)"
    lines.append("=" * 70)
    lines.append("CLEVELAND CLINICS, OH — DAILY OPERATIONS REPORT")
    lines.append(f"Report scope : {scope_label}")
    lines.append(f"Generated at : {datetime.now().isoformat(timespec='seconds')}")
    lines.append("=" * 70)

    lines.append("\n1. PATIENT VISITS")
    lines.append("-" * 40)
    lines.append(f"Total appointments: {visits_total}")
    if not followups.empty and followups['total'][0]:
        lines.append(f"Follow-ups required: {int(followups['followups_required'][0])} of {followups['total'][0]}")
    lines.append("\nTop branches by visits:")
    if visits_by_branch.empty:
        lines.append("  (no appointments in this scope)")
    else:
        for _, r in visits_by_branch.iterrows():
            lines.append(f"  {r['branch_id']:<10} {r['visits']} visits")

    lines.append("\n2. REVENUE")
    lines.append("-" * 40)
    if revenue.empty or revenue["num_claims"][0] == 0:
        lines.append("  (no billing records in this scope)")
    else:
        r = revenue.iloc[0]
        lines.append(f"Claims processed        : {int(r['num_claims'])}")
        lines.append(f"Total billed            : ${r['total_billed']:,.2f}")
        lines.append(f"Total covered by ins.   : ${r['total_covered']:,.2f}")
        lines.append(f"Total out-of-pocket     : ${r['total_out_of_pocket']:,.2f}")
        lines.append("\nBy claim status:")
        for _, row in revenue_by_status.iterrows():
            lines.append(f"  {row['claim_status']:<10} {int(row['num_claims'])} claims  ${row['total_billed']:,.2f}")

    lines.append("\n3. MOST COMMON DIAGNOSES")
    lines.append("-" * 40)
    if top_diagnoses.empty:
        lines.append("  (no appointments in this scope)")
    else:
        for _, r in top_diagnoses.iterrows():
            lines.append(f"  {r['diagnosis']:<20} {r['num_cases']} cases")

    lines.append("\n" + "=" * 70)
    lines.append("End of report.")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Cleveland Clinics daily report generator")
    parser.add_argument("--date", default=None, help="Report date, YYYY-MM-DD (single-day mode)")
    parser.add_argument("--summary", action="store_true", help="Summary mode: report over all loaded data")
    parser.add_argument(
        "--db-url",
        default="postgresql+psycopg2://postgres:postgres@localhost:5432/cleveland_clinics_oh",
        help="SQLAlchemy database URL",
    )
    parser.add_argument("--output-dir", default="./sample_output", help="Folder to write the report file into")
    args = parser.parse_args()

    if not args.summary and not args.date:
        args.date = date.today().isoformat()

    report_date = None if args.summary else datetime.strptime(args.date, "%Y-%m-%d").date()

    engine = create_engine(args.db_url)
    log.info("Generating report (scope=%s) ...", "summary" if args.summary else report_date)
    report_text = build_report(engine, report_date)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    scope_tag = "summary" if args.summary else report_date.isoformat()
    out_file = out_dir / f"daily_report_{scope_tag}_{stamp}.txt"
    out_file.write_text(report_text)

    print(report_text)
    log.info("Report written to %s", out_file)


if __name__ == "__main__":
    main()
