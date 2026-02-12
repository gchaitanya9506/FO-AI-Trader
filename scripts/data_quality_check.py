#!/usr/bin/env python3
"""
Data quality monitoring script for F&O AI Trader.
Runs comprehensive quality checks on stored data.
"""
import os
import sys
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Dict

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.logging_config import get_logger
from database.db_manager import load_table
from utils.data_validation import (
    clean_and_validate_data,
    validate_expiry_dates,
    DataQualityReport
)

logger = get_logger('data_quality')


def check_file_data_quality(file_path: str, data_type: str) -> DataQualityReport:
    """Check data quality for a specific file."""
    report = DataQualityReport()

    if not os.path.exists(file_path):
        report.add_error(f"File not found: {file_path}")
        return report

    try:
        df = pd.read_csv(file_path)
        logger.info(f"Checking data quality for {file_path}")

        df_clean, validation_report = clean_and_validate_data(df, data_type)

        report.warnings.extend(validation_report.warnings)
        report.errors.extend(validation_report.errors)
        report.metrics.update(validation_report.metrics)

        # Additional file-specific checks
        file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(file_path))
        report.add_metric("file_age_hours", round(file_age.total_seconds() / 3600, 1))

        if file_age.days > 1:
            report.add_warning(f"File is {file_age.days} days old")

        report.add_metric("file_size_mb", round(os.path.getsize(file_path) / (1024 * 1024), 2))

    except Exception as e:
        report.add_error(f"Failed to read or validate file {file_path}: {e}")

    return report


def check_database_data_quality(table_name: str, data_type: str) -> DataQualityReport:
    """Check data quality for a database table."""
    report = DataQualityReport()

    try:
        df = load_table(table_name)
        logger.info(f"Checking data quality for database table: {table_name}")

        df_clean, validation_report = clean_and_validate_data(df, data_type)

        report.warnings.extend(validation_report.warnings)
        report.errors.extend(validation_report.errors)
        report.metrics.update(validation_report.metrics)

        # Additional database-specific checks
        if "Datetime" in df.columns or "Date" in df.columns:
            date_col = "Datetime" if "Datetime" in df.columns else "Date"
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

            # Check data freshness
            latest_date = df[date_col].max()
            if pd.notna(latest_date):
                data_age = datetime.now() - latest_date.to_pydatetime()
                report.add_metric("data_age_hours", round(data_age.total_seconds() / 3600, 1))

                if data_age.days > 1:
                    report.add_warning(f"Latest data is {data_age.days} days old")

            # Check for data gaps
            df_sorted = df.sort_values(date_col)
            if len(df_sorted) > 1:
                date_diffs = df_sorted[date_col].diff().dropna()
                median_interval = date_diffs.median()
                large_gaps = date_diffs > median_interval * 3

                if large_gaps.any():
                    gap_count = large_gaps.sum()
                    report.add_warning(f"Found {gap_count} large time gaps in data")

    except Exception as e:
        report.add_error(f"Failed to check database table {table_name}: {e}")

    return report


def run_comprehensive_data_quality_check() -> Dict[str, DataQualityReport]:
    """Run comprehensive data quality checks across the system."""
    logger.info("Starting comprehensive data quality check")

    reports = {}

    # Check raw data files
    raw_data_files = [
        ("data/raw/nifty_data_5m.csv", "price"),
        ("data/raw/nifty_data_15m.csv", "price"),
        ("data/raw/nifty_data_30m.csv", "price"),
        ("data/raw/nifty_option_chain.csv", "option"),
        ("data/raw/nifty_option_chain_clean.csv", "option"),
    ]

    for file_path, data_type in raw_data_files:
        report_key = f"file_{os.path.basename(file_path)}"
        reports[report_key] = check_file_data_quality(file_path, data_type)

    # Check processed data files
    processed_data_files = [
        ("data/processed/nifty_option_features.csv", "option"),
    ]

    for file_path, data_type in processed_data_files:
        report_key = f"file_{os.path.basename(file_path)}"
        reports[report_key] = check_file_data_quality(file_path, data_type)

    # Check database tables
    database_tables = [
        ("underlying_features", "price"),
        ("option_chain_features", "option"),
    ]

    for table_name, data_type in database_tables:
        report_key = f"table_{table_name}"
        try:
            reports[report_key] = check_database_data_quality(table_name, data_type)
        except Exception as e:
            logger.warning(f"Skipping table {table_name} - may not exist yet: {e}")

    return reports


def generate_quality_summary(reports: Dict[str, DataQualityReport]) -> str:
    """Generate a summary of all quality reports."""
    total_checks = len(reports)
    passed_checks = sum(1 for report in reports.values() if report.passed)
    failed_checks = total_checks - passed_checks

    total_warnings = sum(len(report.warnings) for report in reports.values())
    total_errors = sum(len(report.errors) for report in reports.values())

    summary = f"""
=== DATA QUALITY SUMMARY ===
Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Overall Status: {'PASSED' if failed_checks == 0 else 'FAILED'}
Total Checks: {total_checks}
Passed: {passed_checks}
Failed: {failed_checks}
Total Warnings: {total_warnings}
Total Errors: {total_errors}

=== DETAILED REPORTS ===
"""

    for name, report in reports.items():
        summary += f"\n--- {name} ---\n"
        summary += report.summary()
        summary += "\n"

    return summary


def main():
    """Main function to run data quality checks."""
    try:
        # Run comprehensive checks
        reports = run_comprehensive_data_quality_check()

        # Generate summary
        summary = generate_quality_summary(reports)

        # Log summary
        logger.info(f"Data quality check completed:\n{summary}")

        # Save summary to file
        os.makedirs("logs", exist_ok=True)
        summary_file = f"logs/data_quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(summary_file, 'w') as f:
            f.write(summary)

        logger.info(f"Detailed quality report saved to {summary_file}")

        # Exit with error code if any checks failed
        failed_reports = [name for name, report in reports.items() if not report.passed]
        if failed_reports:
            logger.error(f"Data quality checks failed for: {failed_reports}")
            sys.exit(1)
        else:
            logger.info("All data quality checks passed!")
            sys.exit(0)

    except Exception as e:
        logger.error(f"Data quality check failed with exception: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()