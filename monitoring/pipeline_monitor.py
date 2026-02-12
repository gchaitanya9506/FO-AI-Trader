# pipeline_monitor.py
"""
Pipeline Health Monitor for NSE Option Chain Automation
Provides data freshness checks, API monitoring, quality validation, and alerts.
"""

import os
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import pandas as pd

from config.logging_config import get_logger
from config.app_config import get_config


class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class HealthCheck:
    """Represents a health check result."""
    name: str
    status: HealthStatus
    message: str
    timestamp: datetime
    details: Optional[Dict[str, Any]] = None
    metric_value: Optional[float] = None
    threshold: Optional[float] = None


@dataclass
class PipelineHealth:
    """Overall pipeline health assessment."""
    overall_status: HealthStatus
    timestamp: datetime
    checks: List[HealthCheck]
    summary: str
    uptime_hours: float
    last_successful_fetch: Optional[datetime] = None


class PipelineMonitor:
    """
    Comprehensive pipeline health monitoring system.
    Monitors data freshness, API health, data quality, and system performance.
    """

    def __init__(self):
        self.config = get_config()
        self.logger = get_logger('monitor')
        self.start_time = datetime.now()
        self.monitoring_enabled = True

        # Health thresholds from config
        self.data_freshness_warning_hours = 2.0  # Warn if data is >2 hours old
        self.data_freshness_critical_hours = 6.0  # Critical if data is >6 hours old
        self.api_timeout_seconds = 10
        self.min_expected_records = 100  # Minimum expected option chain records

    def run_all_health_checks(self) -> PipelineHealth:
        """
        Run all health checks and return comprehensive health assessment.
        """
        self.logger.info("🏥 Running comprehensive pipeline health checks...")

        checks = []

        # Data freshness checks
        checks.extend(self._check_data_freshness())

        # API availability checks
        checks.extend(self._check_api_availability())

        # Database health checks
        checks.extend(self._check_database_health())

        # Data quality checks
        checks.extend(self._check_data_quality())

        # System performance checks
        checks.extend(self._check_system_performance())

        # Determine overall status
        overall_status = self._determine_overall_status(checks)

        # Calculate uptime
        uptime_hours = (datetime.now() - self.start_time).total_seconds() / 3600

        # Get last successful fetch time
        last_successful_fetch = self._get_last_successful_fetch()

        # Generate summary
        summary = self._generate_health_summary(overall_status, checks)

        health_assessment = PipelineHealth(
            overall_status=overall_status,
            timestamp=datetime.now(),
            checks=checks,
            summary=summary,
            uptime_hours=uptime_hours,
            last_successful_fetch=last_successful_fetch
        )

        self._log_health_assessment(health_assessment)
        return health_assessment

    def _check_data_freshness(self) -> List[HealthCheck]:
        """Check if data is fresh and up-to-date."""
        checks = []

        try:
            from database.db_manager import get_latest_option_chain_data

            # Check option chain data freshness
            latest_data = get_latest_option_chain_data("NIFTY", limit=1)

            if latest_data.empty:
                checks.append(HealthCheck(
                    name="Option Chain Data Freshness",
                    status=HealthStatus.CRITICAL,
                    message="No option chain data found in database",
                    timestamp=datetime.now(),
                    details={"records_found": 0}
                ))
            else:
                last_update = pd.to_datetime(latest_data.iloc[0]['created_at'])
                hours_since_update = (datetime.now() - last_update).total_seconds() / 3600

                if hours_since_update > self.data_freshness_critical_hours:
                    status = HealthStatus.CRITICAL
                    message = f"Option chain data is critically stale ({hours_since_update:.1f} hours old)"
                elif hours_since_update > self.data_freshness_warning_hours:
                    status = HealthStatus.WARNING
                    message = f"Option chain data is getting stale ({hours_since_update:.1f} hours old)"
                else:
                    status = HealthStatus.HEALTHY
                    message = f"Option chain data is fresh ({hours_since_update:.1f} hours old)"

                checks.append(HealthCheck(
                    name="Option Chain Data Freshness",
                    status=status,
                    message=message,
                    timestamp=datetime.now(),
                    metric_value=hours_since_update,
                    threshold=self.data_freshness_warning_hours,
                    details={
                        "last_update": last_update.isoformat(),
                        "hours_since_update": hours_since_update
                    }
                ))

            # Check NIFTY underlying data freshness
            nifty_file_path = "data/raw/nifty_data_5m.csv"
            if os.path.exists(nifty_file_path):
                file_modified = datetime.fromtimestamp(os.path.getmtime(nifty_file_path))
                hours_since_nifty_update = (datetime.now() - file_modified).total_seconds() / 3600

                if hours_since_nifty_update > self.data_freshness_critical_hours:
                    status = HealthStatus.CRITICAL
                    message = f"NIFTY data is critically stale ({hours_since_nifty_update:.1f} hours old)"
                elif hours_since_nifty_update > self.data_freshness_warning_hours:
                    status = HealthStatus.WARNING
                    message = f"NIFTY data is getting stale ({hours_since_nifty_update:.1f} hours old)"
                else:
                    status = HealthStatus.HEALTHY
                    message = f"NIFTY data is fresh ({hours_since_nifty_update:.1f} hours old)"

                checks.append(HealthCheck(
                    name="NIFTY Data Freshness",
                    status=status,
                    message=message,
                    timestamp=datetime.now(),
                    metric_value=hours_since_nifty_update,
                    threshold=self.data_freshness_warning_hours
                ))
            else:
                checks.append(HealthCheck(
                    name="NIFTY Data Freshness",
                    status=HealthStatus.CRITICAL,
                    message="NIFTY data file not found",
                    timestamp=datetime.now()
                ))

        except Exception as e:
            checks.append(HealthCheck(
                name="Data Freshness Check",
                status=HealthStatus.CRITICAL,
                message=f"Error checking data freshness: {e}",
                timestamp=datetime.now(),
                details={"error": str(e)}
            ))

        return checks

    def _check_api_availability(self) -> List[HealthCheck]:
        """Check if external APIs are available and responsive."""
        checks = []

        # Check NSE API availability
        try:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            session = requests.Session()
            retry_strategy = Retry(
                total=2,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)

            # Test NSE website accessibility
            nse_url = "https://www.nseindia.com"
            start_time = time.time()

            response = session.get(nse_url, timeout=self.api_timeout_seconds)
            response_time = time.time() - start_time

            if response.status_code == 200:
                if response_time > 5.0:
                    status = HealthStatus.WARNING
                    message = f"NSE API responding slowly ({response_time:.1f}s)"
                else:
                    status = HealthStatus.HEALTHY
                    message = f"NSE API responsive ({response_time:.1f}s)"
            else:
                status = HealthStatus.WARNING
                message = f"NSE API returned status {response.status_code}"

            checks.append(HealthCheck(
                name="NSE API Availability",
                status=status,
                message=message,
                timestamp=datetime.now(),
                metric_value=response_time,
                threshold=5.0,
                details={
                    "status_code": response.status_code,
                    "response_time_seconds": response_time
                }
            ))

        except Exception as e:
            checks.append(HealthCheck(
                name="NSE API Availability",
                status=HealthStatus.CRITICAL,
                message=f"Cannot reach NSE API: {e}",
                timestamp=datetime.now(),
                details={"error": str(e)}
            ))

        # Check YFinance API (for NIFTY data)
        try:
            import yfinance as yf

            start_time = time.time()
            ticker = yf.Ticker("^NSEI")
            info = ticker.info
            response_time = time.time() - start_time

            if info and 'regularMarketPrice' in info:
                checks.append(HealthCheck(
                    name="YFinance API Availability",
                    status=HealthStatus.HEALTHY,
                    message=f"YFinance API responsive ({response_time:.1f}s)",
                    timestamp=datetime.now(),
                    metric_value=response_time,
                    details={"market_price": info.get('regularMarketPrice')}
                ))
            else:
                checks.append(HealthCheck(
                    name="YFinance API Availability",
                    status=HealthStatus.WARNING,
                    message="YFinance API returned incomplete data",
                    timestamp=datetime.now(),
                    metric_value=response_time
                ))

        except Exception as e:
            checks.append(HealthCheck(
                name="YFinance API Availability",
                status=HealthStatus.WARNING,
                message=f"YFinance API issue: {e}",
                timestamp=datetime.now(),
                details={"error": str(e)}
            ))

        return checks

    def _check_database_health(self) -> List[HealthCheck]:
        """Check database connectivity and integrity."""
        checks = []

        try:
            from database.db_manager import get_conn, get_latest_option_chain_data

            # Test database connection
            conn = get_conn()
            if conn:
                conn.close()
                checks.append(HealthCheck(
                    name="Database Connectivity",
                    status=HealthStatus.HEALTHY,
                    message="Database connection successful",
                    timestamp=datetime.now()
                ))

                # Check table existence and record counts
                option_data = get_latest_option_chain_data("NIFTY")
                record_count = len(option_data)

                if record_count < self.min_expected_records:
                    status = HealthStatus.WARNING
                    message = f"Low option chain record count: {record_count} (expected: >{self.min_expected_records})"
                else:
                    status = HealthStatus.HEALTHY
                    message = f"Option chain records: {record_count}"

                checks.append(HealthCheck(
                    name="Database Record Count",
                    status=status,
                    message=message,
                    timestamp=datetime.now(),
                    metric_value=record_count,
                    threshold=self.min_expected_records
                ))

            else:
                checks.append(HealthCheck(
                    name="Database Connectivity",
                    status=HealthStatus.CRITICAL,
                    message="Cannot connect to database",
                    timestamp=datetime.now()
                ))

        except Exception as e:
            checks.append(HealthCheck(
                name="Database Health",
                status=HealthStatus.CRITICAL,
                message=f"Database error: {e}",
                timestamp=datetime.now(),
                details={"error": str(e)}
            ))

        return checks

    def _check_data_quality(self) -> List[HealthCheck]:
        """Check data quality and consistency."""
        checks = []

        try:
            from database.db_manager import get_latest_option_chain_data

            # Get recent option chain data
            recent_data = get_latest_option_chain_data("NIFTY", limit=1000)

            if not recent_data.empty:
                # Check for null values in critical columns
                critical_columns = ['strike_price', 'option_type', 'last_price']
                null_counts = {}

                for col in critical_columns:
                    if col in recent_data.columns:
                        null_count = recent_data[col].isnull().sum()
                        null_counts[col] = null_count

                total_nulls = sum(null_counts.values())
                null_percentage = (total_nulls / (len(recent_data) * len(critical_columns))) * 100

                if null_percentage > 10:
                    status = HealthStatus.CRITICAL
                    message = f"High null value percentage: {null_percentage:.1f}%"
                elif null_percentage > 5:
                    status = HealthStatus.WARNING
                    message = f"Moderate null value percentage: {null_percentage:.1f}%"
                else:
                    status = HealthStatus.HEALTHY
                    message = f"Data quality good: {null_percentage:.1f}% null values"

                checks.append(HealthCheck(
                    name="Data Quality - Null Values",
                    status=status,
                    message=message,
                    timestamp=datetime.now(),
                    metric_value=null_percentage,
                    threshold=5.0,
                    details=null_counts
                ))

                # Check for reasonable value ranges
                if 'last_price' in recent_data.columns:
                    price_data = recent_data['last_price'].dropna()
                    if not price_data.empty:
                        negative_prices = (price_data < 0).sum()
                        extremely_high_prices = (price_data > 10000).sum()  # Arbitrary high threshold

                        quality_issues = negative_prices + extremely_high_prices
                        quality_percentage = (quality_issues / len(price_data)) * 100

                        if quality_percentage > 5:
                            status = HealthStatus.WARNING
                            message = f"Data quality concerns: {quality_percentage:.1f}% suspicious prices"
                        else:
                            status = HealthStatus.HEALTHY
                            message = f"Price data quality good: {quality_percentage:.1f}% outliers"

                        checks.append(HealthCheck(
                            name="Data Quality - Price Values",
                            status=status,
                            message=message,
                            timestamp=datetime.now(),
                            metric_value=quality_percentage,
                            details={
                                "negative_prices": int(negative_prices),
                                "extremely_high_prices": int(extremely_high_prices)
                            }
                        ))

        except Exception as e:
            checks.append(HealthCheck(
                name="Data Quality Check",
                status=HealthStatus.WARNING,
                message=f"Error checking data quality: {e}",
                timestamp=datetime.now(),
                details={"error": str(e)}
            ))

        return checks

    def _check_system_performance(self) -> List[HealthCheck]:
        """Check system performance metrics."""
        checks = []

        try:
            import psutil

            # Check memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent

            if memory_percent > 90:
                status = HealthStatus.CRITICAL
                message = f"Critical memory usage: {memory_percent:.1f}%"
            elif memory_percent > 75:
                status = HealthStatus.WARNING
                message = f"High memory usage: {memory_percent:.1f}%"
            else:
                status = HealthStatus.HEALTHY
                message = f"Memory usage normal: {memory_percent:.1f}%"

            checks.append(HealthCheck(
                name="Memory Usage",
                status=status,
                message=message,
                timestamp=datetime.now(),
                metric_value=memory_percent,
                threshold=75.0
            ))

            # Check disk space
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100

            if disk_percent > 95:
                status = HealthStatus.CRITICAL
                message = f"Critical disk usage: {disk_percent:.1f}%"
            elif disk_percent > 85:
                status = HealthStatus.WARNING
                message = f"High disk usage: {disk_percent:.1f}%"
            else:
                status = HealthStatus.HEALTHY
                message = f"Disk usage normal: {disk_percent:.1f}%"

            checks.append(HealthCheck(
                name="Disk Usage",
                status=status,
                message=message,
                timestamp=datetime.now(),
                metric_value=disk_percent,
                threshold=85.0
            ))

        except ImportError:
            checks.append(HealthCheck(
                name="System Performance",
                status=HealthStatus.UNKNOWN,
                message="psutil not available - install with: pip install psutil",
                timestamp=datetime.now()
            ))
        except Exception as e:
            checks.append(HealthCheck(
                name="System Performance",
                status=HealthStatus.WARNING,
                message=f"Error checking system performance: {e}",
                timestamp=datetime.now(),
                details={"error": str(e)}
            ))

        return checks

    def _determine_overall_status(self, checks: List[HealthCheck]) -> HealthStatus:
        """Determine overall health status based on individual checks."""
        if not checks:
            return HealthStatus.UNKNOWN

        statuses = [check.status for check in checks]

        if HealthStatus.CRITICAL in statuses:
            return HealthStatus.CRITICAL
        elif HealthStatus.WARNING in statuses:
            return HealthStatus.WARNING
        elif any(status == HealthStatus.HEALTHY for status in statuses):
            return HealthStatus.HEALTHY
        else:
            return HealthStatus.UNKNOWN

    def _generate_health_summary(self, overall_status: HealthStatus, checks: List[HealthCheck]) -> str:
        """Generate a human-readable health summary."""
        total_checks = len(checks)
        healthy_count = sum(1 for c in checks if c.status == HealthStatus.HEALTHY)
        warning_count = sum(1 for c in checks if c.status == HealthStatus.WARNING)
        critical_count = sum(1 for c in checks if c.status == HealthStatus.CRITICAL)

        summary = f"Pipeline Status: {overall_status.value.upper()} | "
        summary += f"Checks: {healthy_count} healthy, {warning_count} warnings, {critical_count} critical"

        if overall_status == HealthStatus.CRITICAL:
            critical_issues = [c.name for c in checks if c.status == HealthStatus.CRITICAL]
            summary += f" | Critical issues: {', '.join(critical_issues)}"

        return summary

    def _get_last_successful_fetch(self) -> Optional[datetime]:
        """Get the timestamp of the last successful data fetch."""
        try:
            from database.db_manager import get_latest_option_chain_data

            latest_data = get_latest_option_chain_data("NIFTY", limit=1)
            if not latest_data.empty:
                return pd.to_datetime(latest_data.iloc[0]['created_at'])
        except Exception:
            pass

        return None

    def _log_health_assessment(self, health: PipelineHealth):
        """Log the health assessment results."""
        status_emoji = {
            HealthStatus.HEALTHY: "✅",
            HealthStatus.WARNING: "⚠️",
            HealthStatus.CRITICAL: "🚨",
            HealthStatus.UNKNOWN: "❓"
        }

        emoji = status_emoji.get(health.overall_status, "❓")
        self.logger.info(f"{emoji} {health.summary}")

        # Log details for non-healthy statuses
        if health.overall_status != HealthStatus.HEALTHY:
            for check in health.checks:
                if check.status in [HealthStatus.WARNING, HealthStatus.CRITICAL]:
                    check_emoji = status_emoji.get(check.status, "❓")
                    self.logger.warning(f"{check_emoji} {check.name}: {check.message}")

    def export_health_report(self, health: PipelineHealth, format: str = "json") -> str:
        """Export health report to specified format."""
        timestamp_str = health.timestamp.strftime("%Y%m%d_%H%M%S")

        if format.lower() == "json":
            # Convert to JSON-serializable format
            report_data = {
                "overall_status": health.overall_status.value,
                "timestamp": health.timestamp.isoformat(),
                "summary": health.summary,
                "uptime_hours": health.uptime_hours,
                "last_successful_fetch": health.last_successful_fetch.isoformat() if health.last_successful_fetch else None,
                "checks": [
                    {
                        "name": check.name,
                        "status": check.status.value,
                        "message": check.message,
                        "timestamp": check.timestamp.isoformat(),
                        "metric_value": check.metric_value,
                        "threshold": check.threshold,
                        "details": check.details
                    }
                    for check in health.checks
                ]
            }

            filename = f"health_report_{timestamp_str}.json"
            filepath = f"logs/{filename}"

            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, 'w') as f:
                json.dump(report_data, f, indent=2)

            self.logger.info(f"Health report exported to: {filepath}")
            return filepath

        else:
            raise ValueError(f"Unsupported format: {format}")


# Convenience functions
def run_health_check() -> PipelineHealth:
    """Convenience function to run a complete health check."""
    monitor = PipelineMonitor()
    return monitor.run_all_health_checks()


def get_pipeline_monitor() -> PipelineMonitor:
    """Get a configured pipeline monitor instance."""
    return PipelineMonitor()


if __name__ == "__main__":
    # Test the health monitoring system
    print("🏥 Running pipeline health check...")

    monitor = PipelineMonitor()
    health_assessment = monitor.run_all_health_checks()

    print(f"\n📊 Health Assessment Results:")
    print(f"Overall Status: {health_assessment.overall_status.value.upper()}")
    print(f"Summary: {health_assessment.summary}")
    print(f"Uptime: {health_assessment.uptime_hours:.1f} hours")

    print(f"\n🔍 Individual Checks:")
    for check in health_assessment.checks:
        status_icon = {"healthy": "✅", "warning": "⚠️", "critical": "🚨", "unknown": "❓"}
        icon = status_icon.get(check.status.value, "❓")
        print(f"{icon} {check.name}: {check.message}")

    # Export report
    report_path = monitor.export_health_report(health_assessment)
    print(f"\n📄 Detailed report saved to: {report_path}")