# deploy_automation.py
"""
Deployment Script for NSE Option Chain Automation
Sets up and starts the fully automated pipeline system.
"""

import os
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path

from config.logging_config import get_logger

logger = get_logger('deploy')


def check_dependencies():
    """Check if all required dependencies are installed."""
    logger.info("🔍 Checking dependencies...")

    required_packages = [
        'pandas', 'numpy', 'requests', 'yfinance', 'schedule', 'pytz', 'psutil'
    ]

    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
            logger.info(f"  ✅ {package}")
        except ImportError:
            logger.error(f"  ❌ {package}")
            missing_packages.append(package)

    # Check optional NSE packages
    nse_packages = ['nsepython']
    nse_available = False
    for package in nse_packages:
        try:
            __import__(package)
            logger.info(f"  ✅ {package} (NSE data source)")
            nse_available = True
            break
        except ImportError:
            logger.warning(f"  ⚠️ {package} (optional NSE data source)")

    if missing_packages:
        logger.error(f"Missing required packages: {missing_packages}")
        logger.info("Install with: pip install " + " ".join(missing_packages))
        return False

    if not nse_available:
        logger.warning("No NSE data source available. Install nsepython: pip install nsepython")
        return False

    logger.info("✅ All dependencies satisfied")
    return True


def setup_directories():
    """Create necessary directories."""
    logger.info("📁 Setting up directories...")

    directories = [
        "data/raw",
        "data/processed",
        "logs",
        "monitoring"
    ]

    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        logger.info(f"  ✅ {directory}")

    return True


def run_pipeline_tests():
    """Run the pipeline validation tests."""
    logger.info("🧪 Running pipeline validation tests...")

    try:
        # Run the test suite
        result = subprocess.run([
            sys.executable, "test_automated_pipeline.py"
        ], capture_output=True, text=True, timeout=120)

        if result.returncode == 0:
            logger.info("✅ Pipeline tests passed")
            return True
        else:
            logger.error("❌ Pipeline tests failed")
            logger.error(result.stdout)
            logger.error(result.stderr)
            return False

    except subprocess.TimeoutExpired:
        logger.error("❌ Pipeline tests timed out")
        return False
    except Exception as e:
        logger.error(f"❌ Error running pipeline tests: {e}")
        return False


def initialize_database():
    """Initialize the database schema."""
    logger.info("🗄️ Initializing database...")

    try:
        from database.db_manager import init_option_chain_table, get_conn

        # Test database connection
        conn = get_conn()
        if conn:
            conn.close()
            logger.info("  ✅ Database connection successful")
        else:
            logger.error("  ❌ Database connection failed")
            return False

        # Initialize tables
        success = init_option_chain_table()
        if success:
            logger.info("  ✅ Database tables initialized")
            return True
        else:
            logger.error("  ❌ Database table initialization failed")
            return False

    except Exception as e:
        logger.error(f"❌ Database initialization error: {e}")
        return False


def start_automation_system():
    """Start the automated pipeline system."""
    logger.info("🚀 Starting automation system...")

    try:
        from data_ingestion.automated_scheduler import AutomatedScheduler

        # Create and configure scheduler
        scheduler = AutomatedScheduler()

        logger.info("📅 Automation system is now running...")
        logger.info("🔍 Monitor logs for pipeline activity")
        logger.info("🛑 Press Ctrl+C to stop")

        # Start the scheduler
        scheduler.run()

    except KeyboardInterrupt:
        logger.info("🛑 Automation system stopped by user")
        return True
    except Exception as e:
        logger.error(f"❌ Error starting automation system: {e}")
        return False


def main():
    """Main deployment function."""
    print("🚀 NSE Option Chain Automation Deployment")
    print("=" * 50)

    start_time = datetime.now()

    # Step 1: Check dependencies
    if not check_dependencies():
        print("❌ Dependency check failed. Please install missing packages.")
        return 1

    # Step 2: Setup directories
    if not setup_directories():
        print("❌ Directory setup failed.")
        return 1

    # Step 3: Initialize database
    if not initialize_database():
        print("❌ Database initialization failed.")
        return 1

    # Step 4: Run validation tests
    print("\n🧪 Running pipeline validation tests...")
    if not run_pipeline_tests():
        print("❌ Pipeline validation failed. Check logs for details.")
        response = input("\nContinue anyway? (y/N): ")
        if response.lower() != 'y':
            return 1

    # Step 5: Show deployment summary
    duration = (datetime.now() - start_time).total_seconds()
    print(f"\n✅ Deployment completed in {duration:.2f} seconds")
    print("\n🎉 NSE Option Chain Automation is ready!")
    print("\nThe system will now:")
    print("  📈 Automatically fetch NSE option chain data during market hours")
    print("  🧹 Clean and process data without manual intervention")
    print("  💾 Store data directly in the database")
    print("  📊 Generate technical indicators and Greeks")
    print("  🏥 Monitor pipeline health and data quality")
    print("  ⚠️ Alert on any issues or stale data")

    # Step 6: Start automation system
    response = input("\nStart the automation system now? (Y/n): ")
    if response.lower() != 'n':
        print("\n🚀 Starting automation system...")
        start_automation_system()
    else:
        print("\n📝 To start the automation system later, run:")
        print("   python data_ingestion/automated_scheduler.py")
        print("\n📊 To monitor pipeline health, run:")
        print("   python monitoring/pipeline_monitor.py")

    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)