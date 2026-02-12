#!/usr/bin/env python3
"""
Test runner for F&O AI Trader.
Runs all tests and generates reports.
"""
import unittest
import sys
import os
import coverage
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.logging_config import get_logger

logger = get_logger('test_runner')


def discover_and_run_tests(test_dir='tests', pattern='test_*.py', verbosity=2):
    """
    Discover and run all tests in the test directory.

    Args:
        test_dir: Directory containing test files
        pattern: Pattern to match test files
        verbosity: Test output verbosity (0-2)

    Returns:
        TestResult object
    """
    logger.info(f"Discovering tests in {test_dir} with pattern {pattern}")

    # Discover all test files
    loader = unittest.TestLoader()
    test_suite = loader.discover(test_dir, pattern=pattern)

    # Count total tests
    test_count = test_suite.countTestCases()
    logger.info(f"Found {test_count} test cases")

    # Run tests
    runner = unittest.TextTestRunner(verbosity=verbosity, stream=sys.stdout, buffer=True)
    result = runner.run(test_suite)

    return result


def run_tests_with_coverage(test_dir='tests', pattern='test_*.py'):
    """
    Run tests with coverage analysis.

    Args:
        test_dir: Directory containing test files
        pattern: Pattern to match test files

    Returns:
        Tuple of (TestResult, Coverage)
    """
    logger.info("Starting test run with coverage analysis")

    # Initialize coverage
    cov = coverage.Coverage(source=['.'])
    cov.start()

    try:
        # Run tests
        result = discover_and_run_tests(test_dir, pattern)

        # Stop coverage
        cov.stop()
        cov.save()

        return result, cov

    except Exception as e:
        cov.stop()
        logger.error(f"Error during test execution: {e}")
        raise


def generate_test_report(result, cov=None, output_dir='test_reports'):
    """
    Generate test reports.

    Args:
        result: TestResult object
        cov: Coverage object (optional)
        output_dir: Directory to save reports
    """
    logger.info("Generating test reports")

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Generate summary report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    summary_file = os.path.join(output_dir, f'test_summary_{timestamp}.txt')

    with open(summary_file, 'w') as f:
        f.write(f"F&O AI Trader Test Report\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 50 + "\n\n")

        # Test summary
        f.write(f"Test Summary:\n")
        f.write(f"  Tests Run: {result.testsRun}\n")
        f.write(f"  Failures: {len(result.failures)}\n")
        f.write(f"  Errors: {len(result.errors)}\n")
        f.write(f"  Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}\n")
        f.write(f"  Success Rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%\n\n")

        # Failures
        if result.failures:
            f.write("FAILURES:\n")
            f.write("-" * 20 + "\n")
            for test, traceback in result.failures:
                f.write(f"Test: {test}\n")
                f.write(f"Traceback:\n{traceback}\n\n")

        # Errors
        if result.errors:
            f.write("ERRORS:\n")
            f.write("-" * 20 + "\n")
            for test, traceback in result.errors:
                f.write(f"Test: {test}\n")
                f.write(f"Traceback:\n{traceback}\n\n")

        # Coverage report
        if cov:
            f.write("COVERAGE REPORT:\n")
            f.write("-" * 20 + "\n")
            import io
            coverage_output = io.StringIO()
            cov.report(file=coverage_output)
            f.write(coverage_output.getvalue())

    logger.info(f"Test summary saved to: {summary_file}")

    # Generate HTML coverage report if coverage is available
    if cov:
        html_dir = os.path.join(output_dir, f'coverage_html_{timestamp}')
        cov.html_report(directory=html_dir)
        logger.info(f"HTML coverage report saved to: {html_dir}")

    return summary_file


def run_specific_test_module(module_name, verbosity=2):
    """
    Run tests from a specific module.

    Args:
        module_name: Name of the test module (without .py extension)
        verbosity: Test output verbosity

    Returns:
        TestResult object
    """
    logger.info(f"Running specific test module: {module_name}")

    # Import and run specific module
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromName(module_name)

    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)

    return result


def main():
    """Main function for test runner."""
    import argparse

    parser = argparse.ArgumentParser(description='F&O AI Trader Test Runner')
    parser.add_argument('--module', '-m', help='Run specific test module')
    parser.add_argument('--no-coverage', action='store_true', help='Disable coverage analysis')
    parser.add_argument('--pattern', '-p', default='test_*.py', help='Test file pattern')
    parser.add_argument('--verbosity', '-v', type=int, default=2, choices=[0, 1, 2], help='Test verbosity')
    parser.add_argument('--output-dir', '-o', default='test_reports', help='Output directory for reports')

    args = parser.parse_args()

    logger.info("Starting F&O AI Trader test runner")
    logger.info(f"Arguments: {args}")

    try:
        if args.module:
            # Run specific module
            result = run_specific_test_module(args.module, args.verbosity)
            cov = None
        else:
            # Run all tests
            if args.no_coverage:
                result = discover_and_run_tests('tests', args.pattern, args.verbosity)
                cov = None
            else:
                result, cov = run_tests_with_coverage('tests', args.pattern)

        # Generate reports
        report_file = generate_test_report(result, cov, args.output_dir)

        # Print summary
        success_rate = (result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100
        logger.info(f"Test run completed:")
        logger.info(f"  Tests: {result.testsRun}")
        logger.info(f"  Failures: {len(result.failures)}")
        logger.info(f"  Errors: {len(result.errors)}")
        logger.info(f"  Success Rate: {success_rate:.1f}%")
        logger.info(f"  Report: {report_file}")

        # Exit with appropriate code
        if result.failures or result.errors:
            logger.error("Some tests failed!")
            sys.exit(1)
        else:
            logger.info("All tests passed!")
            sys.exit(0)

    except Exception as e:
        logger.error(f"Test runner failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()