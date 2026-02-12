#!/bin/bash
# Test runner script for F&O AI Trader

set -e  # Exit on any error

echo "F&O AI Trader Test Suite"
echo "========================"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required but not found"
    exit 1
fi

# Check if we're in a virtual environment (recommended)
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "✓ Running in virtual environment: $VIRTUAL_ENV"
else
    echo "⚠ Warning: Not running in a virtual environment"
    echo "  It's recommended to create and activate a virtual environment:"
    echo "  python3 -m venv .venv && source .venv/bin/activate"
fi

# Install test dependencies if not already installed
echo "Installing test dependencies..."
pip install -q coverage

# Create necessary directories
mkdir -p logs
mkdir -p test_reports

# Parse command line arguments
COVERAGE_FLAG=""
MODULE_FLAG=""
VERBOSITY=2

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-coverage)
            COVERAGE_FLAG="--no-coverage"
            shift
            ;;
        --module|-m)
            MODULE_FLAG="--module $2"
            shift 2
            ;;
        --verbosity|-v)
            VERBOSITY="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --no-coverage      Disable coverage analysis"
            echo "  --module, -m NAME  Run specific test module"
            echo "  --verbosity, -v N  Set verbosity level (0-2)"
            echo "  --help, -h         Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Run the tests
echo "Running tests..."
python3 tests/test_runner.py $COVERAGE_FLAG $MODULE_FLAG --verbosity $VERBOSITY

echo ""
echo "Test run completed!"
echo "Check test_reports/ directory for detailed reports"