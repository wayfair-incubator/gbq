#!/usr/bin/env bash

set -eo pipefail

RUFF_FIX=""

function usage
{
    echo "usage: run_tests.sh [--format-code]"
    echo ""
    echo " --format-code : Format the code instead of checking formatting."
    exit 1
}

while [[ $# -gt 0 ]]; do
    arg="$1"
    case $arg in
        --format-code)
        RUFF_FIX="--fix"
        ;;
        -h|--help)
        usage
        ;;
        "")
        # ignore
        ;;
        *)
        echo "Unexpected argument: ${arg}"
        usage
        ;;
    esac
    shift
done

# only generate html locally
pytest tests --cov-report html

echo "Running ruff linter..."
ruff check ${RUFF_FIX} gbq tests

echo "Running ruff formatter..."
if [ -z "${RUFF_FIX}" ]; then
    ruff format --check gbq tests
else
    ruff format gbq tests
fi

echo "Running mypy..."
mypy gbq

echo "Running bandit..."
bandit -c pyproject.toml --quiet -r gbq
