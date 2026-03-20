#!/bin/bash
#
# Perform simple checks roughly equivalent to cliboa-test-* on GitHub Actions.
# These checks do not include a per-Python-version verification.
# This script is intended to be run from the root directory.

MAX_PROCESS=7
CURRENT_PROCESS=1
DO_FIX=0

if [ "$1" == "fix" ]; then
    DO_FIX=1
    MAX_PROCESS=$((MAX_PROCESS+1))
fi

set -eu

echo "[${CURRENT_PROCESS}/${MAX_PROCESS}] validate poetry"
poetry check --strict

if [ $DO_FIX -eq 1 ]; then
    CURRENT_PROCESS=$((CURRENT_PROCESS+1))
    echo "[${CURRENT_PROCESS}/${MAX_PROCESS}] FIX: black, isort, and remove temp files."
    poetry run black .
    poetry run isort .
    rm -fv common/environment.py
    rm -fv conf/logging.conf
    rm -fv conf/cliboa.ini
    rm -fv logs/app.log
fi

CURRENT_PROCESS=$((CURRENT_PROCESS+1))
echo "[${CURRENT_PROCESS}/${MAX_PROCESS}] execute flake8"
poetry run pflake8 .

CURRENT_PROCESS=$((CURRENT_PROCESS+1))
echo "[${CURRENT_PROCESS}/${MAX_PROCESS}] check black format"
poetry run black --check .

CURRENT_PROCESS=$((CURRENT_PROCESS+1))
echo "[${CURRENT_PROCESS}/${MAX_PROCESS}] check isort"
poetry run isort --check .

CURRENT_PROCESS=$((CURRENT_PROCESS+1))
echo "[${CURRENT_PROCESS}/${MAX_PROCESS}] execute bandit"
poetry run bandit --severity-level high -r cliboa

CURRENT_PROCESS=$((CURRENT_PROCESS+1))
echo "[${CURRENT_PROCESS}/${MAX_PROCESS}] check layer dependency"
poetry run lint-imports

CURRENT_PROCESS=$((CURRENT_PROCESS+1))
echo "[${CURRENT_PROCESS}/${MAX_PROCESS}] execute unittest and instrument coverage"
poetry run pytest --cov cliboa --cov-report term-missing --cov-report=xml

echo "Complete!"
