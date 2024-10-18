#!/bin/bash
#
# See we can run without extra deps installed
#

if [[ -n "$VIRTUAL_ENV" ]]; then
      echo "Error: A virtual environment is active. Cannot run."
      exit 1
fi

set -e
set -u

CURRENT_PATH=`pwd`
TEST_SCRIPT=$CURRENT_PATH/tests/standalone-check.py
TEST_PATH=$(mktemp -d)

cd $TEST_PATH

python -m venv venv
source venv/bin/activate
pip install $CURRENT_PATH

python $TEST_SCRIPT

echo "All done"



