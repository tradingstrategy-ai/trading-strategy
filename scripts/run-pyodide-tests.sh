#!/bin/bash
#
# See https://github.com/pyodide/pytest-pyodide
#

set -e

PYODIDE_BUNDLE_DIR=$PWD/dist

install -d $PYODIDE_BUNDLE_DIR

if command -v chromedriver /dev/null ; then
  # pass
else
  # New Docker versions have command "docker compose"
  echo "chromedriver command not installed"
  echo "Try: "
  echo "   brew install chromedriver"
  echo "   xattr -d com.apple.quarantine `which chromedriver`"
  exit 1
fi

# Get Pyodide distribution if we do not have one yet
if [ ! -e "$PYODIDE_BUNDLE_DIR/pyodide" ] ; then
  echo "Downloading Pyodide bundle"
  cd /tmp
  wget https://github.com/pyodide/pyodide/releases/download/0.21.0/pyodide-build-0.21.0.tar.bz2
  tar xjf pyodide-build-0.21.0.tar.bz2
  mv pyodide $PYODIDE_BUNDLE_DIR
else
  echo "Pyodide bundle already installed"
fi

pytest --runtime=chrome --dist-dir=./dist/pyodide -k pyodide



