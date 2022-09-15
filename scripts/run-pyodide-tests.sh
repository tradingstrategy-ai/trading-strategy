#!/bin/bash
#
# See https://github.com/pyodide/pytest-pyodide
#

set -e

PYODIDE_VERSION=0.21.2
PYODIDE_BUNDLE_DIR=$PWD/dist

install -d $PYODIDE_BUNDLE_DIR

if command -v chromedriver /dev/null ; then
  echo "Chromedriver is `chromedriver --version`"
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
  wget https://github.com/pyodide/pyodide/releases/download/$PYODIDE_VERSION/pyodide-build-$PYODIDE_VERSION.tar.bz2
  tar xjf pyodide-build-$PYODIDE_VERSION.tar.bz2
  mv pyodide $PYODIDE_BUNDLE_DIR
else
  echo "Pyodide bundle already installed"
fi

pytest --runtime=chrome --dist-dir=./dist/pyodide -k pyodide



