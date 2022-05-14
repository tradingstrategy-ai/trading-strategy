# Create a new PyPi release
release:
	git submodule update --recursive --init
	poetry build
	poetry publish

# Build Sphinx documentation locally
build-docs:
	poetry run python -V
	poetry run sphinx-build -M html "docs/source" "docs/build"

clean-docs:
	rm -rf docs/build/html