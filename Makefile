release:
	git submodule update --recursive --init
	poetry build
	poetry publish

build-docs:
	poetry run python -V
	poetry run sphinx-build -M html "docs/source" "docs/build"