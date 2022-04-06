release:
	git submodule update --recursive --init
	poetry build
	poetry publish