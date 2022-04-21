
setup:
	pip3 install --upgrade pip
	pip3 install poetry --upgrade
	poetry install
	# poetry shell

tests:
	poetry run pytest
