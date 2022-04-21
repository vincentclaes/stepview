
install:
	pip3 install --upgrade pip
	pip3 install poetry --upgrade
	poetry install

setup: install
	poetry shell

tests:
	poetry run pytest
