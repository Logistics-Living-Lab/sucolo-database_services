isort:
	poetry run isort sucolo_database_services

black:
	poetry run black --config .black.cfg sucolo_database_services

flake8:
	poetry run flake8 sucolo_database_services

format: isort black

mypy:
	poetry run mypy --incremental --install-types --show-error-codes --pretty sucolo_database_services

test:
	poetry run pytest sucolo_database_services

build: isort black flake8 mypy test
