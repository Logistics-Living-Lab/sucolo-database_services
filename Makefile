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

test_cov:
	poetry run coverage run -m pytest sucolo_database_services --cov-config=.coveragerc --junit-xml=coverage/junit/test-results.xml --cov-report=html --cov-report=xml
	poetry run coverage html -d coverage/html
	poetry run coverage xml -o coverage/coverage.xml
	poetry run coverage report --show-missing

test_tox:
	poetry run tox

build: isort black flake8 mypy test

stubs:
	poetry run stubgen -o sucolo_database_services-stubs sucolo_database_services
	poetry run isort sucolo_database_services-stubs
	poetry run black --config .black.cfg sucolo_database_services-stubs
