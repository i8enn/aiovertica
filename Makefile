#!make

.DEFAULT_GOAL := cq

library_root = aiovertica
tests_root = tests

## Export environments from .env file
-include .env
export $(shell sed 's/=.*//' .env)

args = $(filter-out $@,$(MAKECMDGOALS))

# Command shortcuts
mypy = mypy ${library_root}
flake = flake8 ${library_root} ${tests_root}
isort = isort ${library_root} ${tests_root}
black = black --fast ${library_root} ${tests_root}
bandit = bandit -iii -ll -r ${library_root}
autopep8 = autopep8 --aggressive --experimental -r -i ${library_root} ${tests_root}

# Shortcut for create environment and install dependencies
.PHONY: install
install:
	# Create virtual env and install dependencies
	poetry env use python3.8
	poetry install

test:
	pytest --no-cov -svv ${args}

test-verbose:
	pytest --no-cov -svvl ${args}

test-cov:
	pytest -vv --junitxml=.artefacts/test_report.xml

test-watch:
	pytest --no-cov -f -svv --ff ${args}

# Shortcut for code quality (run linting and test with coverage)
.PHONY: cq
cq: test-cov lint

.PHONY: format
format:
	-$(isort)
	-$(black) &
	-$(autopep8)

.PHONY: lint
lint:
	-$(flake)
	-$(bandit)
	-$(isort) --check-only
	-$(black) --check
	-$(mypy)

.PHONY: clean
clean:
	-rm -rf `find . -name __pycache__`
	-rm -f `find . -type f -name '*.py[co]' `
	-rm -f `find . -type f -name '*~' `
	-rm -f `find . -type f -name '.*~' `
	-rm -rf dist *.egg-info
	-rm -rf .cache
	-rm -rf .pytest_cache
	-rm -rf .mypy_cache
	-rm -rf htmlcov
	-rm -f .coverage
	-rm -f .coverage.*
