.PHONY: format
format:
	@autoflake --remove-all-unused-imports --remove-unused-variables --ignore-init-module-imports --recursive --in-place .
	@isort .
	@black .

.PHONY: lint
lint:
	@flake8 .
	@black --check .

.PHONY: type-check
type-check:
	@mypy .
