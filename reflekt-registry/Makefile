# Allows Make commands to be run from inside reflex-registry/reflex-registry/
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

.PHONY: requirements
requirements:
	@poetry export --without-hashes > requirements.txt
	@echo ""
	@echo "Successfully generated requirements.txt"
