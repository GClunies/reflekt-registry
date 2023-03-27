# Allows Make commands to be run from root of repo
.PHONY: format
format:
	@autoflake --remove-all-unused-imports --remove-unused-variables --ignore-init-module-imports --recursive --in-place ./reflekt-registry
	@isort ./reflekt-registry
	@black ./reflekt-registry

.PHONY: lint
lint:
	@flake8 ./reflekt-registry
	@black --check ./reflekt-registry

.PHONY: type-check
type-check:
	@mypy ./reflekt-registry

.PHONY: requirements
requirements:
	@poetry export --without-hashes > ./reflekt-registry/requirements.txt
	@echo ""
	@echo "Successfully generated requirements.txt"
