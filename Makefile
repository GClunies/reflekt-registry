.PHONY: format
format:
	@autoflake --remove-all-unused-imports --remove-unused-variables --ignore-init-module-imports --recursive --in-place ./registry
	@isort ./registry
	@black ./registry

.PHONY: lint
lint:
	@flake8 ./registry
	@black --check ./registry

.PHONY: type-check
type-check:
	@mypy ./registry
