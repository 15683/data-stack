.PHONY: test lint render deploy smoke doctor build import-images status demo backfill backup restore-check
PYTHON ?= python3

test:
	$(PYTHON) -m pytest -m 'not integration'

lint:
	$(PYTHON) -m ruff check pipeline dags scripts tests
	$(PYTHON) -m ruff format --check pipeline dags scripts tests
	helm lint deploy/chart
	for script in scripts/*.sh; do bash -n "$$script" || exit; done

render:
	helm template data-stack deploy/chart --namespace data-stack

deploy:
	bash scripts/local.sh deploy

doctor build import-images status demo:
	bash scripts/local.sh $@

backfill:
	bash scripts/local.sh backfill $(ARGS)

backup restore-check smoke:
	bash scripts/local.sh $@
