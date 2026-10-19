.PHONY: test audit quality generate parquet demo ware house ci
PYTHON ?= python
PROFILE ?= sample
SOURCE ?= leads_ventas

test:
	$(PYTHON) -m pytest -q tests

audit:
	$(PYTHON) -m sunset.audit --input data/sample --output reports/data_audit.json

quality:
	$(PYTHON) -m sunset.quality --input data/sample --scope full --report reports/quality_local.json

generate:
	$(PYTHON) -m sunset.cli generate --profile $(PROFILE)

parquet:
	$(PYTHON) -m sunset.parquet --input data/$(PROFILE) --output data/parquet --config config/pipeline.json

warehouse-load:
	$(PYTHON) -m warehouse.load_parquet --source $(SOURCE)

marts:
	$(PYTHON) -m warehouse.build_marts

dbt:
	dbt run --project-dir dbt --profiles-dir dbt
	dbt test --project-dir dbt --profiles-dir dbt

demo: test audit quality

ci: demo

# SQLite relational proxy only. Run the real infrastructure acceptance separately.
stress-smoke:
	$(PYTHON) stress/sqlite_load.py --config config/stress_smoke.json --db data/stress/sunset_proxy_smoke.sqlite --report reports/stress_smoke.json

stress-plus:
	$(PYTHON) stress/sqlite_load.py --config config/stress.json --db data/stress/sunset_proxy_plus.sqlite --report reports/stress_sqlite_plus.json

stress-double:
	$(PYTHON) stress/sqlite_load.py --config config/stress_2x.json --db data/stress/sunset_proxy_2x.sqlite --report reports/stress_sqlite_2x.json

stress-csv:
	$(PYTHON) -m stress.run_original_csv_generation --config config/stress.json --out data/stress/csv --report reports/stress_csv_generation.json
