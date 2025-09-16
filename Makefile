.PHONY: install db_setup

DB =  $(DUCKDB_PATH)

all: install db_setup

install:
	@echo "Installing duckdb and dbt-duckdb"
	brew install duckdb
	python3 -m venv venv
	source ./venv/bin/activate
	pip3 install -U duckdb==0.10.3 dbt-core dbt-duckdb==1.7.4

db_setup:
	@echo "Creating schemas and static tables in DuckDB"
	duckdb $(DB) -c ".read db_migration/create__schemas.sql"
	duckdb $(DB) -c ".read db_migration/create__ports__table.sql"
	duckdb $(DB) -c ".read db_migration/create__regions__table.sql"
	duckdb $(DB) -c ".read db_migration/create__country__table.sql"
	duckdb $(DB) -c ".read db_migration/create__exchange_rates_to_USD__table.sql"
	duckdb $(DB) -c ".read db_migration/create__datapoints__table.sql"
	duckdb $(DB) -c ".read db_migration/create__charges__table.sql"
	@echo "DuckDB initial set up completed"
	@echo "DuckDB CLI - run sql queries here. Exit before running dbt"
	duckdb $(DB)