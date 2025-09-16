# Shipping Analytics
---
## Overview
dbt based data pipeline to load given csv files into a data warehouse, validate and transform data to generate daily regional aggregated prices based on port-to-port contract data.

## Quick start
1. Clone the repo and cd into the directory
```
git clone https://github.com/aparna-k-s/shipping_price_analytics.git 
cd shipping_price_analytics
```
2. Activate virtual env, set duckdb env variable, and run the make command to install duckdb, dbt and load csv files into duckdb
```
python -m venv .venv && source .venv/bin/activate
export DUCKDB_PATH="$(pwd)/<>.duckdb" 
make
```
Check if the tables are created, sample queries
```
show tables;
select * from staging.ports limit 5;
```
3. If there is an issue with config or if using existing duckdb, run make db_setup to load the files with the right duckdb config to complete initial set up 
```
make db_setup
```
4. cd into the dbt project and dbt run would load from raw to staging to final and generate the reports
```
cd dbt_shipping_price_analytics
dbt run
```
5. Run the dbt macro to load datapoints and charges files incrementally
```
dbt run-operation insert_csv_into_raw_table --args '{"schema":"raw", "table":"raw_datapoints", "file_path":"input_files/DE_casestudy_datapoints_2.csv"}'
dbt run-operation insert_csv_into_raw_table --args '{"schema":"raw", "table":"raw_charges", "file_path":"input_files/DE_casestudy_charges_2.csv"}'
dbt run 
```

# Design

# Architecture
- Datawarehouse: DuckDB
- Transformation: dbt, SQL
- Orchestration: Makefile
- Makefile to set up and do the intial loads from csv files since this is built with the intend to run locally and a few times
- Incremental loads are file based and run using dbt for datapoints and charges within the scope of the case study

# Assumptions
- Price is valid for all the days in the datapoints file between valid_from and valid_to dates

# Data Modeling

Model Lineage 

<img width="1185" height="380" alt="Pasted Graphic 2" src="https://github.com/user-attachments/assets/59b700ad-fddd-4dbb-b95b-bc33fd98cd11" />




- Opted not to do elaborate dimensional modeling as the datasets and scope *(_and business knowledge_)* is limited
- Naming convention - <schema-code>_<dimension-name/fact-name>_<optional - transformation or agg detail>
- Table DDL is based on csv files, maintained in data migrations directory or dbt depending on scope
- Raw tables and static dimensions are modeled after csv files with auto data types (For e.g. IDs are mostly bigint)
- Static Dimensions are de-duped and loaded to staging directly
- Datapoints and Charges csv files are loaded into raw tables as-is using dbt macro and one file at a time
- Transformed (e.g. USD conversion) and aggregated data is loaded to final tables to generate price reports
- Region Hierarchy is flattened in a model and the final report can be used to get the price for any level of region. For e.g. from a port to a top level

Future Scope:
- Add dimensions such as Equipment, Supplier, Company, Country in addition to Ports and Regions & Model slowly changing dimensions to capture changes over time
- Establish naming conventions, data types, formatting (case etc),column definitions, DB migration tool, RBAC 
- Add an intermediate data warehousing layer with well defined dimensions and facts
- Define and remodel Region Hierarchy as this is not clear from the requirements or data
- How to handle contract updates or amendments
- Price is calculated for each day between valid_from and valid_to dates, leading to multiple rows. Price data is needed for each day, but keep an eye on performance as the data grows. This can be pre-aggregated but keeping it direct since runs are all completed in under 1 second.

# Data Quality and Validation
- Mostly limited to de-duping, null checks and referential integrity checks done in dbt tests
Future scope:
- More dbt tests, validations, business rule checks

# Analytics
Sample Queries to find daily price between ports
`SELECT * FROM final.final_shipping_metric_by_port WHERE (valid_date BETWEEN '2022-01-01' AND '2022-01-31') AND origin_port_code = 'HKHKG' AND destination_port_code ='USLGB';`
`SELECT * FROM final.final_shipping_metric_by_port WHERE (valid_date BETWEEN '2022-01-01' AND '2022-01-31') AND origin_slug = 'china_east_main' AND destination_slug ='us_west_coast';`
`SELECT * FROM final.final_shipping_metric_by_port WHERE (valid_date BETWEEN '2022-01-01' AND '2022-01-31') AND dq_ok;`
Find 97.5 percentile for a lane
```
SELECT shipping_lane, valid_date, equipment_id,
    PERCENTILE_CONT(0.975) WITHIN GROUP (ORDER BY avg_price_usd) OVER (PARTITION BY shipping_lane, valid_date, equipment_id) AS market_high
FROM final.final_shipping_metric_by_port;
```

# Optional Feature
- Create a history table similar to `final_price_dq_check` and add `processed_at` timestamp and insert the data that passes check (gets more than 5 companies and more than 2 suppliers) into it if not already present
- Add the above logic as pre-hook in the model `final_price_dq_check`

##Productionization
Data modeling is covered in Future Scope.
For Architecture, 
- Migrate to Snowflake/BigQuery/Redshift/Motherduck
- Ingest files in an event-driven manner while adding file metadata tracking, standardization to data types and names, column mapping if needed
- Orchestrate dbt using dbt cloud or Dagster/Airflow. Convert pipelines to load incrementally based on timestamp and use partitioning for better insert/update performance
- Add Data Quality checks and business rule validation at every step with exception handling with dbt packages such as great expectations 
- Pipeline, performance monitoring and alerting depending on the technology
- Add BI & Analytics tools to generate reports and dashboards
