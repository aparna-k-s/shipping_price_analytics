
CREATE OR REPLACE TABLE staging.regions
    AS SELECT * FROM read_csv_auto('input_files/DE_casestudy_regions.csv');