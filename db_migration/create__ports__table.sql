
CREATE OR REPLACE TABLE staging.ports
    AS SELECT * FROM read_csv_auto('input_files/DE_casestudy_ports.csv');