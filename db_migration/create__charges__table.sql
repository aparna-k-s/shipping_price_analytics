
CREATE OR REPLACE TABLE  raw.raw_charges
       AS SELECT *, 'DE_casestudy_charges_1.csv' as FILE_NAME, current_timestamp as INSERTED_AT
          FROM read_csv_auto('input_files/DE_casestudy_charges_1.csv');

