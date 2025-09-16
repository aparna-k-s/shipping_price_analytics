

CREATE OR REPLACE TABLE raw.raw_exchange_rates_to_USD
     AS SELECT *, 'DE_casestudy_exchange_rates.csv' as FILE_NAME, current_timestamp as INSERTED_AT
          FROM read_csv_auto('input_files/DE_casestudy_exchange_rates.csv');

