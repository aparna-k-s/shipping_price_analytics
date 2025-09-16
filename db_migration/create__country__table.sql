
CREATE OR REPLACE TABLE staging.country
    AS SELECT distinct COUNTRY,  COUNTRY_CODE FROM staging.ports;