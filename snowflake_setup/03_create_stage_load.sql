--Create File Format and Stage
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = PARQUET;

CREATE OR REPLACE STAGE daap_processed_stage
  STORAGE_INTEGRATION = s3_integration
  URL = 's3://digital-asset-analytics-data-lake/processed/candles/'
  FILE_FORMAT = parquet_format;

-- Create the Destination Table
CREATE OR REPLACE TABLE RAW_CANDLES (
    window OBJECT,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE
);

-- Load Data
COPY INTO RAW_CANDLES
  FROM @daap_processed_stage
  MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
  PATTERN = '.*snappy.parquet';