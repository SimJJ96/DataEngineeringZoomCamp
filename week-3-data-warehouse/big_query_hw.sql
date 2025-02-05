-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dataengineerzoomcamp-449116.ny_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-zoomcamp-3/yellow_tripdata/yellow_tripdata_2024-*.parquet']
);

-- Creating table
CREATE OR REPLACE TABLE `dataengineerzoomcamp-449116.ny_taxi.yellow_tripdata`
AS
SELECT * FROM `ny_taxi.external_yellow_tripdata`;


-- 1) Check count of records for the 2024 Yellow Taxi Data
SELECT COUNT(*) FROM `ny_taxi.external_yellow_tripdata`;

-- 2) Check count the distinct number of PULocationIDs
SELECT COUNT(DISTINCT PULocationID) FROM ny_taxi.external_yellow_tripdata;
SELECT COUNT(DISTINCT(PULocationID)) FROM ny_taxi.yellow_tripdata;

-- 3) Retrieve the PULocationID from the table (not the external table) in BigQuery
SELECT PULocationID FROM ny_taxi.yellow_tripdata;
SELECT PULocationID, DOLocationID FROM ny_taxi.yellow_tripdata;

-- 4) Record that have a fare_amount of 0
SELECT COUNT(fare_amount) FROM `ny_taxi.external_yellow_tripdata` WHERE fare_amount = 0;

-- 5) Create a new table with this strategy
CREATE OR REPLACE TABLE dataengineerzoomcamp-449116.ny_taxi.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM ny_taxi.external_yellow_tripdata;

-- 6) Retrieve the distinct VendorIDs between tpep_dropoff_datetime 03/01/2024 and 03/15/2024
SELECT DISTINCT(VendorID) FROM `ny_taxi.yellow_tripdata` WHERE tpep_dropoff_datetime BETWEEN TIMESTAMP('2024-03-01') AND TIMESTAMP('2024-03-15');
SELECT DISTINCT(VendorID) FROM `ny_taxi.yellow_tripdata_partitoned_clustered` WHERE tpep_dropoff_datetime BETWEEN TIMESTAMP('2024-03-01') AND TIMESTAMP('2024-03-15');