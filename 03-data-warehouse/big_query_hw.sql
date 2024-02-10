CREATE OR REPLACE EXTERNAL TABLE `ny-rides-yadayaza.ny_rides_yadayaza_dataset.2022_external_green_tripdata`
    OPTIONS (
        format = 'PARQUET',
        uris = ['gs://ny_rides_yadayaza_terra_bucket/green/green_tripdata_2022-*.parquet']
);


CREATE OR REPLACE TABLE `ny-rides-yadayaza.ny_rides_yadayaza_dataset.2022_green_tripdata` AS (
    SELECT
    *
    FROM
    `ny-rides-yadayaza.ny_rides_yadayaza_dataset.2022_external_green_tripdata`
);

SELECT COUNT(*) FROM `ny-rides-yadayaza.ny_rides_yadayaza_dataset.2022_external_green_tripdata`;

SELECT COUNT(DISTINCT(PULocationID)) FROM `ny-rides-yadayaza.ny_rides_yadayaza_dataset.2022_external_green_tripdata`;

SELECT COUNT(DISTINCT(PULocationID)) FROM `ny-rides-yadayaza.ny_rides_yadayaza_dataset.2022_green_tripdata`;

SELECT 
    COUNT(*) 
FROM 
    `ny-rides-yadayaza.ny_rides_yadayaza_dataset.2022_green_tripdata`
WHERE 
    fare_amount=0;

CREATE OR REPLACE TABLE `ny-rides-yadayaza.ny_rides_yadayaza_dataset.2022_green_tripdata_partioned`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS (
  SELECT * FROM `ny-rides-yadayaza.ny_rides_yadayaza_dataset.2022_green_tripdata`
);

SELECT 
    COUNT(DISTINCT(PULocationID)) 
FROM  
    `ny-rides-yadayaza.ny_rides_yadayaza_dataset.2022_green_tripdata`
WHERE 
    DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT 
    COUNT(DISTINCT(PULocationID)) 
FROM  
    `ny-rides-yadayaza.ny_rides_yadayaza_dataset.2022_green_tripdata_partioned`
WHERE 
    DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT COUNT(*) FROM `ny-rides-yadayaza.ny_rides_yadayaza_dataset.2022_green_tripdata`;

