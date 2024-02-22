{{ config(materialized='table') }}

with fhv_tripdata as (
    select * from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    -- identifiers 
    fhv_tripdata.tripid, 
    fhv_tripdata.dispatching_base_num, 
    coalesce(cast(fhv_tripdata.is_shared as integer),0) as is_shared,
    fhv_tripdata.affiliated_base_number,

    -- location information
    fhv_tripdata.pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,

    -- timestamp  
    fhv_tripdata.pickup_datetime, 
    fhv_tripdata.dropoff_datetime,
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid