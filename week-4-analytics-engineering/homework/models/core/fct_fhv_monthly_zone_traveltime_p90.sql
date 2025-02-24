{{
    config(
        materialized='table'
    )
}}

with trip_data as (
    select
        -- Calculate trip_duration in seconds
        timestamp_diff(dropoff_datetime, pickup_datetime, SECOND) as trip_duration,
        
        -- Extract Year and Month from pickup_datetime
        year,
        month,

        -- Pickup and Dropoff Location IDs
        pickup_locationid,
        pickup_borough,
        pickup_zone, 

        dropoff_locationid,
        dropoff_borough,
        dropoff_zone

    from {{ ref('dim_fhv_trips') }}
    where dropoff_datetime is not null 
      and pickup_datetime is not null
)

select
    *,
    PERCENTILE_CONT(trip_duration, 0.90) OVER (
        partition by year, month, pickup_locationid, dropoff_locationid
    ) as trip_duration_p90
from trip_data
order by trip_duration_p90 desc