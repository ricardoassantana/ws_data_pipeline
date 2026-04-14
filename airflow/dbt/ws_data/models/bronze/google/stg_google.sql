{{ config(
    materialized='view',
    schema='bronze'
)}}

WITH source AS (
    SELECT * FROM {{source('raw_google_source','raw_google')}}
)

select 
    cast(cid as varchar) as cid
    , cast(name as varchar) as name
    , cast(place_type as varchar) as place_type
    , cast(street_name as varchar) as street_name
    , cast(street_number as varchar) as street_number
    , cast(district as varchar) as district
    , cast(city as varchar) as city
    , cast(state as varchar) as state
    , cast(zipcode as varchar) as zipcode
    , cast(phone as varchar) as phone
    , cast(rating as float) as rating
    , cast(rating_count as float) as rating_count
    , cast(is_operational as varchar) as is_operational
    , cast(latitude as float) as latitude
    , cast(longitude as float) as longitude
    ,concat(latitude,longitude) as lat_lon
    ,CURRENT_TIMESTAMP AS data_carga_bronze
 ,'google' as sistema_origem

 from source
 