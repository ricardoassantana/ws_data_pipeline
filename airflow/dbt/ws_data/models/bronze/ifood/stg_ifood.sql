{{ config(
    materialized='view',
    schema='bronze'
)}}

WITH source AS (
    SELECT * FROM {{source('raw_ifood_source','raw_ifood')}}
)

select 
 cast(slug as varchar) as slug
 ,cast(name as varchar) as name
 ,cast(category as varchar) as category
 ,cast(cnpj as varchar) as cnpj
 ,cast(street_name as varchar) as street_name
 ,cast(street_number as varchar) as street_number
 ,cast(complement as varchar) as complement
 ,cast(district as varchar) as district
 ,cast(city as varchar) as city
 ,cast(state as varchar) as state
 ,cast(zipcode as varchar) as zipcode
 ,cast(phone as varchar) as phone
 ,cast(latitude as float) as latitude
 ,cast(longitude as float) as longitude
 ,CURRENT_TIMESTAMP AS data_carga_bronze
 ,concat(latitude,longitude) as lat_lon
 ,'ifood' as sistema_origem

 from source
 