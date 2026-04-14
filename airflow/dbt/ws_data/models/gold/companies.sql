{{ config(
    materialized='table',
    schema='gold'
)}}

WITH cte_companies AS (
    select * from {{ref ('int_companies')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(['ifood_slug', 'google_cid', 'rfb_document_number']) }} AS id
    ,name
    ,address_street_name 
    ,address_street_number
    ,address_district
    ,address_city
    ,address_state
    ,address_zipcode
    ,latitude
    ,longitude
    ,has_rfb
    ,rfb_document_number
    ,rfb_situation
    ,rfb_cnae
    ,rfb_social_name
    ,rfb_trading_name
    ,google_cid
    ,google_place_type
    ,google_rating
    ,google_rating_count
    ,ifood_slug
    ,ifood_category
    ,sistema_origem as source
    ,sk_google
    ,sk_ifood
    ,sk_rfb

from cte_companies