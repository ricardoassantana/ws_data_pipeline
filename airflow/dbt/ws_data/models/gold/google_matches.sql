{{ config(
    materialized='table',
    schema='gold'
)}}

WITH cte_companies AS (
    select * from {{ref ('int_companies')}}
)
select 
    {{ dbt_utils.generate_surrogate_key(['ifood_slug', 'google_cid', 'rfb_document_number']) }} AS company_id
    , google_cid
    ,CONCAT('ifood_slug',' ','google_cid',' ','rfb_document_number') AS match_policy 
from cte_companies
where google_cid is not null