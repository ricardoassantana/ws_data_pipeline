{{ config(
    materialized='table',
    schema='silver'
)}}

WITH stg_google AS (
    SELECT * FROM {{ ref('stg_google') }}
),

higienizacao AS (
    SELECT 
        *,
        LPAD(REPLACE(CAST(zipcode AS VARCHAR), '-', ''), 8, '0') AS cep_limpo
    FROM stg_google
),

formatacao AS (
    SELECT 
        cid,
        TRIM(UPPER(name)) AS name,
        TRIM(UPPER(place_type)) AS place_type,
        CASE WHEN LEFT(TRIM(UPPER(street_name)),2) = 'R.' THEN CONCAT('RUA', right(upper(street_name),length(street_name)-2)) ELSE TRIM(UPPER(street_name)) END AS street_name ,
        CAST(regexp_replace(street_number, '[^0-9]', '', 'g') AS varchar) AS street_number,
        regexp_replace(street_number, '[0-9]', '', 'g') AS complement,
        TRIM(UPPER(district)) AS district,
        TRIM(UPPER(city)) as city,
        TRIM(UPPER(state)) as state,
        
        -- Máscara do CEP
        SUBSTRING(cep_limpo, 1, 5) || '-' || 
        SUBSTRING(cep_limpo, 6, 3) AS zipcode,
        
        phone,
        rating,
        rating_count,
        is_operational,
        latitude,
        longitude,
        data_carga_bronze,
        lat_lon,
        sistema_origem,
        CURRENT_TIMESTAMP AS data_processamento_silver
    FROM higienizacao
),
chave_sk as (
    select 
    {{dbt_utils.generate_surrogate_key(['cid'])}} as sk_google
    ,*

    from formatacao
)

select * from chave_sk