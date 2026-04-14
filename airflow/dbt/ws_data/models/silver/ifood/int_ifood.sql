{{ config(
    materialized='table',
    schema='silver'
)}}

WITH stg_ifood AS (
    SELECT * FROM {{ ref('stg_ifood') }}
),

higienizacao AS (
    SELECT 
        *,
        LPAD(CAST(cnpj AS VARCHAR), 14, '0') AS cnpj_limpo,
        LPAD(REPLACE(CAST(zipcode AS VARCHAR), '-', ''), 8, '0') AS cep_limpo
    FROM stg_ifood
),

formatacao AS (
    SELECT 
        slug,
        TRIM(UPPER(name)) AS name,
        TRIM(UPPER(category)) as category,
        
        -- Máscara do CNPJ
        SUBSTRING(cnpj_limpo, 1, 2) || '.' || 
        SUBSTRING(cnpj_limpo, 3, 3) || '.' || 
        SUBSTRING(cnpj_limpo, 6, 3) || '/' || 
        SUBSTRING(cnpj_limpo, 9, 4) || '-' || 
        SUBSTRING(cnpj_limpo, 13, 2) AS cnpj,
        
        TRIM(UPPER(street_number)) AS street_number,
        CASE WHEN LEFT(TRIM(UPPER(street_name)),2) = 'R.' THEN CONCAT('RUA', right(upper(street_name),length(street_name)-2)) ELSE TRIM(UPPER(street_name)) END AS street_name,
        TRIM(UPPER(complement)) AS complement,
        TRIM(UPPER(district)) AS district,
        TRIM(UPPER(city)) AS city,
        TRIM(UPPER(state)) AS state,
        
        -- Máscara do CEP
        SUBSTRING(cep_limpo, 1, 5) || '-' || 
        SUBSTRING(cep_limpo, 6, 3) AS zipcode,
        
        phone,
        latitude,
        longitude,
        lat_lon,
        data_carga_bronze,
        sistema_origem,
        CURRENT_TIMESTAMP AS data_processamento_silver
    FROM higienizacao
)

, chave_sk as (
    select 
    {{dbt_utils.generate_surrogate_key(['slug'])}} as sk_ifood
    ,*

    from formatacao
)

SELECT * FROM chave_sk