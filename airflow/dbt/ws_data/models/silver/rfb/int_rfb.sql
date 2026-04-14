{{ config(
    materialized='table',
    schema='silver'
)}}

WITH stg_rfb AS (
    SELECT * FROM {{ ref('stg_rfb') }}
),

higienizacao AS (
    SELECT 
        *,
        LPAD(CAST(TRIM(cnpj) AS VARCHAR), 14, '0') AS cnpj_limpo,
        LPAD(REPLACE(CAST(TRIM(cep) AS VARCHAR), '-', ''), 8, '0') AS cep_limpo
    FROM stg_rfb
),

formatacao AS (
    SELECT 
        -- Máscara do CNPJ
        SUBSTRING(cnpj_limpo, 1, 2) || '.' || 
        SUBSTRING(cnpj_limpo, 3, 3) || '.' || 
        SUBSTRING(cnpj_limpo, 6, 3) || '/' || 
        SUBSTRING(cnpj_limpo, 9, 4) || '-' || 
        SUBSTRING(cnpj_limpo, 13, 2) AS cnpj,
        
        TRIM(UPPER(razao_social)) AS real_name ,
        TRIM(UPPER(nome_fantasia)) AS name,
        TRIM(UPPER(logradouro)) AS street_name,
        numero as street_number,
        TRIM(UPPER(complemento)) AS complement ,
        TRIM(UPPER(bairro)) AS district,
        TRIM(UPPER(municipio)) AS city,
        TRIM(UPPER(uf)) AS state,
        
        -- Máscara do CEP
        SUBSTRING(cep_limpo, 1, 5) || '-' || 
        SUBSTRING(cep_limpo, 6, 3) AS cep,
        
        cnae_principal,
        TRIM(UPPER(descricao_cnae)) AS cnae_description,
        TRIM(UPPER(situacao_cadastral)) AS status,
        data_abertura,
        data_carga_bronze,
        sistema_origem,
        CURRENT_TIMESTAMP AS data_processamento_silver
    FROM higienizacao
)
, chave_sk as (
    select 
    {{dbt_utils.generate_surrogate_key(['cnpj'])}} as sk_rfb
    ,*
    from formatacao
)
SELECT * FROM chave_sk