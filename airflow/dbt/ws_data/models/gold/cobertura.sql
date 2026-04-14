{{ config(
    materialized='table',
    schema='gold'
)}}


WITH master_empresas AS (
    SELECT * FROM {{ ref('int_companies') }} 
),

-- 2. Calcula as métricas de cobertura usando as Surrogate Keys
metricas_cobertura AS (
    SELECT
        
        COUNT(*) AS total_geral_empresas,

        -- VOLUMES INDIVIDUAIS 
        COUNT(CASE WHEN sk_ifood IS NOT NULL THEN 1 END) AS total_ifood,
        COUNT(CASE WHEN sk_rfb IS NOT NULL THEN 1 END) AS total_rfb,
        COUNT(CASE WHEN sk_google IS NOT NULL THEN 1 END) AS total_google,

        -- Interseções entre duas bases
        COUNT(CASE WHEN sk_google IS NOT NULL AND sk_ifood IS NOT NULL THEN 1 END) AS intersecao_google_x_ifood,
        COUNT(CASE WHEN sk_google IS NOT NULL AND sk_rfb IS NOT NULL THEN 1 END) AS intersecao_google_x_rfb,
        COUNT(CASE WHEN sk_ifood IS NOT NULL AND sk_rfb IS NOT NULL THEN 1 END) AS intersecao_ifood_x_rfb,

        -- (Aparece APENAS nesta fonte e em nenhuma outra)
        COUNT(CASE WHEN sk_google IS NOT NULL AND sk_ifood IS NULL AND sk_rfb IS NULL THEN 1 END) AS apenas_google,
        COUNT(CASE WHEN sk_ifood IS NOT NULL AND sk_google IS NULL AND sk_rfb IS NULL THEN 1 END) AS apenas_ifood,
        COUNT(CASE WHEN sk_rfb IS NOT NULL AND sk_google IS NULL AND sk_ifood IS NULL THEN 1 END) AS apenas_rfb,

        --  (Aparece nas 3 fontes)
        COUNT(CASE WHEN sk_google IS NOT NULL AND sk_ifood IS NOT NULL AND sk_rfb IS NOT NULL THEN 1 END) AS contem_nas_tres_fontes,
        
        -- Timestamp de quando essa foto foi tirada
        CURRENT_TIMESTAMP AS data_atualizacao
        
    FROM master_empresas
)

SELECT * FROM metricas_cobertura