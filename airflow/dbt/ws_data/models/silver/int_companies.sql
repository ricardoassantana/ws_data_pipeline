{{ config(
    materialized='table',
    schema='silver'
)}}

{#
    
     -- COMPANIES — Tabela consolidada de empresas (Google + iFood + RFB)     
    ╠════════════════════════════════════════════════════════════════════════╣
   
      Defini os joins pelas chaves únicas de cada tabela:                    

      1. iFood ↔ RFB    → CNPJ (identificador único de empresa)             
      2. Google ↔ iFood  → lat_lon (combinação latitude+longitude+end)      
      3. Google ↔ RFB    → street_name + end (endereço)       
      Resultado: tabela unificada com dados das 3 fontes.                   
      UNION de 3 passes garante que nenhum registro se perca.               

   
#}

-- ═══════════════════════════════════════════════════════════════
-- FONTES: Tabelas intermediárias da Silver usando dbt
-- ═══════════════════════════════════════════════════════════════
WITH google AS (
    SELECT * FROM {{ ref('int_google') }}
),

ifood AS (
    SELECT * FROM {{ ref('int_ifood') }}
),

rfb AS (
    SELECT * FROM {{ ref('int_rfb') }}
),

-- ═══════════════════════════════════════════════════════════════
-- PASSO 1: iFood + RFB via CNPJ
-- ═══════════════════════════════════════════════════════════════
ifood_com_rfb AS (
    SELECT
        i.sk_ifood, 
        i.slug,
        i.name              AS ifood_name,
        i.category,
        i.cnpj,
        i.street_name       AS ifood_street_name,
        i.street_number     AS ifood_street_number,
        i.complement        AS ifood_complement,
        i.district          AS ifood_district,
        i.city              AS ifood_city,
        i.state             AS ifood_state,
        i.zipcode           AS ifood_zipcode,
        i.latitude          AS ifood_latitude,
        i.longitude         AS ifood_longitude,
        i.lat_lon           AS ifood_lat_lon,
        
        r.sk_rfb, 
        r.real_name         AS rfb_social_name,
        r.name              AS rfb_trading_name,
        r.street_name       AS rfb_street_name,
        CAST(r.street_number AS VARCHAR) AS rfb_street_number,
        r.complement        AS rfb_complement,
        r.district          AS rfb_district,
        r.city              AS rfb_city,
        r.state             AS rfb_state,
        r.cep               AS rfb_zipcode,
        r.cnae_principal    AS rfb_cnae,
        r.cnae_description  AS rfb_cnae_description,
        r.status            AS rfb_situation,
        
        CASE WHEN r.cnpj IS NOT NULL THEN TRUE ELSE FALSE END AS has_rfb
    FROM ifood i
    LEFT JOIN rfb r ON i.cnpj = r.cnpj
),

-- ═══════════════════════════════════════════════════════════════
-- PASSO 2: Enriquecer a base iFood+RFB com dados do Google
-- ═══════════════════════════════════════════════════════════════
ifood_rfb_google AS (
    SELECT
        ir.*,
        coalesce(g.sk_google, g_end.sk_google) AS sk_google, 
        coalesce(g.cid,g_end.cid) AS google_cid,
        coalesce(g.name,g_end.name) AS google_name,
        coalesce(g.place_type,g_end.place_type) AS google_place_type,
        coalesce(g.rating,g_end.rating) AS google_rating,
        coalesce(g.rating_count,g_end.rating) AS google_rating_count,
        coalesce(g.street_name,g_end.street_name) AS google_street_name,
        coalesce(g.district,g_end.district) AS google_district,
        coalesce(g.city,g_end.city) AS google_city,
        coalesce(g.state,g_end.state) AS google_state,
        coalesce(g.zipcode, g_end.state) AS google_zipcode
    FROM ifood_com_rfb ir
    LEFT JOIN google g ON ir.ifood_lat_lon = g.lat_lon
    LEFT JOIN google g_end ON g_end.street_name   = ir.rfb_street_name 
                          and g_end.street_number = ir.rfb_street_number
),

-- ═══════════════════════════════════════════════════════════════
-- PARTE A: Registros originados do iFood 
-- ═══════════════════════════════════════════════════════════════
parte_ifood AS (
    SELECT
        sk_ifood,   
        sk_google,  
        sk_rfb,     
        COALESCE(google_name, ifood_name, rfb_trading_name)                 AS name,
        COALESCE(ifood_street_name, rfb_street_name, google_street_name)    AS address_street_name,
        COALESCE(ifood_street_number, rfb_street_number)                    AS address_street_number,
        COALESCE(ifood_district, rfb_district, google_district)             AS address_district,
        COALESCE(ifood_city, rfb_city, google_city)                         AS address_city,
        COALESCE(ifood_state, rfb_state, google_state)                      AS address_state,
        COALESCE(ifood_zipcode, rfb_zipcode, google_zipcode)                AS address_zipcode,
        ifood_latitude                                                      AS latitude,
        ifood_longitude                                                     AS longitude,
        has_rfb,
        cnpj                                                                AS rfb_document_number,
        rfb_situation,
        rfb_cnae,
        rfb_social_name,
        rfb_trading_name,
        google_cid,
        google_place_type,
        google_rating,
        google_rating_count,
        slug                                                                AS ifood_slug,
        category                                                            AS ifood_category
    FROM ifood_rfb_google
),

-- ═══════════════════════════════════════════════════════════════
-- Rastrear quais CIDs do Google já foram usados no passo 2
-- ═══════════════════════════════════════════════════════════════
google_ja_usado_lat_lon AS (
    SELECT DISTINCT g.cid
    FROM google g
    INNER JOIN ifood i ON g.lat_lon = i.lat_lon
),

google_ja_usado_end AS (
    SELECT DISTINCT g.cid
    FROM google g
    INNER JOIN ifood i ON g.street_name   = i.street_name 
                        and g.street_number = i.street_number
),

google_ja_usado as (
    select distinct cid
    from google_ja_usado_lat_lon
    union 
    select distinct cid
    from google_ja_usado_end
),

-- ═══════════════════════════════════════════════════════════════
-- PASSO 3: Google órfãos 
-- ═══════════════════════════════════════════════════════════════
google_orfao AS (
    SELECT g.*
    FROM google g
    WHERE g.cid NOT IN (SELECT cid FROM google_ja_usado)
),

google_orfao_com_rfb AS (
    SELECT
        go.sk_google, 
        go.cid,
        go.name                                     AS google_name,
        go.place_type                               AS google_place_type,
        go.street_name                              AS google_street_name,
        go.district                                 AS google_district,
        go.city                                     AS google_city,
        go.state                                    AS google_state,
        go.zipcode                                  AS google_zipcode,
        go.rating                                   AS google_rating,
        go.rating_count                             AS google_rating_count,
        go.latitude,
        go.longitude,
        
        r.sk_rfb, 
        r.cnpj                                      AS rfb_cnpj,
        r.real_name                                 AS rfb_social_name,
        r.name                                      AS rfb_trading_name,
        CAST(r.street_number AS VARCHAR)            AS rfb_street_number,
        r.cnae_principal                            AS rfb_cnae,
        r.status                                    AS rfb_situation,
        
        ROW_NUMBER() OVER (
            PARTITION BY go.cid
            ORDER BY
                CASE WHEN r.cnpj IS NOT NULL THEN 0 ELSE 1 END,
                r.cnpj
        ) AS rn
    FROM google_orfao go
    LEFT JOIN rfb r
        ON go.street_name = r.street_name
        AND go.street_number = r.street_number
        AND go.district = r.district
),

google_orfao_dedup AS (
    SELECT * FROM google_orfao_com_rfb WHERE rn = 1
),

-- ═══════════════════════════════════════════════════════════════
-- PARTE B: Registros originados do Google órfão
-- ═══════════════════════════════════════════════════════════════
parte_google AS (
    SELECT
        NULL::VARCHAR                                                       AS sk_ifood,  -- Forçando Nulo para o UNION
        g.sk_google,                                                        
        g.sk_rfb,                                                           
        g.google_name                                                       AS name,
        g.google_street_name                                                AS address_street_name,
        g.rfb_street_number                                                 AS address_street_number,
        g.google_district                                                   AS address_district,
        g.google_city                                                       AS address_city,
        g.google_state                                                      AS address_state,
        g.google_zipcode                                                    AS address_zipcode,
        g.latitude,
        g.longitude,
        CASE WHEN g.rfb_cnpj IS NOT NULL THEN TRUE ELSE FALSE END          AS has_rfb,
        g.rfb_cnpj                                                          AS rfb_document_number,
        g.rfb_situation,
        g.rfb_cnae,
        g.rfb_social_name,
        g.rfb_trading_name,
        g.cid                                                               AS google_cid,
        g.google_place_type,
        g.google_rating,
        g.google_rating_count,
        NULL::VARCHAR                                                       AS ifood_slug,
        NULL::VARCHAR                                                       AS ifood_category
    FROM google_orfao_dedup g
),

-- ═══════════════════════════════════════════════════════════════
-- PASSO 4: RFB órfãos 
-- ═══════════════════════════════════════════════════════════════
rfb_cnpj_usado_ifood AS (
    SELECT DISTINCT r.cnpj
    FROM rfb r
    INNER JOIN ifood i ON r.cnpj = i.cnpj
),

rfb_cnpj_usado_google AS (
    SELECT DISTINCT rfb_cnpj AS cnpj
    FROM google_orfao_dedup
    WHERE rfb_cnpj IS NOT NULL
),

rfb_orfao AS (
    SELECT r.*
    FROM rfb r
    WHERE r.cnpj NOT IN (SELECT cnpj FROM rfb_cnpj_usado_ifood WHERE cnpj IS NOT NULL)
      AND r.cnpj NOT IN (SELECT cnpj FROM rfb_cnpj_usado_google WHERE cnpj IS NOT NULL)
),

-- ═══════════════════════════════════════════════════════════════
-- PARTE C: Registros originados exclusivamente da RFB
-- ═══════════════════════════════════════════════════════════════
parte_rfb AS (
    SELECT
        NULL::VARCHAR                                                       AS sk_ifood,  -- Forçando Nulo para o UNION
        NULL::VARCHAR                                                       AS sk_google, -- Forçando Nulo para o UNION
        r.sk_rfb,                                                          
        COALESCE(r.name, r.real_name)                                       AS name,
        r.street_name                                                       AS address_street_name,
        CAST(r.street_number AS VARCHAR)                                    AS address_street_number,
        r.district                                                          AS address_district,
        r.city                                                              AS address_city,
        r.state                                                             AS address_state,
        r.cep                                                               AS address_zipcode,
        NULL::DOUBLE PRECISION                                              AS latitude,
        NULL::DOUBLE PRECISION                                              AS longitude,
        TRUE                                                                AS has_rfb,
        r.cnpj                                                              AS rfb_document_number,
        r.status                                                            AS rfb_situation,
        r.cnae_principal                                                    AS rfb_cnae,
        r.real_name                                                         AS rfb_social_name,
        r.name                                                              AS rfb_trading_name,
        NULL::VARCHAR                                                       AS google_cid,
        NULL::VARCHAR                                                       AS google_place_type,
        NULL::DOUBLE PRECISION                                              AS google_rating,
        NULL::DOUBLE PRECISION                                              AS google_rating_count,
        NULL::VARCHAR                                                       AS ifood_slug,
        NULL::VARCHAR                                                       AS ifood_category
    FROM rfb_orfao r
)

-- ═══════════════════════════════════════════════════════════════
-- RESULTADO FINAL: União das 3 partes
-- ═══════════════════════════════════════════════════════════════
SELECT *
,'ifood' as sistema_origem
 FROM parte_ifood
UNION ALL
SELECT * ,'google' as sistema_origem
FROM parte_google
UNION ALL
SELECT * ,'rfb' as sistema_origem
FROM parte_rfb