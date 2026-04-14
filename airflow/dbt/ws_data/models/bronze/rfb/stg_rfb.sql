{{ config(
    materialized='view',
    schema='bronze'
)}}

WITH source AS (
    SELECT * FROM {{source('raw_rfb_source','raw_rfb')}}
)

select 
 cast(cnpj as varchar) as cnpj
 ,cast(razao_social as varchar) as razao_social
 ,cast(nome_fantasia as varchar) as nome_fantasia
 ,cast(logradouro as varchar) as logradouro
 ,cast(numero as varchar) as numero
 ,cast(complemento as varchar) as complemento
 ,cast(bairro as varchar) as bairro
 ,cast(municipio as varchar) as municipio
 ,cast(uf as varchar) as uf
 ,cast(cep as varchar) as cep
 ,cast(cnae_principal as varchar) as cnae_principal
 ,cast(descricao_cnae as varchar) as descricao_cnae
 ,cast(situacao_cadastral as varchar) as situacao_cadastral
 ,cast(data_abertura as timestamp) as data_abertura
 ,CURRENT_TIMESTAMP AS data_carga_bronze
 ,'rfb' as sistema_origem

 from source
 