{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {# Se o modelo não tiver um schema customizado, usa o padrão (bronze) #}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {# Se tiver um schema customizado (ex: 'silver'), usa APENAS ele e ignora o prefixo #}
    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}