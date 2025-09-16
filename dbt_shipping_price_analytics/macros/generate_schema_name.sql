{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- set schema_map = { 'stg': 'staging', 'raw': 'raw','final': 'final'} -%}

    {%- if custom_schema_name is none -%}

        {% set prefix = node.name.split('_')[0] %}
        {% if prefix in schema_map %}
            {{ schema_map[prefix] }}
        {% else %}
            {{ default_schema }}
        {% endif %}

    {%- else -%}

        {{ custom_schema_name }}

    {%- endif -%}

{%- endmacro %}