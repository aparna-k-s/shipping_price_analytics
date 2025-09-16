{% macro insert_csv_into_raw_table(schema, table, file_path=None, delimiter=',', header=true, auto_detect=true) %}
    {% set schema_table = schema + '.' + table %}
    {% set file_name = file_path.split('/')[-1] %}

    {% set insert_query %}
        INSERT INTO {{ schema_table }}
        select *, '{{ file_name }}' as file_name, current_timestamp as inserted_at
        from read_csv('{{ file_path }}')
        ;
    {% endset %}

    {% do run_query(insert_query) %}
    {% do log("Data from " ~ file_path ~ " inserted into " ~ schema_table, info=true) %}

{% endmacro %}
