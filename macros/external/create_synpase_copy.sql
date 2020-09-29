{% macro synapse_create_empty_table(source_node) %}
    {%- set relation = source(source_node.source_name, source_node.name) %}
    {%- set columns = source_node.columns.values() %}
    
    {% if not adapter.check_schema_exists(relation.database, relation.schema) %}
        {% do create_schema(relation) %}
    {% endif %}

    create table {{relation}} (
        {% if columns|length == 0 %}
            value variant,
        {% else -%}
        {%- for column in columns -%}
            {{column.name}} {{column.data_type}}{{ ',' if not loop.last}}
        {% endfor -%}
        {% endif %}
    );

    {# extend metadata here #}
{% endmacro %}

{% macro synapse_get_copy_sql(source_node, table_exists) %}
    {%- set relation = source(source_node.source_name, source_node.name) %}
    {%- set synapse = source_node.external.synapse %}
    {%- set truncate = synapse.get('truncate', false) if synapse is mapping %}

    {%- if truncate and table_exists %}
        {%- do truncate_relation(relation) %}
    {%- endif %}

    copy into {{relation}}
    from '{{source_node.external.location}}'
    with
    (
    {%- for key, value in synapse.items() -%}
        {% if key in ['credential', 'errorfile_credential'] %}
        {{key}} = (
            {%- for cred_key, cred_val in value.items() -%}
            {{cred_key}}='{{cred_val}}'{{', ' if not loop.last}}
            {%- endfor -%}
        ){{',' if not loop.last}}
        {%- elif key in ['maxerrors', 'firstrow'] %}
        {{key}} = {{value}}{{',' if not loop.last}}
        {%- elif key not in ['load_type', 'truncate'] %}
        {{key}} = '{{value}}'{{',' if not loop.last}}
        {%- endif %}
    {%- endfor %}
    )

{% endmacro %}
