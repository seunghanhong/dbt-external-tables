{% macro render_from_context(name) -%}
{% set original_name = name %}
  {% if '.' in name %}
    {% set package_name, name = name.split(".", 1) %}
  {% else %}
    {% set package_name = none %}
  {% endif %}

  {% if package_name is none %}
    {% set package_context = context %}
  {% elif package_name in context %}
    {% set package_context = context[package_name] %}
  {% else %}
    {% set error_msg %}
        Could not find package '{{package_name}}', called by macro '{{original_name}}'
    {% endset %}
    {{ exceptions.raise_compiler_error(error_msg | trim) }}
  {% endif %}
  
    {{ return(package_context[name](*varargs, **kwargs)) }}

{%- endmacro %}

{% macro dropif(node) %}
    {{ adapter.dispatch('dropif')(node) }}
{% endmacro %}

{% macro default__dropif(node) %}
    {% set ddl %}
        drop table if exists {{source(node.source_name, node.name)}} cascade
    {% endset %}
    
    {{return(ddl)}}
{% endmacro %}

{% macro sqlserver__dropif(node) %}
    {% set ddl %}
        if object_id('{{ node.schema }}.{{ node.name }}') is not null
      {%- if node.external.synapse.load_type == 'copy' %}
	          drop table {{ node.schema }}.{{ node.name }}
      {%- else %}
	          drop external table {{ node.schema }}.{{ node.name }}
      {%- endif %}
    {% endset %}
    
    {{return(ddl)}}
{% endmacro %}
