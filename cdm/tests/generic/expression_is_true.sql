{% test expression_is_true(model, expression, column_name=None) %}

  {% set column_list = '*' if should_store_failures() else "1" %}

  select
    {{ column_list }}
  from {{ model }}
  {% if column_name is none %}
  where ({{ expression }}) is null or not({{ expression }})
  {%- else %}
  where ({{ column_name }} {{ expression }}) is null or not({{ column_name }} {{ expression }})
  {%- endif %}

{% endtest %}
