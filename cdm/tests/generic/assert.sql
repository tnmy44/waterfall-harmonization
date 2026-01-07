{% test assert(model, expression, column_name) %}
{% set column_list = '*' if should_store_failures() else "1" %}
{% set expression_final = 'false' if expression == '' else expression %}
{% set column_name_final = '1' if column_name == None else column_name %}

select
    {{ column_list }}
from {{ model }}
where {{ column_name }} is null or ({{expression}}) is null or not( {{ expression }} )

{% endtest %}

 