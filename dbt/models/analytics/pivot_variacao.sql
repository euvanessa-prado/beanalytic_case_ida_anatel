{{ config(materialized='view', schema='ida') }}

{% set groups_tbl = run_query("select distinct nome_grupo from " ~ ref('dim_grupo_economico')) %}
{% if execute %}
  {% set groups = groups_tbl.columns[0].values() %}
{% else %}
  {% set groups = [] %}
{% endif %}

select
  ano_mes as "Mes",
  coalesce(round(avg(var_mercado)::numeric, 1), 0) as "Taxa de Variação Média"
  {% for g in groups %}
  , coalesce(round(max(case when nome_grupo = '{{ g }}' then diferenca end)::numeric, 1), 0) as "{{ g }}"
  {% endfor %}
from {{ ref('long_delta') }}
group by 1
order by "Mes"
