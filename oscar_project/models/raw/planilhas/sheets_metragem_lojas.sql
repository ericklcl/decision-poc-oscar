{{
  config(
    materialized = 'table'
  )
}}
WITH metragem AS (
  SELECT 
    *
  FROM {{source('raw_input', 'sheets_metragem_lojas') }}
)
SELECT * FROM metragem
