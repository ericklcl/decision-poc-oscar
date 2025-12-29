{{
  config(
    materialized = 'table'
  )
}}
WITH metragem AS (
  SELECT 
    diretor,
    regional,
    sigla,
    nome_fantasia,
    bandeira,
    metragem
  FROM {{source('raw_input', 'sheets_metragem_lojas') }}
)
SELECT * FROM metragem
