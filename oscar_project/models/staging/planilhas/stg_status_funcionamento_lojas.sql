{{
  config(
    materialized = 'table',
    tags="daily"
  ) 
}}


WITH status_funcionamento_lojas AS (
    SELECT 
    idestabelecimento,
    ds_sigla,
    ds_unidadenegocio,
    CASE 
      WHEN loja_ativa = 'SIM' THEN 'Aberta'
      WHEN loja_ativa = 'NAO' THEN 'Fechada'
      WHEN loja_ativa IS NULL THEN 'Fechada'
    ELSE 'Revisar'
    END AS loja_ativa
    FROM {{ ref('seed_status_funcionamento_lojas') }}
)

SELECT *
FROM status_funcionamento_lojas