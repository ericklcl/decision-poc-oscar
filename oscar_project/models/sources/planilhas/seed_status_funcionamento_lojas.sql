{{
  config(
    materialized = 'table'
  ) 
}}


WITH status_funcionamento_lojas AS (
    SELECT
        idestabelecimento,
        ds_sigla,
        ds_unidadenegocio,
        loja_ativa
    FROM
		    {{ ref('aux_status_funcionamento_lojas') }}
)

SELECT *
FROM status_funcionamento_lojas
