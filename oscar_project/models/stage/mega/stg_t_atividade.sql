{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_atividade AS (

    SELECT
      TRIM(INITCAP(descricao))        AS descricao,
      idatividade

    FROM {{ ref('mstore_t_atividade') }}
)

SELECT *
FROM mstore_t_atividade