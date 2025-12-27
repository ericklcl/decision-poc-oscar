{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_cor AS (

    SELECT
      INITCAP(descricao)          AS descricao,
      idcor

    FROM {{ ref('mstore_t_cor') }}

)

SELECT *
FROM mstore_t_cor