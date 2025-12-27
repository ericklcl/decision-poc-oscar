{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_unidadenegocio AS (

    SELECT
      TRIM(INITCAP(descricao))    AS descricao,
      idunidadenegocio

    FROM {{ ref('mstore_t_unidadenegocio') }}

)

SELECT *
FROM mstore_t_unidadenegocio