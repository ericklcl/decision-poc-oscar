{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_marca AS (

    SELECT
      TRIM(INITCAP(descricao))    AS descricao,
      idmarca,
      status

    FROM {{ ref('mstore_t_marca') }}

)

SELECT *
FROM mstore_t_marca