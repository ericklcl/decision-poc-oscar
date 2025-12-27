{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_departamento AS (

    SELECT
      iddepartamento,
      TRIM(INITCAP(descricao))    AS descricao

    FROM {{ ref('mstore_t_departamento') }}

)

SELECT *
FROM mstore_t_departamento