{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_contatocliente AS (

    SELECT
      ddd,
      descricao,
      idcliente,
      idtipocontato

    FROM {{ ref('mstore_t_contatocliente') }}

)

SELECT *
FROM mstore_t_contatocliente