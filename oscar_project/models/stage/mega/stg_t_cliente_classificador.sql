{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_cliente_classificador AS (

    SELECT DISTINCT
        idcliente,
        SPLIT_PART(nome, ' ', 1) AS primeiro_nome,
        TO_CHAR(_dms_loaded_at, 'YYYY-MM-DD') AS dt_carga

    FROM {{ ref('mstore_t_cliente') }}
    where nome is not null

)

SELECT *
FROM mstore_t_cliente_classificador