{{
  config(
    materialized = 'view'
  ) 
}}

WITH mstore_t_grupof AS (

    SELECT
      idgrupof,
	    initcap(trim(descricao)) as descricao,
	    status

    FROM {{ ref('mstore_t_grupof') }}

)

SELECT *
FROM mstore_t_grupof