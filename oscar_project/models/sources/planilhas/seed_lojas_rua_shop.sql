{{
  config(
    materialized = 'table'
  ) 
}}

WITH infos_shop_rua_lojas AS (

    SELECT  
      *

    FROM {{ ref('aux_shop_rua_lojas') }}

)

SELECT *
FROM infos_shop_rua_lojas