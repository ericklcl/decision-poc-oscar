{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_estadocivil AS (

    SELECT
      TRIM(INITCAP(descricao))    AS  descricao,
      idestadocivil

    FROM {{ ref('mstore_t_estadocivil') }}

)

SELECT *
FROM mstore_t_estadocivil