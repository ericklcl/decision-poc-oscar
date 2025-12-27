{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_endereco AS (

    SELECT
      TRIM(INITCAP(bairro))       AS bairro,
      cep,
      idendereco,
      TRIM(INITCAP(localidade))   AS localidade,
      TRIM(INITCAP(logradouro))   AS logradouro,
      numero,
      TRIM(uf)                    AS uf

    FROM {{ ref('mstore_t_endereco') }}

)

SELECT *
FROM mstore_t_endereco