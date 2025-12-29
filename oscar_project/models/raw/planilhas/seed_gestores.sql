{{
  config(
    materialized = 'table',
    tags="daily"
  ) 
}}

WITH gestores AS (

    SELECT  
      grupo,
      bandeira,
      diretor,
      regional,
      codigo_loja,
      descricao,
      loja,
      idestabelecimento
    FROM {{ ref('aux_gestores') }}

)

SELECT *
FROM gestores