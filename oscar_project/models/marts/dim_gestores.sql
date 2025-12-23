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
      "codigo loja"                                                            AS cod_loja,
      descricao                                                                AS nm_loja,
      LPAD(CAST("codigo loja"  AS VARCHAR), 3, '0')                            AS sigla,
      loja                                                                     AS store,
      idestabelecimento
    FROM {{ ref('stg_seed_gestores') }}

), estabelecimento as (

  SELECT
    sigla,
    idestabelecimento,
    nomefantasia,
    razaosocial,
    cnpj

    from  {{ ref('stg_t_estabelecimento') }}

), tabela_final AS (

  SELECT 
    {{ dbt_utils.generate_surrogate_key (['e.idestabelecimento', 'e.cnpj']) }}                        AS sk_loja,
    {{ dbt_utils.generate_surrogate_key(['e.idestabelecimento','e.cnpj'])}}                           AS sk_gestor,
    e.idestabelecimento                                                                               AS nk_idestabelecimento,
    g.sigla                                                                                           AS nk_sigla,
    store                                                                                             AS loja,
    nm_loja                                                                                           AS nm_loja,
    grupo,                          
    bandeira,                         
    NVL(diretor, 'NA', diretor)                                                                       AS diretor,
    NVL(regional, 'NA', regional)                                                                     AS regional

  FROM gestores g 
  LEFT JOIN estabelecimento e ON
    g.idestabelecimento = e.idestabelecimento
)
SELECT *
FROM tabela_final