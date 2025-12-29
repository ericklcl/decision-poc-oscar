{{
  config(
    materialized = 'table',
    tags="daily"
  ) 
}}

WITH funcionario AS (

    SELECT
      idfuncionario,
      idusuario,
      idatividade,
      idestabelecimento,
      idendereco,
      idestadocivil,
      nome, 
      cpf,
      CASE
        WHEN sexo = 'F' THEN 'Feminino'
        WHEN sexo = 'M' THEN 'Masculino'
        ELSE 'Desconhecido'
      END AS genero,
      DATE_PART('year', GETDATE()) - DATE_PART('year', datanascimento::DATE)        AS idade_atual,
      DATE_PART('year', GETDATE()) - DATE_PART('year', dataadmissao::DATE)          AS tempo_de_empresa

    FROM {{ ref('stg_t_funcionario') }}

), atividade AS (

    SELECT
      idatividade,
      descricao AS descricao_atividade
      
    FROM {{ ref('stg_t_atividade') }}

), estabelecimento AS (

    SELECT
      idestabelecimento,
      nomefantasia AS nome_fantasia_estabelecimento
      
    FROM {{ ref('stg_t_estabelecimento') }}

), estadocivil AS (

    SELECT
      idestadocivil,
      descricao AS descricao_estadocivil
      
    FROM {{ ref('stg_t_estadocivil') }}

), endereco AS (

    SELECT
      idendereco,
      cep,
      uf,
      bairro,
      numero,
      logradouro
      
    FROM {{ ref('stg_t_endereco') }}

), tabela_final AS (

    SELECT
      {{dbt_utils.generate_surrogate_key(['f.idfuncionario','f.idusuario', 'f.cpf'])}} AS sk_funcionario,
      f.idfuncionario                       AS nk_idfuncionario,
      f.idusuario                           AS id_usuario,
      f.nome                                AS nm_nome,
      f.cpf                                 AS num_cpf,
      f.idade_atual                         AS num_idade_atual,
      f.genero                              AS ds_genero,
      ec.descricao_estadocivil              AS ds_estadocivil,
      atv.descricao_atividade               AS ds_atividade,
      e.uf                                  AS ds_uf,
      e.bairro                              AS ds_bairro,
      e.logradouro                          AS ds_logradouro,
      e.numero                              AS nm_numero,
      e.cep                                 AS num_cep,
      est.nome_fantasia_estabelecimento     AS nm_fantasia_estabelecimento,
      f.tempo_de_empresa                    AS num_tempo_de_empresa
      
    FROM funcionario f
    LEFT JOIN atividade atv
      ON f.idatividade = atv.idatividade
    LEFT JOIN estabelecimento est
      ON f.idestabelecimento = est.idestabelecimento
    LEFT JOIN estadocivil ec
      ON f.idestadocivil = ec.idestadocivil
    LEFT JOIN endereco e
      ON f.idendereco = e.idendereco

)

SELECT *
FROM tabela_final