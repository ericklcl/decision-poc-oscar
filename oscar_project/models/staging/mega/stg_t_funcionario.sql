{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_funcionario AS (

    SELECT
      cpf,
      dataadmissao::DATE                      AS dataadmissao,
      datanascimento::DATE                    AS datanascimento,
      idatividade,
      idendereco,
      idestabelecimento,
      idestadocivil,
      idfuncionario,
      idusuario,
      TRIM(INITCAP(nome))                     AS nome,
      TRIM(INITCAP(nomereduzido))             AS nomereduzido,
      TRIM(sexo)                              AS sexo,
      status

    FROM {{ ref('mstore_t_funcionario') }}

)

SELECT *
FROM mstore_t_funcionario