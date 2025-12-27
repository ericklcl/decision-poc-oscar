{{
  config(
    materialized = 'table'
  ) 
}}

WITH genero as (

    SELECT
      idcliente,
      classificacao

    FROM {{ ref('mstore_gender_classifier') }} 

), mstore_t_cliente AS (

    SELECT
      cpfcnpj,
      datanascimento,
      idcliente,
      idendereco,
      nomereduzido,
      cadastradocards,
      divulgaremail,
      idtipocadastro,
      codigoprodutojo,
      codigoprodutooscar,
      codigoprodutoabys,
      codigoprodutocarioca

    FROM {{ ref('mstore_t_cliente') }}

), tabela_final as (

    SELECT 
      tc.cpfcnpj,
      tc.datanascimento,
      tc.idcliente,
      tc.idendereco,
      tc.nomereduzido,
      tc.cadastradocards,
      tc.divulgaremail,
      CASE 
        WHEN classificacao = 'F' THEN 'Feminino'
        WHEN classificacao = 'M' THEN 'Masculino'
        ELSE 'Desconhecido'
      END AS genero,
      idtipocadastro,
      codigoprodutojo,
      codigoprodutooscar,
      codigoprodutoabys,
      codigoprodutocarioca

    FROM mstore_t_cliente tc 
    LEFT JOIN genero gn 
      ON gn.idcliente = tc.idcliente

)

SELECT *
FROM tabela_final