{{
  config(
    materialized = 'table',
    tags="daily"
  ) 
}}

WITH orcamento AS (

     SELECT
      idcliente,
      idestabelecimento,
      idestabelecimentovendedor,
      idorcamento,
      idvendedor,
      data,
      status, 
      host,
      nk_id_pedido_vtex,
		  ds_canal_venda,
      valorbruto,
      valorfrete

    FROM {{ ref('stg_t_orcamento') }}
  
), orcamento_produto AS (

    SELECT
      idproduto,
      idorcamento,
      flg_status_produto,
      qtd_produto_vendido,
      vlr_venda,
      vlr_desconto,
      vlr_cupom,
      vlr_cashback,
      vlr_custo,
      vlr_desconto_funcionario,
      vlr_venda_liquida,
      vlr_kit_desconto,
      vlr_custo_bruto

    FROM {{ ref('stg_t_orcamentoproduto') }}

), estabelecimento AS (
  
    SELECT
      idestabelecimento,
      cnpj

    FROM {{ ref('stg_t_estabelecimento') }}

), funcionario AS (

    SELECT
      idfuncionario,
      idusuario,
      cpf

    FROM {{ ref('stg_t_funcionario') }}

), cliente AS (

    SELECT
      nk_idcliente,
      num_cpf,
      nk_idendereco 
    
    FROM {{ ref('dim_cliente') }}

), tabela_final AS (

    SELECT
      TO_CHAR(ven.data::DATE, 'YYYYmmdd')::INT                                                          AS sk_dt_venda,
      {{ dbt_utils.generate_surrogate_key (['ven.idvendedor', 'fun.idusuario', 'fun.cpf']) }}           AS sk_vendedor,
      {{ dbt_utils.generate_surrogate_key (['ven.idestabelecimento', 'est.cnpj']) }}                    AS sk_loja,
      {{ dbt_utils.generate_surrogate_key (['ven.idestabelecimentovendedor', 'est.cnpj']) }}            AS sk_loja_vendedor, 
      {{ dbt_utils.generate_surrogate_key (['cli.nk_idcliente', 'cli.num_cpf','cli.nk_idendereco']) }}  AS sk_cliente,
      {{ dbt_utils.generate_surrogate_key (['vo.idproduto']) }}                                         AS sk_produto,
      ven.idorcamento                                                                                   AS id_orcamento_dd,
      ven.status                                                                                        AS ds_status,               
      vo.qtd_produto_vendido,
      vo.vlr_venda,
      vo.vlr_venda_liquida,
      vo.vlr_desconto,
      vo.vlr_cupom,
      vo.vlr_cashback,
      vo.vlr_desconto_funcionario,
      vo.vlr_custo,
      vo.vlr_custo_bruto,
      vo.vlr_kit_desconto,
      vo.flg_status_produto,
      CASE WHEN NVL(ven.valorbruto,0) = 0 THEN 0 ELSE (vo.vlr_venda/ven.valorbruto)*ven.valorfrete END  AS vlr_frete,
      ven.nk_id_pedido_vtex                                                                             AS id_pedido_vtex_dd,
      ven.host                                                                                          AS nm_host,
      ven.ds_canal_venda                                                                                AS ds_canal_venda
    FROM orcamento ven
    LEFT JOIN orcamento_produto vo
      ON vo.idorcamento = ven.idorcamento
    LEFT JOIN estabelecimento est
      ON ven.idestabelecimento = est.idestabelecimento
    LEFT JOIN cliente cli
      ON ven.idcliente = cli.nk_idcliente
    LEFT JOIN funcionario fun
      ON ven.idvendedor = fun.idfuncionario
    WHERE status = 'O'  
    
)

SELECT *
FROM tabela_final