{{
  config(
    materialized = 'table',
    tags="daily"
  )
}}

WITH orcamento_troca AS (

    SELECT distinct 
      idtroca,
      idorcamento,
      idorcamentotroca

    FROM {{ ref( 'stg_t_orcamentotroca' ) }}

), orcamento AS (

    SELECT distinct
      idorcamento,
      status,
      data,
      idvendedor,
      idestabelecimento,
      idestabelecimentovendedor,
      idcliente,
      nk_id_pedido_vtex,
      host,
      ds_canal_venda

    FROM  {{ ref( 'stg_t_orcamento') }}
    WHERE status = 'O'

), produto_troca AS (

    SELECT distinct
      idtroca,
      quantidade,
      customedioproduto,
      idproduto,
      idprodutotroca,
      valor,
      custobruto,
      flg_status_produto

    FROM {{ ref( 'stg_t_produtotroca' )}}

), troca AS (

    SELECT distinct
      idtroca,
      quantidadeprodutos,
      idpontovenda,
      valortotalprodutos,
      idestabelecimento,
      status,
      idorcamento,
      idcliente,
      idnfe

    FROM {{ ref( 'stg_t_troca' ) }}
    WHERE status  <> 'C'

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
      o.idorcamento                                                                                               AS id_orcamento_dd,
      o.status                                                                                                    AS ds_status,
      TO_CHAR(o.data, 'YYYYmmdd')::INT                                                                            AS sk_dt_troca,
      {{ dbt_utils.generate_surrogate_key (['o.idvendedor', 'fun.idusuario', 'fun.cpf']) }}                       AS sk_vendedor,
      {{ dbt_utils.generate_surrogate_key (['o.idestabelecimento', 'est.cnpj']) }}                                AS sk_loja,
      {{ dbt_utils.generate_surrogate_key (['o.idestabelecimentovendedor', 'est.cnpj']) }}                        AS sk_lojavendedor,      
      {{ dbt_utils.generate_surrogate_key (['cli.nk_idcliente', 'cli.num_cpf','cli.nk_idendereco']) }}            AS sk_cliente,
      {{ dbt_utils.generate_surrogate_key (['pt.idproduto']) }}                                                   AS sk_produto,
      pt.quantidade                                                                                               AS qtd_produto_trocado,
      pt.valor                                                                                                    AS vlr_venda_troca,
      (pt.customedioproduto * pt.quantidade)                                                                      AS vlr_venda_custo,
      (pt.custobruto * pt.quantidade)                                                                             AS vlr_custo_bruto,
      pt.flg_status_produto                                                                                       AS flg_status_produto,
      o.nk_id_pedido_vtex                                                                                         AS id_pedido_vtex_dd,
      o.host                                                                                                      AS nm_host,
      o.ds_canal_venda
    FROM orcamento o
    INNER JOIN orcamento_troca ot
      ON o.idorcamento = ot.idorcamento
    INNER JOIN troca t
      ON t.idtroca = ot.idtroca
    LEFT JOIN produto_troca pt
      ON pt.idtroca = t.idtroca
    LEFT JOIN funcionario fun
      ON o.idvendedor = fun.idfuncionario
    LEFT JOIN cliente cli
      ON o.idcliente = cli.nk_idcliente
    LEFT JOIN estabelecimento est
      ON o.idestabelecimento = est.idestabelecimento
      
)

SELECT *
FROM tabela_final