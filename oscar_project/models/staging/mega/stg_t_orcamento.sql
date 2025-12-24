{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_orcamento AS (

    SELECT
      idcliente,
      idestabelecimento,
      idlojavendedor                    AS idestabelecimentovendedor,
      idorcamento,
      idvendedor,
      data::DATE                        AS data,
      status, 
      descontopercentual,
      descontovalor,
      totaldepecas,   
      RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(REPLACE(observacao, ' - ObservaÃ§Ã£o: ', ''), 'Pedido:', ''),'PEDIDO',''), '-', ''))) AS nk_id_pedido_vtex,
      integrador AS host,
      CASE WHEN COALESCE(orcamentowebstoreflag, 'N') = 'S' THEN 'Online'
		    ELSE 'Offline'
		      END AS ds_canal_venda,
      valorfrete,
      valorbruto,
      idvenda,
      tempo,
      valor,
      idusuario,
      idusuarioautorizacao,
      idusuariocancelamento,
      datacancelamento,
      idpontovenda,
      valefuncionario,
      nomecliente,
      idcondicaopagamento,
      perfilcliente,
      idtroca,
      observacaocancelamento,
      acrescimopercentual,
      codigocondicaopagamentocards,
      descricondicaopagamentocards,
      retiradodoestoque,
      idcaixa,
      idusuariocaixa,
      dataconfirmacao,
      codigoab,
      cpfcnpj,
      idultimohistoricospcscore,
      idultimohistoricopesquisaspc,
      ticket,  
      statusconferencia,  
      idusuarioconferencia,  
      valorcupomdesconto,  
      chaveseguranca,  
      codigovalefuncionario,  
      cpfcnpjdependente, 
      valorkitdesconto,  
      idcupomdescontocliente,  
      valorcashback,  
      autoatendimento,  
      idlojavendedor,
      orcamentowebstoreflag,
      observacao,
      pedidoecommerce

    FROM {{ ref('mstore_t_orcamento') }}

)

SELECT *
FROM mstore_t_orcamento