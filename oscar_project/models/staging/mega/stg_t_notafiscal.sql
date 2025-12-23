{{
  config(
    materialized = 'table'
  )
}}

SELECT
	numeronota
	, valortotalprodutos
	, idestabelecimento
	, idnotafiscal
	, idfornecedor
	, data::DATE
	, dataemissao::DATE
	, dataconfirmacao::DATE
	, valortotalnota
	, status
	, porcentagemdesconto
	, cnpj
	, valoricms
FROM
	{{ ref('mstore_t_notafiscal') }}