{{ 
	config(
		materialized = 'table'
		, tags = "daily"
	)
}}


WITH produto AS (
	SELECT
		nome,
		precovenda,
		precobase,
		primeiroprecovenda,
		status,
		foradelinha,
		ecommerce,
		idproduto,
		idreferenciaproduto,
		idcor,
		idtamanho,
		iddepartamento,
		closeout,
		ean,
		idsituacaoproduto
	FROM
		{{ ref('stg_t_produto') }}
),
referenciaproduto AS (
	SELECT
		idreferenciaproduto,
		idmarca,
		idprodutoespecifico,
		idprodutogenerico,
		idsecao,
		idmaterial,
		idgrupoproduto,
		idcomprador,
		referencia,
		ncm
	FROM
		{{ ref('stg_t_referenciaproduto') }}
),
cor AS (
	SELECT
		idcor,
		descricao AS descricao_cor
	FROM
		{{ ref('stg_t_cor') }}
),
produto_da_nota AS (
	SELECT
		sk_idprodutonota,
		idnotafiscal
	FROM
		{{ ref('stg_t_produtodanota') }}
),
nota_fiscal AS (
	SELECT
		idnotafiscal,
		idfornecedor
	FROM
		{{ ref('stg_t_notafiscal') }}
),
fornecedor AS (
	SELECT
		idfornecedor,
		nome,
		nomereduzido
	FROM
		{{ ref('stg_t_fornecedor') }}
),
tamanho AS (
	SELECT
		idtamanho,
		descricao AS descricao_tamanho
	FROM
		{{ ref('stg_t_tamanho') }}
),
departamento AS (
	SELECT
		iddepartamento,
		descricao AS descricao_departamento
	FROM
		{{ ref('stg_t_departamento') }}
),
produtogenerico AS (
	SELECT
		idprodutogenerico,
		descricao AS descricao_produtogenerico
	FROM
		{{ ref('stg_t_produtogenerico') }}
),
produtoespecifico AS (
	SELECT
		idprodutoespecifico,
		descricao AS descricao_produtoespecifico
	FROM
		{{ ref('stg_t_produtoespecifico') }}
),
marca AS (
	SELECT
		idmarca,
		descricao AS descricao_marca
	FROM
		{{ ref('stg_t_marca') }}
),
secao AS (
	SELECT
		idsecao,
		descricao
	FROM
		{{ ref('stg_t_secao') }}
),
grupoproduto AS (
	SELECT
		idgrupoproduto,
		descricao
	FROM
		{{ ref('stg_t_grupoproduto') }}
),
marcafornecedor AS (
	SELECT
		idfornecedor,
		idmarcafornecedor,
		idmarca
	FROM
		{{ ref('stg_t_marcafornecedor') }}
),
metaprodutosportal AS (
	SELECT
		idgrupoproduto,
		iddepartamento,
		idsecao,
		idfamilia
	FROM
		{{ ref('stg_t_metaprodutosportal') }}
),
material AS (
	SELECT
		idmaterial,
		descricao
	FROM
		{{ ref('stg_t_material') }}
),
situacao_produto AS (
	SELECT
	idsituacaoproduto,
    descricao
  FROM {{ ref('stg_t_situacaoproduto') }}
),
tabela_intermediaria AS (
	SELECT
		{{ dbt_utils.generate_surrogate_key(['p.idproduto']) }} AS sk_produto,
		--{{ dbt_utils.generate_surrogate_key(['m.descricao_marca', 'rp.referencia', 'c.descricao_cor', 'mt.descricao'])}} AS sk_produto_curva,
		p.idproduto AS nk_produto,
		p.nome AS nm_nome,
		p.ean,
		Nvl(
			(
				SELECT
					DISTINCT D.descricao_departamento
				FROM
					DEPARTAMENTO D,
					METAPRODUTOSPORTAL MP
				WHERE
					MP.IDGRUPOPRODUTO = RP.IDGRUPOPRODUTO
					AND MP.IDSECAO = RP.IDSECAO
					AND MP.IDFAMILIA = RP.IDPRODUTOGENERICO
					AND D.IDDEPARTAMENTO = MP.IDDEPARTAMENTO
			),
			'OUTROS'
		) AS ds_departamento,
		gp.descricao AS ds_grupo_produto,
		pg.descricao_produtogenerico AS ds_familia,
		pe.descricao_produtoespecifico AS ds_subfamilia,
		se.descricao AS ds_secao,
		m.descricao_marca AS ds_marca,
		mt.descricao AS ds_material,
		c.descricao_cor AS ds_cor,
		t.descricao_tamanho AS ds_tamanho,
		rp.referencia AS ds_referencia,
		p.precovenda AS vl_precovenda,
		p.precobase AS vlr_precobase,
		p.primeiroprecovenda AS vlr_primeiroprecovenda,
		p.status AS ds_status,
		p.foradelinha AS ds_foradelinha,
		p.closeout AS ds_closeout,
		p.ecommerce AS ds_ecommerce,
		rp.idcomprador AS nk_idcomprador,
		mt.descricao AS material,
		CASE
			WHEN m.descricao_marca IN (
				'Kult',
				'Lore',
				'Luiza Camargo',
				'M Shuz',
				'Osc',
				'Cazzac',
				'Constantino',
				'Savona',
				'Paganezzi',
				'Done Head',
				'Funfeet',
				'Disport',
				'M Shuz Kids'
			) THEN 'S'
			ELSE 'N'
		END AS ds_exclusivo,
		(
			CASE
				WHEN p.foradelinha = 'S'
				AND p.closeout = 'N' THEN 'FL'
				WHEN p.foradelinha = 'N'
				AND p.closeout = 'S' THEN 'CO'
				WHEN p.foradelinha = 'N'
				AND p.closeout = 'N' THEN 'NORMAL'
			END
		) AS flg_status_produto_antiga,
		sp.descricao AS flg_status_produto,
		rp.referencia || '-' || c.descricao_cor || '-' || mt.descricao AS desc_ref_cor_material,
		ncm

	FROM
		produto p
		LEFT JOIN referenciaproduto rp ON p.idreferenciaproduto = rp.idreferenciaproduto
		LEFT JOIN marca m ON rp.idmarca = m.idmarca
		LEFT JOIN tamanho t ON p.idtamanho = t.idtamanho
		LEFT JOIN produtoespecifico pe ON rp.idprodutoespecifico = pe.idprodutoespecifico
		LEFT JOIN produtogenerico pg ON rp.idprodutogenerico = pg.idprodutogenerico
		LEFT JOIN secao se ON rp.idsecao = se.idsecao
		LEFT JOIN cor c ON p.idcor = c.idcor
		LEFT JOIN grupoproduto gp ON rp.idgrupoproduto = gp.idgrupoproduto
		LEFT JOIN material mt ON mt.idmaterial = rp.idmaterial
		LEFT JOIN situacao_produto sp ON sp.idsituacaoproduto = p.idsituacaoproduto
), tabela_final AS (
SELECT
	ti.*,
	{{ dbt_utils.generate_surrogate_key(['ti.ds_marca', 'ti.ds_referencia', 'ti.ds_cor', 'ti.material'])}} AS sk_produto_curva
FROM tabela_intermediaria ti
)
SELECT * FROM tabela_final