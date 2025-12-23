{{
  config(
    materialized = 'table',
    tags="daily"
  ) 
}}

WITH metragem AS (
    SELECT
        sigla,
        metragem
    FROM {{ ref('sheets_metragem_lojas')}}

), emails_lojas AS (
    SELECT
        sigla,
        email_loja
    FROM {{ ref('seed_emails_lojas')}}

), estabelecimento AS (
    SELECT
        idendereco,
        idestabelecimento,
        idgrupof,
        status,
        razaosocial,
        nomefantasia,
        cnpj,
        inscricaoestadual,
        idunidadenegocio,
        sigla
    FROM 
		{{ ref('stg_t_estabelecimento') }}
), 

endereco AS (
    SELECT
        idendereco,
        cep,
        uf,
        localidade,
        TRANSLATE(
          localidade,
          'áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ',
          'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'
        ) AS localidade_padronizada,
        bairro,
        numero,
        logradouro
    FROM 
		{{ ref('stg_t_endereco') }}
),

unidade_negocio AS (
    SELECT
        idunidadenegocio,
        descricao AS descricao_unidadenegocio
    FROM 
		{{ ref('stg_t_unidadenegocio') }}  
), 

rua_shop AS (  
    SELECT *
    FROM {{ ref('seed_lojas_rua_shop')}}
), 

grupo_financeiro AS (
    SELECT *
    FROM {{ ref('stg_t_grupof') }}
), 

gestores AS (  
    SELECT
		sk_loja,
		nk_idestabelecimento,
		regional,
		diretor
    FROM 
		{{ ref('dim_gestores') }}
), 

status_funcionamento_lojas AS (
	SELECT
		sfl.*
	FROM
		{{ ref('stg_status_funcionamento_lojas') }} as sfl
), tabela_intermediaria AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['est.idestabelecimento', 'est.cnpj'])}}             AS sk_loja,
        {{ dbt_utils.generate_surrogate_key(['ende.uf', 'ende.localidade_padronizada'])}}        AS sk_localidade,
        est.idestabelecimento                                                        AS nk_idestabelecimento,
        est.status                                                                   AS ds_status,                     
        est.razaosocial                                                              AS nm_razaosocial,
        est.nomefantasia                                                             AS nm_nomefantasia,
        est.cnpj                                                                     AS num_cnpj,
        est.inscricaoestadual                                                        AS num_inscricaoestadual,
        est.idunidadenegocio                                                         AS nk_idunidadenegocio,
        CASE
			WHEN est.idestabelecimento  = '371' THEN '001 - O&A GUARARAPES'
			WHEN est.idestabelecimento  = '372' THEN '002 - O&A SHOPPING CARUARU'
			WHEN est.idestabelecimento  = '373' THEN '004 - O&A CENTRO CARUARU'
			WHEN est.idestabelecimento  = '374' THEN '005 - O&A PRAZERES'
			WHEN est.idestabelecimento  = '375' THEN '003 - O&A TACARUNA'
        	ELSE est.sigla
        END                                                                          AS ds_sigla,
        ende.cep                                                                     AS num_cep,
        ende.uf                                                                      AS ds_uf,
        ende.localidade                                                              AS ds_localidade,
        ende.bairro                                                                  AS ds_bairro,
        ende.numero                                                                  AS num_numero,
        ende.logradouro                                                              AS ds_logradouro,
        rs.loja                                                                      AS ds_rua_shop,
        uni.descricao_unidadenegocio                                                 AS ds_unidadenegocio,
    CASE
        WHEN est.sigla IN ('338', '343', '345', '349', '353', '356', '362', '348 ') THEN 'Grupo Paquetá Esportes'
        ELSE uni.descricao_unidadenegocio
    END                                                                              AS ds_bandeira,
        gf.descricao                                                                 AS grupo_financeiro,
        ges.diretor                                                                  AS diretor,
        ges.regional                                                                 AS regional,
		sfl.loja_ativa																 AS status_funcionamento_loja,
    CASE
        WHEN uni.descricao_unidadenegocio IN ('Grupo Gaston', 'Grupo Paquetá')             THEN 'Varejo Sul'
        WHEN uni.descricao_unidadenegocio IN ('Grupo Jô Calçados', 'Grupo Oscar Calçados') THEN 'Varejo Oscar'
        WHEN uni.descricao_unidadenegocio = 'Grupo Carioca Calçados'                       THEN 'Varejo Carioca'
    ELSE 'Demais Bandeiras'
    END                                                                              AS ds_varejo

  	FROM 
  		estabelecimento est
	LEFT JOIN endereco 						ende ON est.idendereco = ende.idendereco
	LEFT JOIN unidade_negocio 				uni ON est.idunidadenegocio = uni.idunidadenegocio
	LEFT JOIN rua_shop 						rs ON est.idestabelecimento = rs.idestabelecimento
	LEFT JOIN grupo_financeiro 				gf ON est.idgrupof = gf.idgrupof
	LEFT JOIN gestores 						ges ON est.idestabelecimento = ges.nk_idestabelecimento
	LEFT JOIN status_funcionamento_lojas	sfl ON est.idestabelecimento = sfl.idestabelecimento
), tabela_final AS (
    SELECT
      ti.*,
      mt.metragem,
      el.email_loja
    FROM tabela_intermediaria ti
    LEFT JOIN metragem mt ON LPAD(REGEXP_REPLACE(TRIM(ti.ds_sigla), '[^0-9]', ''), 3, '0') = LPAD(CAST(mt.sigla AS VARCHAR(26)), 3, '0')
    LEFT JOIN emails_lojas el ON LPAD(REGEXP_REPLACE(TRIM(ti.ds_sigla), '[^0-9]', ''), 3, '0') = LPAD(CAST(el.sigla AS VARCHAR(26)), 3, '0')
)
SELECT DISTINCT * FROM tabela_final