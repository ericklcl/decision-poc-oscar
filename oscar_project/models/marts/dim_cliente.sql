{{
  config(
    materialized = 'table',
    tags="daily"
  ) 
}}

WITH cliente AS (

    SELECT
      idcliente,
      idendereco,
      cpfcnpj,
	    nomereduzido,		
      datanascimento,
      genero,
      cadastradocards,
      divulgaremail,
      idtipocadastro,
      codigoprodutojo,
      codigoprodutooscar,
      codigoprodutoabys,
      codigoprodutocarioca

    FROM {{ ref('stg_t_cliente') }}
  
), orcamento AS (

    SELECT
      idcliente,
      idestabelecimento

    FROM {{ ref('stg_t_orcamento') }}
  
), contato_cliente AS (
  
    SELECT 
	    DISTINCT idcliente,
	    CASE
	    	WHEN idtipocontato = 4 AND ddd IS NOT NULL THEN CONCAT(ddd,descricao)
	      WHEN idtipocontato = 4 and ddd IS NULL THEN descricao
	     	ELSE NULL
      END AS celular,
	    CASE
		    WHEN idtipocontato = 5 THEN descricao 
		 	  ELSE NULL
      END AS email

    FROM {{ ref('stg_t_contatocliente') }}
    WHERE idtipocontato IN (4,5)
      AND idcliente IS NOT NULL

), endereco AS (

    SELECT
      idendereco,
      cep,
      bairro,
      logradouro,
      numero,
      uf,
      CASE
        WHEN localidade = 'Nao informado' THEN NULL
        ELSE localidade 
      END AS cidade,
      CASE 
        WHEN uf = 'BR' THEN NULL 
        ELSE uf 
      END AS estado

    FROM {{ ref('stg_t_endereco') }}

), funcionario AS (

    SELECT
      idfuncionario,
      status,
      nomereduzido,
      idendereco,
      CASE 
        WHEN idfuncionario IS NOT NULL AND status = 'A' THEN true 
        ELSE false 
      END	e_funcionario,     
      CASE 
        WHEN cpf = '00000000000' THEN LPAD(IDFUNCIONARIO || IDUSUARIO, 11,0)
        ELSE cpf
      END                                                           AS cpf

    FROM {{ ref('stg_t_funcionario') }}

), canal_venda AS (

    SELECT
      idcliente,
      ds_canal_venda

    FROM {{ ref('stg_t_canal_venda') }}

), dedup_cliente AS (

    SELECT 
      cli.idcliente                                                 AS idcliente,
      cli.cpfcnpj,
      cli.cadastradocards,
      cli.divulgaremail,
	    MAX(cli.nomereduzido) 		                                    AS nomereduzido,
	    MAX(cli.genero) 				                                      AS genero,
	    CASE
        WHEN LEN(MAX(cli.datanascimento)::DATE) > 10 THEN NULL
        ELSE MAX(cli.datanascimento)
      END                                                           AS datanascimento,
	    cli.idendereco		                                            AS idendereco,
	    MAX(ccli.email)				                                        AS email,
	    MAX(ccli.celular)			                                        AS celular,
      idtipocadastro,
      codigoprodutojo,
      codigoprodutooscar,
      codigoprodutoabys,
      codigoprodutocarioca
	   
    FROM cliente cli
    JOIN orcamento orca 			
      ON orca.idcliente = cli.idcliente 
    LEFT JOIN contato_cliente ccli
      ON ccli.idcliente = cli.idcliente
    GROUP BY cli.idcliente, 
      cli.cpfcnpj, 
      cli.idendereco, 
      cli.cadastradocards, 
      cli.divulgaremail,
      cli.idtipocadastro,
      cli.codigoprodutojo,
      cli.codigoprodutooscar,
      cli.codigoprodutoabys,
      cli.codigoprodutocarioca

), tabela_final AS (

    SELECT 
      {{ dbt_utils.generate_surrogate_key(['dcli.idcliente', 'dcli.cpfcnpj','dcli.idendereco']) }}                              AS sk_cliente,
      dcli.idcliente                                                                                                            AS nk_idcliente,
      dcli.cpfcnpj									                                                                                            AS num_cpf,
		  dcli.nomereduzido							                                                                                            AS nm_nome,
		  dcli.genero                                                                                                               AS ds_genero,
		  dcli.datanascimento::DATE                                                                                                 AS dt_datanascimento,
      EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM dcli.datanascimento::DATE)                                            AS num_idade,
      CASE
        WHEN num_idade IS NULL THEN NULL 
        WHEN num_idade < 18 THEN '-18'
        WHEN num_idade >= 18 AND num_idade <= 25 THEN '18 - 25'
        WHEN num_idade >= 26 AND num_idade <= 35 THEN '26 - 35'
        WHEN num_idade >= 36 AND num_idade <= 45 THEN '36 - 45'
        WHEN num_idade >= 46 AND num_idade <= 60 THEN '46 - 60'
        ELSE '+60'
      END                                                                                                                       AS ds_faixa_etaria,		
      CASE
        WHEN num_idade IS NULL THEN NULL 
        WHEN num_idade < 18 THEN 1
        WHEN num_idade >= 18 AND num_idade <= 25 THEN 2
        WHEN num_idade >= 26 AND num_idade <= 35 THEN 3
        WHEN num_idade >= 36 AND num_idade <= 45 THEN 4
        WHEN num_idade >= 46 AND num_idade <= 60 THEN 5
        ELSE 6
      END                                                                                                                       AS ordem_faixa_et,				 									
		  dcli.celular                                                                                                              AS num_celular,																				
		  dcli.email                                                                                                                AS ds_email,	
      dcli.idendereco                                                                                                           AS nk_idendereco,																			
		  ende.cep                                                                                                                  AS num_cep,
      ende.logradouro                                                                                                           AS ds_logradouro,
      ende.numero                                                                                                               AS num_numero,
      ende.bairro                                                                                                               AS ds_bairro,
      ende.cidade                                                                                                               AS ds_cidade,
		  ende.estado                                                                                                               AS ds_estado,											
      cv.ds_canal_venda                                                                                                         AS ds_canal_venda,
      dcli.cadastradocards,
      dcli.divulgaremail,
      dcli.idtipocadastro,
      dcli.codigoprodutojo,
      dcli.codigoprodutooscar,
      dcli.codigoprodutoabys,
      dcli.codigoprodutocarioca,
      CASE
        WHEN dcli.cadastradocards = 'S' AND dcli.idtipocadastro = 4 
        AND(
            dcli.codigoprodutojo         IS NOT NULL 
            OR dcli.codigoprodutooscar   IS NOT NULL 
            OR dcli.codigoprodutoabys    IS NOT NULL 
            OR dcli.codigoprodutocarioca IS NOT NULL )
        THEN 'S'
      ELSE 'N'
      END                                                                                                                      AS ds_cliente_festcard                                                                                           

    FROM dedup_cliente dcli 
    LEFT JOIN endereco ende  
      ON ende.idendereco =	dcli.idendereco
    LEFT JOIN canal_venda cv
      ON cv.idcliente = dcli.idcliente
      
)

SELECT *
FROM tabela_final 