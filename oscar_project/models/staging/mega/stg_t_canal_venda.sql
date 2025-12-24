{{
  config(
    materialized = 'table',
	  tags="daily"
  ) 
}}

WITH canal_venda AS (

    SELECT 
      idcliente,
	    CASE 
        WHEN (offline_qtd > 0 AND online_qtd > 0) THEN 'AMBOS'
		    WHEN (offline_qtd > 0) THEN 'OFFLINE'
		    WHEN online_qtd > 0 THEN 'ONLINE'
		    ELSE 'ERRO'
	    END                                               AS ds_canal_venda

    FROM(
        SELECT
          *
        FROM (
          SELECT 
            DISTINCT idcliente, 
            ds_canal_venda 
          FROM {{ ref('stg_t_orcamento')}}
        ) 
        PIVOT (COUNT(*) AS qtd FOR ds_canal_venda IN ('Offline', 'Online'))
    )

)

SELECT *
FROM canal_venda 