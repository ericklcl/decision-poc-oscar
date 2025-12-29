{{
  config(
    materialized = 'table',
    tags="daily"
  ) 
}}

WITH fat_venda AS (

     SELECT
      sk_dt_venda,
      sk_vendedor,
      sk_loja,
      sk_loja_vendedor,
      sk_cliente,
      sk_produto,
      id_orcamento_dd,
      ds_status,                     
      qtd_produto_vendido,
      vlr_venda_liquida,
      vlr_custo,
      vlr_custo_bruto,
      flg_status_produto,
      id_pedido_vtex_dd,
      nm_host,
      ds_canal_venda

     FROM {{ ref('fat_venda') }}
  
), fat_troca AS (

     SELECT
      sk_dt_troca,
      sk_vendedor,
      sk_loja,
      sk_lojavendedor,
      sk_cliente,
      sk_produto,
      id_orcamento_dd,
      ds_status,                     
      qtd_produto_trocado,
      vlr_venda_troca,
      vlr_venda_custo,
      vlr_custo_bruto,
      flg_status_produto,
      id_pedido_vtex_dd,
      nm_host,
      ds_canal_venda

     FROM {{ ref ('fat_troca') }}
     WHERE (TRIM(nm_host) <> 'SUPER TROCO' OR nm_host IS NULL)
), venda_liquida AS (

    SELECT

      'Venda'                   AS tipo,
      sk_dt_venda               AS sk_data,
      sk_vendedor,
      sk_loja,
      sk_loja_vendedor,
      sk_cliente,
      sk_produto,
      id_orcamento_dd,
      ds_status,                     
      qtd_produto_vendido       AS qtd_vendida,
      vlr_venda_liquida,
      vlr_custo                 AS vlr_venda_custo,
      vlr_custo_bruto,
      flg_status_produto,
      id_pedido_vtex_dd,
      nm_host,
      ds_canal_venda 
    
    FROM fat_venda
    WHERE (TRIM(nm_host) <> 'SUPER TROCO' OR nm_host IS NULL)
    UNION ALL

    SELECT

     'Troca'                     AS tipo,
      sk_dt_troca                AS sk_data,
      sk_vendedor,
      sk_loja,                   
      sk_lojavendedor           AS sk_loja_vendedor,
      sk_cliente,
      sk_produto,
      id_orcamento_dd,
      ds_status,                     
      -1 * qtd_produto_trocado   AS qtd_vendida,
      -1 * vlr_venda_troca       AS vlr_venda_liquida,
      -1 * vlr_venda_custo       AS vlr_venda_custo,
      vlr_custo_bruto,
      flg_status_produto,
      id_pedido_vtex_dd,
      nm_host,
      ds_canal_venda

     FROM fat_troca

), calendario AS (

      SELECT
        sk_data,
        "data"
      FROM {{ ref( 'dim_calendario' ) }}

), produto AS (

      SELECT
        sk_produto,
        ds_departamento

      FROM {{ ref( 'dim_produto' ) }}

), primeira_compra AS (

    select 
      vl.sk_cliente,
		  min(cal.data) dt_primeira_compra
	  from venda_liquida vl
	  LEFT join calendario cal 
      on vl.sk_data = cal.sk_data
	  group by sk_cliente

), orcamento AS (

    SELECT 
      idorcamento,
      to_char("data", 'HH24:MI:SS')                   AS hora,
      CASE 
        WHEN TO_CHAR("data", 'HH24') = '00' THEN '00'
        WHEN TO_CHAR("data", 'HH24') = '01' THEN '01'
        WHEN TO_CHAR("data", 'HH24') = '02' THEN '02'
        WHEN TO_CHAR("data", 'HH24') = '03' THEN '03'
        WHEN TO_CHAR("data", 'HH24') = '04' THEN '04'
        WHEN TO_CHAR("data", 'HH24') = '05' THEN '05'
        WHEN TO_CHAR("data", 'HH24') = '06' THEN '06'
        WHEN TO_CHAR("data", 'HH24') = '07' THEN '07'
        WHEN TO_CHAR("data", 'HH24') = '08' THEN '08'
        WHEN TO_CHAR("data", 'HH24') = '09' THEN '09'
        WHEN TO_CHAR("data", 'HH24') = '10' THEN '10'
        WHEN TO_CHAR("data", 'HH24') = '11' THEN '11'
        WHEN TO_CHAR("data", 'HH24') = '12' THEN '12'
        WHEN TO_CHAR("data", 'HH24') = '13' THEN '13'
        WHEN TO_CHAR("data", 'HH24') = '14' THEN '14'
        WHEN TO_CHAR("data", 'HH24') = '15' THEN '15'
        WHEN TO_CHAR("data", 'HH24') = '16' THEN '16'
        WHEN TO_CHAR("data", 'HH24') = '17' THEN '17'
        WHEN TO_CHAR("data", 'HH24') = '18' THEN '18'
        WHEN TO_CHAR("data", 'HH24') = '19' THEN '19'
        WHEN TO_CHAR("data", 'HH24') = '20' THEN '20'
        WHEN TO_CHAR("data", 'HH24') = '21' THEN '21'
        WHEN TO_CHAR("data", 'HH24') = '22' THEN '22'
        WHEN TO_CHAR("data", 'HH24') = '23' THEN '23'
        ELSE 'Erro'
      END                                             AS ds_faixa_hora

    FROM {{ ref('mstore_t_orcamento') }}

), tabela_final AS (

    Select
      vl.tipo                               AS tipo,
      vl.sk_data                            AS sk_data,
      vl.sk_vendedor                        AS sk_vendedor,
      vl.sk_loja                            AS sk_loja,                   
      vl.sk_loja_vendedor                   AS sk_loja_vendedor,
      vl.sk_cliente                         AS sk_cliente,
      vl.sk_produto                         AS sk_produto,
      vl.id_orcamento_dd                    AS id_orcamento_dd,
      vl.ds_status                          AS ds_status,                     
      vl.qtd_vendida                        AS qtd_vendida,
      vl.vlr_venda_liquida                  AS vlr_venda_liquida,
      vl.vlr_venda_custo                    AS vlr_venda_custo,
      CASE
        WHEN vl.vlr_custo_bruto < vl.vlr_venda_custo OR vl.vlr_custo_bruto = 0 THEN vl.vlr_venda_custo
        ELSE vl.vlr_custo_bruto
      END                                   AS vlr_custo_bruto,
      --vl.vlr_custo_bruto                  AS vlr_custo_bruto,
      vl.flg_status_produto                 AS flg_status_produto,
      vl.id_pedido_vtex_dd                  AS id_pedido_vtex_dd,
      vl.nm_host                            AS nm_host,
      vl.ds_canal_venda                     AS ds_canal_venda,
      CASE
        WHEN dt_primeira_compra IS NULL THEN 'Não'
        ELSE 'Sim'
      END                                   AS flag_primeira_compra,
      CASE
        WHEN vlr_venda_liquida < 0 THEN '38. < 0'
        WHEN vlr_venda_liquida >= 0.00 AND vlr_venda_liquida < 10.00 THEN '1. 0,00 a 9,99'
        WHEN vlr_venda_liquida >= 10.00 AND vlr_venda_liquida < 20.00 THEN '2. 10,00 a 19,99'
        WHEN vlr_venda_liquida >= 20.00 AND vlr_venda_liquida < 30.00 THEN '3. 20,00 a 29,99'
        WHEN vlr_venda_liquida >= 30.00 AND vlr_venda_liquida < 40.00 THEN '4. 30,00 a 39,99'
        WHEN vlr_venda_liquida >= 40.00 AND vlr_venda_liquida < 50.00 THEN '5. 40,00 a 49,99'
        WHEN vlr_venda_liquida >= 50.00 AND vlr_venda_liquida < 60.00 THEN '6. 50,00 a 59,99'
        WHEN vlr_venda_liquida >= 60.00 AND vlr_venda_liquida < 70.00 THEN '7. 60,00 a 69,99'
        WHEN vlr_venda_liquida >= 70.00 AND vlr_venda_liquida < 80.00 THEN '8. 70,00 a 79,99'
        WHEN vlr_venda_liquida >= 80.00 AND vlr_venda_liquida < 90.00 THEN '9. 80,00 a 89,99'
        WHEN vlr_venda_liquida >= 90.00 AND vlr_venda_liquida < 100.00 THEN '10. 90,00 a 99,99'
        WHEN vlr_venda_liquida >= 100.00 AND vlr_venda_liquida < 110.00 THEN '11. 100,00 a 109,99'
        WHEN vlr_venda_liquida >= 110.00 AND vlr_venda_liquida < 120.00 THEN '12. 110,00 a 119,99'
        WHEN vlr_venda_liquida >= 120.00 AND vlr_venda_liquida < 130.00 THEN '13. 120,00 a 129,99'
        WHEN vlr_venda_liquida >= 130.00 AND vlr_venda_liquida < 140.00 THEN '14. 130,00 a 139,99'
        WHEN vlr_venda_liquida >= 140.00 AND vlr_venda_liquida < 150.00 THEN '15. 140,00 a 149,99'
        WHEN vlr_venda_liquida >= 150.00 AND vlr_venda_liquida < 160.00 THEN '16. 150,00 a 159,99'
        WHEN vlr_venda_liquida >= 160.00 AND vlr_venda_liquida < 170.00 THEN '17. 160,00 a 169,99'
        WHEN vlr_venda_liquida >= 170.00 AND vlr_venda_liquida < 180.00 THEN '18. 170,00 a 179,99'
        WHEN vlr_venda_liquida >= 180.00 AND vlr_venda_liquida < 190.00 THEN '19. 180,00 a 189,99'
        WHEN vlr_venda_liquida >= 190.00 AND vlr_venda_liquida < 200.00 THEN '20. 190,00 a 199,99'
        WHEN vlr_venda_liquida >= 200.00 AND vlr_venda_liquida < 220.00 THEN '21. 200,00 a 219,99'
        WHEN vlr_venda_liquida >= 220.00 AND vlr_venda_liquida < 240.00 THEN '22. 220,00 a 239,99'
        WHEN vlr_venda_liquida >= 240.00 AND vlr_venda_liquida < 260.00 THEN '23. 240,00 a 259,99'
        WHEN vlr_venda_liquida >= 260.00 AND vlr_venda_liquida < 280.00 THEN '24. 260,00 a 279,99'
        WHEN vlr_venda_liquida >= 280.00 AND vlr_venda_liquida < 300.00 THEN '25. 280,00 a 299,99'
        WHEN vlr_venda_liquida >= 300.00 AND vlr_venda_liquida < 350.00 THEN '26. 300,00 a 349,99'
        WHEN vlr_venda_liquida >= 350.00 AND vlr_venda_liquida < 400.00 THEN '27. 350,00 a 399,99'
        WHEN vlr_venda_liquida >= 400.00 AND vlr_venda_liquida < 450.00 THEN '28. 400,00 a 449,99'
        WHEN vlr_venda_liquida >= 450.00 AND vlr_venda_liquida < 500.00 THEN '29. 450,00 a 499,99'
        WHEN vlr_venda_liquida >= 500.00 AND vlr_venda_liquida < 600.00 THEN '30. 500,00 a 599,99'
        WHEN vlr_venda_liquida >= 600.00 AND vlr_venda_liquida < 700.00 THEN '31. 600,00 a 699,99'
        WHEN vlr_venda_liquida >= 700.00 AND vlr_venda_liquida < 800.00 THEN '32. 700,00 a 799,99'
        WHEN vlr_venda_liquida >= 800.00 AND vlr_venda_liquida < 900.00 THEN '33. 800,00 a 899,99'
        WHEN vlr_venda_liquida >= 900.00 AND vlr_venda_liquida < 1000.00 THEN '34. 900,00 a 999,99'
        WHEN vlr_venda_liquida >= 1000.00 AND vlr_venda_liquida < 1500.00 THEN '35. 1.000,00 a 1499,99'
        WHEN vlr_venda_liquida >= 1500.00 AND vlr_venda_liquida < 2000.00 THEN '36. 1.500,00 a 1999,99'
        WHEN vlr_venda_liquida >= 2000.00 THEN '37. >= 2.000,00'
        ELSE 'Sem faixa de valor'
      END                                      AS ds_faixa_valor,
      oc.ds_faixa_hora,
      CASE
        WHEN p.ds_departamento = 'Brindes' THEN 0
        ELSE vl.qtd_vendida
      END                                                 AS qtd_vendida_sem_brinde 

    FROM venda_liquida vl
    LEFT JOIN calendario cal
      ON cal.sk_data = vl.sk_data 
    LEFT JOIN primeira_compra pc 
      ON (
        pc.sk_cliente = vl.sk_cliente AND
        pc.dt_primeira_compra = cal.data
      )
    LEFT JOIN orcamento oc 
      ON oc.idorcamento = vl.id_orcamento_dd
    LEFT JOIN produto p
      ON vl.sk_produto = p.sk_produto

)

SELECT *
FROM tabela_final 