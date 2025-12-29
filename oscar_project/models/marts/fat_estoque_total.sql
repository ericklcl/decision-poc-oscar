{{
 config(
   materialized = 'table',
   tags = 'daily'
 )
}}


WITH estabelecimento AS (


   SELECT
     idestabelecimento,
     cnpj,
     CASE
       WHEN idunidadenegocio = 53 THEN 52
       ELSE idunidadenegocio
     END                                                                                             AS idunidadenegocio
  
   FROM {{ ref('stg_t_estabelecimento')}}


), history_estoque AS (


   SELECT
     *
   FROM {{ ref('stg_t_history_estoque') }}


), fat_history_estoque AS (


   SELECT
     {{ dbt_utils.generate_surrogate_key (['sk_produto']) }}                                         AS sk_produto,
     CAST(SUBSTRING(sk_data, 7, 4)||SUBSTRING(sk_data, 4, 2)|| SUBSTRING(sk_data, 1, 2) as INT)      AS sk_data,
     {{ dbt_utils.generate_surrogate_key (['he.sk_loja', 'est.cnpj']) }}                             AS sk_loja,
     sk_data                                                                                         AS data,
     qtd_estoque_pecas,
     CAST(REPLACE(vlr_estoque_venda, ',', '.') AS FLOAT)                                             AS vlr_unitario,
     CASE
       WHEN vlr_estoque_custo_sem_imposto IS NULL OR vlr_estoque_custo_sem_imposto = '' THEN 0
       ELSE CAST(REPLACE(vlr_estoque_custo_sem_imposto, ',', '.') AS FLOAT)
       END                                                                                           AS vlr_custo_unitario_sem_imposto,
     CASE
       WHEN vlr_estoque_custo_com_imposto IS NULL OR vlr_estoque_custo_com_imposto = '' THEN 0
       ELSE CAST(REPLACE(vlr_estoque_custo_com_imposto, ',', '.') AS FLOAT)
       END                                                                                           AS vlr_custo_unitario_com_imposto,
     CASE
       WHEN vlr_estoque_custo_sem_imposto IS NULL OR vlr_estoque_custo_sem_imposto = '' THEN 0
       ELSE CAST(REPLACE(vlr_estoque_custo_sem_imposto, ',', '.') AS FLOAT)
       END  * qtd_estoque_pecas                                                                      AS vlr_estoque_custo_historico,
     CAST(REPLACE(vlr_estoque_venda, ',', '.') AS FLOAT) * qtd_estoque_pecas                         AS vlr_estoque_venda,
     num_idade_estoque,
     ds_faixa_de_idade_dd
   FROM history_estoque he
   LEFT JOIN estabelecimento est
     ON he.sk_loja = est.idestabelecimento
   WHERE sk_data !~ '[a-zA-Z]'
     AND CAST(SUBSTRING(sk_data, 7, 4)||SUBSTRING(sk_data, 4, 2)|| SUBSTRING(sk_data, 1, 2) as INT) >= 20220101


), t_estoque_lake AS (


   SELECT
     cast(to_char(dt_data_ingestao, 'YYYYmmdd') as int) as sk_data,
     te.nk_produto,
     e.idunidadenegocio,
     te.nk_loja,
     te.vlr_precovenda,
     te.qtd_estoque,
     te.vlr_valorcusto,
     te.vlr_customedio


   FROM {{ ref('stg_history_estoque') }} te
   LEFT JOIN estabelecimento e ON
     te.nk_loja = e.idestabelecimento
   WHERE nk_produto <> '999999992539626'
   AND dt_data_ingestao <> '2024-08-19' -- remover os dados de teste do modelo incremental


), t_estoque_mega AS (


   SELECT
     cast(to_char(trunc(DATE_TRUNC('month', current_date)), 'YYYYmmdd') as int) as sk_data,
     nk_produto,
     idunidadenegocio,
     nk_loja,
     vlr_precovenda,
     qtd_estoque,
     vlr_valorcusto,
     vlr_customedio


   FROM {{ ref('stg_t_estoque') }} te
   LEFT JOIN estabelecimento e ON
     te.nk_loja = e.idestabelecimento
   WHERE nk_produto <> '999999992539626'


), t_estoque AS (


   SELECT
     sk_data,
     nk_produto,
     idunidadenegocio,
     nk_loja,
     vlr_precovenda,
     qtd_estoque,
     vlr_valorcusto,
     vlr_customedio
   FROM t_estoque_lake


   UNION ALL


   SELECT
     sk_data,
     nk_produto,
     idunidadenegocio,
     nk_loja,
     vlr_precovenda,
     qtd_estoque,
     vlr_valorcusto,
     vlr_customedio
   FROM t_estoque_mega


), t_produtodanota AS (


   SELECT
     sk_idprodutonota,
     vlr_precodecustonota,
     idnotafiscal


   FROM {{ ref('stg_t_produtodanota') }}


), t_notafiscal AS (


   SELECT
     numeronota,
     idnotafiscal,
     data,
     dataemissao,
     dataconfirmacao,
     cnpj,
     idestabelecimento
  
   FROM {{ ref('stg_t_notafiscal')}}


), recebimentos_lojas AS (

    SELECT
      tnf.idnotafiscal
      ,tnf.numeronota
      ,tnf.data
      ,tnf.dataemissao
      ,tnf.dataconfirmacao
      ,tpn.sk_idprodutonota
      ,e.idestabelecimento
      ,e.idunidadenegocio
      ,CASE
        WHEN tnf.dataconfirmacao IS NULL THEN tnf.data
        ELSE tnf.data
      END                                                               AS data_entrada_produto
      ,ROW_NUMBER() OVER ( PARTITION BY tpn.sk_idprodutonota, tnf.idestabelecimento ORDER BY tnf.data DESC) AS rn

    FROM
    t_notafiscal tnf
    LEFT JOIN t_produtodanota tpn ON tnf.idnotafiscal = tpn.idnotafiscal
    LEFT JOIN estabelecimento e ON tnf.idestabelecimento = e.idestabelecimento

    WHERE e.idestabelecimento NOT IN ('355','276','14')

), entrega_loja AS (

    SELECT
      idnotafiscal
      ,numeronota
      ,data
      ,dataconfirmacao
      ,dataemissao
      ,sk_idprodutonota
      ,idestabelecimento
      ,data_entrada_produto
      ,CURRENT_DATE - data_entrada_produto          AS dias_em_estoque
      ,idunidadenegocio

    FROM recebimentos_lojas rlj 

    WHERE rn = 1


), recebimentos_cd AS (

    SELECT
      tnf.idnotafiscal
      ,tnf.numeronota
      ,tnf.data
      ,tnf.dataemissao
      ,tnf.dataconfirmacao
      ,tpn.sk_idprodutonota
      ,e.idestabelecimento
      ,e.idunidadenegocio
      ,CASE
        WHEN tnf.dataconfirmacao IS NULL THEN tnf.data
        ELSE tnf.data
      END                                                               AS data_entrada_produto
      ,ROW_NUMBER() OVER ( PARTITION BY tpn.sk_idprodutonota, tnf.idestabelecimento ORDER BY tnf.data DESC) AS rn

    FROM
    t_notafiscal tnf
    LEFT JOIN t_produtodanota tpn ON tnf.idnotafiscal = tpn.idnotafiscal
    LEFT JOIN estabelecimento e ON tnf.idestabelecimento = e.idestabelecimento

    WHERE e.idestabelecimento in ('355','276','14')

), entrega_cd AS (

    SELECT
      idnotafiscal
      ,numeronota
      ,data
      ,dataconfirmacao
      ,dataemissao
      ,sk_idprodutonota
      ,idestabelecimento
      ,data_entrada_produto
      ,CURRENT_DATE - data_entrada_produto          AS dias_em_estoque
      ,idunidadenegocio

    FROM recebimentos_cd rcd 

    WHERE rn = 1


), recebimentos_bandeira AS (

    SELECT
      tnf.idnotafiscal
      ,tnf.numeronota
      ,tnf.data
      ,tnf.dataemissao
      ,tnf.dataconfirmacao
      ,tpn.sk_idprodutonota
      ,e.idestabelecimento
      ,e.idunidadenegocio
      ,CASE
        WHEN tnf.dataconfirmacao IS NULL THEN tnf.data
        ELSE tnf.data
      END                                                               AS data_entrada_produto
      ,ROW_NUMBER() OVER ( PARTITION BY tpn.sk_idprodutonota, e.idunidadenegocio ORDER BY tnf.data DESC) AS rn

    FROM
    t_notafiscal tnf
    LEFT JOIN t_produtodanota tpn ON tnf.idnotafiscal = tpn.idnotafiscal
    LEFT JOIN estabelecimento e ON tnf.idestabelecimento = e.idestabelecimento

), entrega_bandeira AS (

    SELECT
      idnotafiscal
      ,numeronota
      ,data
      ,dataconfirmacao
      ,dataemissao
      ,sk_idprodutonota
      ,idestabelecimento
      ,data_entrada_produto
      ,CURRENT_DATE - data_entrada_produto          AS dias_em_estoque
      ,idunidadenegocio

    FROM recebimentos_cd rcd 

    WHERE rn = 1


), recebimentos_empresa AS (

    SELECT
      tnf.idnotafiscal
      ,tnf.numeronota
      ,tnf.data
      ,tnf.dataemissao
      ,tnf.dataconfirmacao
      ,tpn.sk_idprodutonota
      ,e.idestabelecimento
      ,e.idunidadenegocio
      ,CASE
        WHEN tnf.dataconfirmacao IS NULL THEN tnf.data
        ELSE tnf.data
      END                                                               AS data_entrada_produto
      ,ROW_NUMBER() OVER ( PARTITION BY tpn.sk_idprodutonota, tnf.idestabelecimento ORDER BY tnf.data DESC) AS rn

    FROM
    t_notafiscal tnf
    LEFT JOIN t_produtodanota tpn ON tnf.idnotafiscal = tpn.idnotafiscal
    LEFT JOIN estabelecimento e ON tnf.idestabelecimento = e.idestabelecimento


), entrega_empresa AS (

    SELECT
      idnotafiscal
      ,numeronota
      ,data
      ,dataconfirmacao
      ,dataemissao
      ,sk_idprodutonota
      ,idestabelecimento
      ,data_entrada_produto
      ,CURRENT_DATE - data_entrada_produto          AS dias_em_estoque
      ,idunidadenegocio

    FROM recebimentos_empresa re 

    WHERE rn = 1

), datas_entrada AS (


   SELECT
     est.nk_loja,
     est.nk_produto,
     COALESCE(MAX(el.data_entrada_produto),
     MAX(ecd.data_entrada_produto),
     MAX(ebd.data_entrada_produto),
     MAX(ee.data_entrada_produto))                               AS data_entrada
   FROM
     t_estoque est
   LEFT JOIN entrega_loja el ON
     el.idestabelecimento = est.nk_loja
     AND el.sk_idprodutonota = est.nk_produto
   LEFT JOIN entrega_cd ecd ON
     ecd.idunidadenegocio = est.idunidadenegocio
     AND ecd.sk_idprodutonota = est.nk_produto
   LEFT JOIN entrega_bandeira ebd ON
     ebd.idunidadenegocio = est.idunidadenegocio
     AND ebd.sk_idprodutonota = est.nk_produto
   LEFT JOIN entrega_empresa ee ON
     ee.sk_idprodutonota = est.nk_produto
   GROUP BY
     nk_loja,
     nk_produto


), dias_em_estoque as (


   SELECT
     nk_loja,
     nk_produto,
     CURRENT_DATE - data_entrada                                     AS dias_em_estoque
   FROM
     datas_entrada


), fat_estoque AS (


   SELECT
     sk_data,
     {{ dbt_utils.generate_surrogate_key (['est.nk_produto']) }}                          AS sk_produto,
     {{ dbt_utils.generate_surrogate_key (['est.nk_loja', 'loja.cnpj']) }}                AS sk_loja,
     SUM(est.qtd_estoque)                                                                 AS qtd_estoque,
     SUM(est.vlr_valorcusto)                                                              AS vlr_valorcusto,
     SUM(est.vlr_precovenda * est.qtd_estoque)                                            AS vlr_estoque_venda,
     SUM(est.vlr_valorcusto * est.qtd_estoque)                                            AS vlr_estoque_custo,
     SUM(est.vlr_customedio * est.qtd_estoque)                                            AS vlr_estoque_custo_medio,
   CASE
       WHEN dias_em_estoque <= 60 THEN '0 - 60'
       WHEN dias_em_estoque <= 120 AND dias_em_estoque >= 61 THEN '61 - 120'
       WHEN dias_em_estoque <= 180 AND dias_em_estoque >= 121 THEN '121 - 180'
       WHEN dias_em_estoque <= 360 AND dias_em_estoque >= 181 THEN '181 - 360'
       WHEN dias_em_estoque <= 720 AND dias_em_estoque >= 361 THEN '361 - 720'
    ELSE '720+'   
   END                                                                                    AS faixa_idade_agrupada,
    CASE
     WHEN dias_em_estoque <= 30 THEN '0 - 30'
     WHEN dias_em_estoque <= 60 AND dias_em_estoque >= 31 THEN '31 - 60'
     WHEN dias_em_estoque <= 90 AND dias_em_estoque >= 61 THEN '61 - 90'
     WHEN dias_em_estoque <= 120 AND dias_em_estoque >= 91 THEN '91 - 120'
     WHEN dias_em_estoque <= 150 AND dias_em_estoque >= 121 THEN '121 - 150'
     WHEN dias_em_estoque <= 180 AND dias_em_estoque >= 151 THEN '151 - 180'
     WHEN dias_em_estoque <= 360 AND dias_em_estoque >= 181 THEN '181 - 360'
     WHEN dias_em_estoque <= 720 AND dias_em_estoque >= 361 THEN '361 - 720'
    ELSE '720+'   
   END                                                                                    AS faixa_idade_intermediaria,
   CASE
     WHEN dias_em_estoque <= 15 THEN '0 - 15'
     WHEN dias_em_estoque <= 30 AND dias_em_estoque >= 16 THEN '16 - 30'
     WHEN dias_em_estoque <= 45 AND dias_em_estoque >= 31 THEN '31 - 45'
     WHEN dias_em_estoque <= 60 AND dias_em_estoque >= 46 THEN '46 - 60'
     WHEN dias_em_estoque <= 75 AND dias_em_estoque >= 61 THEN '61 - 75'
     WHEN dias_em_estoque <= 90 AND dias_em_estoque >= 76 THEN '76 - 90'
     WHEN dias_em_estoque <= 120 AND dias_em_estoque >= 91 THEN '91 - 120'
     WHEN dias_em_estoque <= 150 AND dias_em_estoque >= 121 THEN '121 - 150'
     WHEN dias_em_estoque <= 180 AND dias_em_estoque >= 151 THEN '151 - 180'
     WHEN dias_em_estoque <= 240 AND dias_em_estoque >= 181 THEN '181 - 240'
     WHEN dias_em_estoque <= 300 AND dias_em_estoque >= 241 THEN '241 - 300'
     WHEN dias_em_estoque <= 360 AND dias_em_estoque >= 301 THEN '301 - 360'
     WHEN dias_em_estoque <= 420 AND dias_em_estoque >= 361 THEN '361 - 420'
     WHEN dias_em_estoque <= 480 AND dias_em_estoque >= 421 THEN '421 - 480'
     WHEN dias_em_estoque <= 540 AND dias_em_estoque >= 481 THEN '481 - 540'
     WHEN dias_em_estoque <= 600 AND dias_em_estoque >= 541 THEN '541 - 600'
     WHEN dias_em_estoque <= 610 AND dias_em_estoque >= 601 THEN '601 - 610'
     WHEN dias_em_estoque <= 730 AND dias_em_estoque >= 611 THEN '611 - 730'
    ELSE '730+' 
   END                                                                                     AS faixa_idade_aberta,
   CASE
    WHEN faixa_idade_aberta = '0 - 15'    THEN 1
    WHEN faixa_idade_aberta = '16 - 30'   THEN 2
    WHEN faixa_idade_aberta = '31 - 45'   THEN 3
    WHEN faixa_idade_aberta = '46 - 60'   THEN 4
    WHEN faixa_idade_aberta = '61 - 75'   THEN 5
    WHEN faixa_idade_aberta = '76 - 90'   THEN 6 
    WHEN faixa_idade_aberta = '91 - 120'  THEN 7
    WHEN faixa_idade_aberta = '121 - 150' THEN 8
    WHEN faixa_idade_aberta = '151 - 180' THEN 9
    WHEN faixa_idade_aberta = '181 - 240' THEN 10
    WHEN faixa_idade_aberta = '241 - 300' THEN 11
    WHEN faixa_idade_aberta = '301 - 360' THEN 12
    WHEN faixa_idade_aberta = '361 - 420' THEN 13
    WHEN faixa_idade_aberta = '421 - 480' THEN 14
    WHEN faixa_idade_aberta = '481 - 540' THEN 15
    WHEN faixa_idade_aberta = '541 - 600' THEN 16
    WHEN faixa_idade_aberta = '601 - 610' THEN 17
    WHEN faixa_idade_aberta = '611 - 730' THEN 18
  ELSE                                         19  
  END                                                                                     AS ordem,
   CASE
       WHEN vlr_estoque_venda < 0 THEN '38. < 0'
       WHEN vlr_estoque_venda >= 0.00 AND vlr_estoque_venda < 10.00 THEN '1. 0,00 a 9,99'
       WHEN vlr_estoque_venda >= 10.00 AND vlr_estoque_venda < 20.00 THEN '2. 10,00 a 19,99'
       WHEN vlr_estoque_venda >= 20.00 AND vlr_estoque_venda < 30.00 THEN '3. 20,00 a 29,99'
       WHEN vlr_estoque_venda >= 30.00 AND vlr_estoque_venda < 40.00 THEN '4. 30,00 a 39,99'
       WHEN vlr_estoque_venda >= 40.00 AND vlr_estoque_venda < 50.00 THEN '5. 40,00 a 49,99'
       WHEN vlr_estoque_venda >= 50.00 AND vlr_estoque_venda < 60.00 THEN '6. 50,00 a 59,99'
       WHEN vlr_estoque_venda >= 60.00 AND vlr_estoque_venda < 70.00 THEN '7. 60,00 a 69,99'
       WHEN vlr_estoque_venda >= 70.00 AND vlr_estoque_venda < 80.00 THEN '8. 70,00 a 79,99'
       WHEN vlr_estoque_venda >= 80.00 AND vlr_estoque_venda < 90.00 THEN '9. 80,00 a 89,99'
       WHEN vlr_estoque_venda >= 90.00 AND vlr_estoque_venda < 100.00 THEN '10. 90,00 a 99,99'
       WHEN vlr_estoque_venda >= 100.00 AND vlr_estoque_venda < 110.00 THEN '11. 100,00 a 109,99'
       WHEN vlr_estoque_venda >= 110.00 AND vlr_estoque_venda < 120.00 THEN '12. 110,00 a 119,99'
       WHEN vlr_estoque_venda >= 120.00 AND vlr_estoque_venda < 130.00 THEN '13. 120,00 a 129,99'
       WHEN vlr_estoque_venda >= 130.00 AND vlr_estoque_venda < 140.00 THEN '14. 130,00 a 139,99'
       WHEN vlr_estoque_venda >= 140.00 AND vlr_estoque_venda < 150.00 THEN '15. 140,00 a 149,99'
       WHEN vlr_estoque_venda >= 150.00 AND vlr_estoque_venda < 160.00 THEN '16. 150,00 a 159,99'
       WHEN vlr_estoque_venda >= 160.00 AND vlr_estoque_venda < 170.00 THEN '17. 160,00 a 169,99'
       WHEN vlr_estoque_venda >= 170.00 AND vlr_estoque_venda < 180.00 THEN '18. 170,00 a 179,99'
       WHEN vlr_estoque_venda >= 180.00 AND vlr_estoque_venda < 190.00 THEN '19. 180,00 a 189,99'
       WHEN vlr_estoque_venda >= 190.00 AND vlr_estoque_venda < 200.00 THEN '20. 190,00 a 199,99'
       WHEN vlr_estoque_venda >= 200.00 AND vlr_estoque_venda < 220.00 THEN '21. 200,00 a 219,99'
       WHEN vlr_estoque_venda >= 220.00 AND vlr_estoque_venda < 240.00 THEN '22. 220,00 a 239,99'
       WHEN vlr_estoque_venda >= 240.00 AND vlr_estoque_venda < 260.00 THEN '23. 240,00 a 259,99'
       WHEN vlr_estoque_venda >= 260.00 AND vlr_estoque_venda < 280.00 THEN '24. 260,00 a 279,99'
       WHEN vlr_estoque_venda >= 280.00 AND vlr_estoque_venda < 300.00 THEN '25. 280,00 a 299,99'
       WHEN vlr_estoque_venda >= 300.00 AND vlr_estoque_venda < 350.00 THEN '26. 300,00 a 349,99'
       WHEN vlr_estoque_venda >= 350.00 AND vlr_estoque_venda < 400.00 THEN '27. 350,00 a 399,99'
       WHEN vlr_estoque_venda >= 400.00 AND vlr_estoque_venda < 450.00 THEN '28. 400,00 a 449,99'
       WHEN vlr_estoque_venda >= 450.00 AND vlr_estoque_venda < 500.00 THEN '29. 450,00 a 499,99'
       WHEN vlr_estoque_venda >= 500.00 AND vlr_estoque_venda < 600.00 THEN '30. 500,00 a 599,99'
       WHEN vlr_estoque_venda >= 600.00 AND vlr_estoque_venda < 700.00 THEN '31. 600,00 a 699,99'
       WHEN vlr_estoque_venda >= 700.00 AND vlr_estoque_venda < 800.00 THEN '32. 700,00 a 799,99'
       WHEN vlr_estoque_venda >= 800.00 AND vlr_estoque_venda < 900.00 THEN '33. 800,00 a 899,99'
       WHEN vlr_estoque_venda >= 900.00 AND vlr_estoque_venda < 1000.00 THEN '34. 900,00 a 999,99'
       WHEN vlr_estoque_venda >= 1000.00 AND vlr_estoque_venda < 1500.00 THEN '35. 1.000,00 a 1499,99'
       WHEN vlr_estoque_venda >= 1500.00 AND vlr_estoque_venda < 2000.00 THEN '36. 1.500,00 a 1999,99'
       WHEN vlr_estoque_venda >= 2000.00 THEN '37. >= 2.000,00'
       ELSE 'Sem faixa de valor'
     END                                                                                   AS ds_faixa_valor,
     MAX(dias_em_estoque)                                                                  AS dias_recebimento_nf


   FROM t_estoque est
   LEFT  JOIN estabelecimento loja ON
     est.nk_loja = loja.idestabelecimento
   LEFT JOIN dias_em_estoque ie ON
     est.nk_produto = ie.nk_produto
     AND est.nk_loja = ie.nk_loja
   GROUP BY
     sk_data,
     sk_produto,
     sk_loja,
     faixa_idade_aberta,
     faixa_idade_agrupada,
     faixa_idade_intermediaria
   

), union_table AS (


   SELECT
     sk_data,
     sk_produto,
     sk_loja,
     qtd_estoque_pecas                         AS qtd_pecas,
     vlr_unitario                              AS vlr_unitario_pdv,
     vlr_custo_unitario_sem_imposto            AS vlr_unitario_custo,
     vlr_estoque_custo_historico               AS est_vlr_custo,
     vlr_estoque_venda                         AS est_vlr_venda,
     NULL                                      AS faixa_idade_agrupada,
     NULL                                      AS faixa_idade_intermediaria,
     NULL                                      AS faixa_idade_aberta,
     NULL                                      AS ordem,
     NULL                                      AS ds_faixa_valor,
     NULL                                      AS dias_recebimento_nf
     
    FROM fat_history_estoque


   UNION ALL


   SELECT
     sk_data,
     sk_produto,
     sk_loja,
     qtd_estoque                               AS qtd_pecas,
     0                                         AS vlr_unitario_pdv,
     0                                         AS vlr_unitario_custo,
     vlr_estoque_custo_medio                   AS est_vlr_custo,
     vlr_estoque_venda                         AS est_vlr_venda,
     faixa_idade_agrupada,
     faixa_idade_intermediaria,
     faixa_idade_aberta,
     ordem,
     ds_faixa_valor,
     dias_recebimento_nf


   FROM fat_estoque


), tabela_final AS (


   SELECT
     sk_data,
     sk_produto,
     sk_loja,
     qtd_pecas,
     ROUND(vlr_unitario_pdv, 2)              AS vlr_unitario_pdv,
     ROUND(vlr_unitario_custo, 2)            AS vlr_unitario_custo,
     ROUND(est_vlr_custo, 2)                 AS est_vlr_custo,
     ROUND(est_vlr_venda, 2)                 AS est_vlr_venda,
     faixa_idade_agrupada,
     faixa_idade_intermediaria,
     faixa_idade_aberta,
     ordem,
     ds_faixa_valor,
     dias_recebimento_nf


   FROM union_table


)

SELECT *
FROM tabela_final where sk_produto <> '1c69461b2236aaa2775337bd4b848552'