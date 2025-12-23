{{
  config(
    materialized = 'table',
    tags="daily"
  ) 
}}

WITH feriado AS (

    SELECT
      data,
      ds_feriado

    FROM {{ ref('stg_seed_feriados') }}

), calendario AS (

    SELECT 
      id_data                              AS sk_data,
      data,
      dia_mes,
      n_dia_semana,
      dia_do_ano,
      ultimo_dia_mes,
      nomes_mes,
      nome_mes_curto,
      numero_mes,
      primeiro_dia_mes,
      trimestre,
      data_final_semana,
      semana_ano,
      data_comeco_semana,
      ultimo_dia_ano,
      ano,
      primeiro_dia_ano,
      nome_dia_semana,
      nome_dia_semana_curto,
      semestre,
      quadrimestre,
      bimestre,
      numero_mes || '-' || ano                    AS mes_ano
        
    FROM {{ ref('stg_dates') }} 

), tabela_final AS (

    SELECT
      cl.sk_data,
      cl.data,
      cl.dia_mes,
      cl.n_dia_semana,
      cl.dia_do_ano,
      cl.ultimo_dia_mes,
      cl.nomes_mes,
      cl.nome_mes_curto,
      cl.numero_mes,
      cl.primeiro_dia_mes,
      CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE)::INT = cl.numero_mes AND EXTRACT(YEAR FROM CURRENT_DATE)::INT = cl.ano THEN 'S'
      ELSE 'N'              
      END                                                AS mes_atual,
      CASE WHEN mes_atual = 'S' THEN TO_CHAR(CURRENT_DATE, 'YYYYmmdd')::INT --cl.data = CURRENT_DATE THEN TO_CHAR(CURENT_DATE, 'YYYYmmdd')::INT
      ELSE TO_CHAR(cl.primeiro_dia_mes, 'YYYYmmdd')::INT      
      END                                                AS sk_primeiro_dia_mes,
      cl.trimestre,
      cl.data_final_semana,
      cl.semana_ano,
      cl.data_comeco_semana,
      cl.ultimo_dia_ano,
      cl.ano,
      cl.primeiro_dia_ano,
      cl.nome_dia_semana,
      cl.nome_dia_semana_curto,
      cl.semestre,
      cl.quadrimestre,
      cl.bimestre,
      cl.mes_ano,
      fd.ds_feriado,
      CASE 
        WHEN cl.data < CURRENT_DATE THEN 'Realizado' 
        ELSE 'Em aberto'
      END AS status_dia,
      CASE 
        WHEN cl.data BETWEEN DATEADD(D,-180, CURRENT_DATE) AND DATEADD(D, 180, CURRENT_DATE) THEN cl.data
      END AS dt_curva
    FROM calendario cl
    LEFT JOIN feriado fd 
      ON fd.data = cl.data

)

SELECT *
FROM tabela_final