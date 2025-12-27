{{
  config(
    materialized = 'table'
  ) 
}}

WITH calendario AS (

	SELECT 
    TO_CHAR(date_day, 'YYYYmmdd')::INT      AS id_data,
    date_day                                AS data,
    day_of_month                            AS dia_mes,
    day_of_week                             AS n_dia_semana,
    day_of_year                             AS dia_do_ano,
    month_end_date                          AS ultimo_dia_mes,
    CASE 
      WHEN month_name = 'January' THEN 'Janeiro'
      WHEN month_name = 'February' THEN 'Fevereiro'
      WHEN month_name = 'March' THEN 'Março'
      WHEN month_name = 'April' THEN 'Abril'
      WHEN month_name = 'May' THEN 'Maio'
      WHEN month_name = 'June' THEN 'Junho'
      WHEN month_name = 'July' THEN 'Julho'
      WHEN month_name = 'August' THEN 'Agosto'
      WHEN month_name = 'September' THEN 'Setembro'
      WHEN month_name = 'October' THEN 'Outubro'
      WHEN month_name = 'November' THEN 'Novembro'
      WHEN month_name = 'December' THEN 'Dezembro'
      ELSE month_name
    END                                     AS nomes_mes,
    CASE
      WHEN month_name_short = 'Jan' THEN 'Jan'
      WHEN month_name_short = 'Feb' THEN 'Fev' 
      WHEN month_name_short = 'Mar' THEN 'Mar'
      WHEN month_name_short = 'Apr' THEN 'Abr'
      WHEN month_name_short = 'May' THEN 'Mai'
      WHEN month_name_short = 'Jun' THEN 'Jun'
      WHEN month_name_short = 'Jul' THEN 'Jul'
      WHEN month_name_short = 'Aug' THEN 'Ago'
      WHEN month_name_short = 'Sep' THEN 'Set'
      WHEN month_name_short = 'Oct' THEN 'Out'
      WHEN month_name_short = 'Nov' THEN 'Nov'
      WHEN month_name_short = 'Dec' THEN 'Dez'
      ELSE month_name_short
    END                                     AS nome_mes_curto,
    month_of_year                           AS numero_mes,
    month_start_date                        AS primeiro_dia_mes,
    CASE
      WHEN quarter_of_year = 1 THEN '1° trimestre'
      WHEN quarter_of_year = 2 THEN '2° trimestre'
      WHEN quarter_of_year = 3 THEN '3° trimestre'
    ELSE '4° trimestre'
    END                                     AS trimestre,
    week_end_date                           AS data_final_semana,
    week_of_year                            AS semana_ano,
    week_start_date                         AS data_comeco_semana,
    year_end_date                           AS ultimo_dia_ano,
    year_number                             AS ano,
    year_start_date                         AS primeiro_dia_ano,
    CASE 
      WHEN day_of_week_name = 'Monday' THEN 'Segunda-feira'
      WHEN day_of_week_name = 'Tuesday' THEN 'Terça-feira'
      WHEN day_of_week_name = 'Wednesday' THEN 'Quarta-feira'
      WHEN day_of_week_name = 'Thursday' THEN 'Quinta-feira'
      WHEN day_of_week_name = 'Friday' THEN 'Sexta-feira'
      WHEN day_of_week_name = 'Saturday' THEN 'Sábado'
      WHEN day_of_week_name = 'Sunday' THEN 'Domingo'
      ELSE day_of_week_name
    END                                     AS nome_dia_semana,
    CASE 
      WHEN day_of_week_name_short = 'Sun' THEN 'Dom'
      WHEN day_of_week_name_short = 'Mon' THEN 'Seg'
      WHEN day_of_week_name_short = 'Tue' THEN 'Ter'
      WHEN day_of_week_name_short = 'Wed' THEN 'Qua'
      WHEN day_of_week_name_short = 'Thu' THEN 'Qui'
      WHEN day_of_week_name_short = 'Fri' THEN 'Sex'
      WHEN day_of_week_name_short = 'Sat' THEN 'Sáb'
    ELSE day_of_week_name_short
    END                                       AS nome_dia_semana_curto,
    CASE
      WHEN quarter_of_year <= 2 THEN '1° semestre'
    ELSE '2° semestre'
    END                                       AS semestre,
    CASE
      WHEN month_of_year <= 4 THEN '1° quadrimestre'
      WHEN month_of_year <= 8 THEN '2° quadrimestre'
    ELSE '3° quadrimestre'
    END                                       AS quadrimestre,
    CASE
      WHEN month_of_year <= 2 THEN '1° bimestre'
      WHEN month_of_year <= 4 THEN '2° bimestre'
      WHEN month_of_year <= 6 THEN '3° bimestre'
      WHEN month_of_year <= 8 THEN '4° bimestre'
      WHEN month_of_year <=10 THEN '5° bimestre'
    ELSE '6° bimestre'
    END                                       AS bimestre
  FROM {{ ref('dates') }}
)

SELECT *
FROM calendario
