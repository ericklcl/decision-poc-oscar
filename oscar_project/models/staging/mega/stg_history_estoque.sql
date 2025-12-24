{{
  config(
    materialized = 'incremental'
  ) 
}}

WITH estoque AS (

    SELECT
      trunc(DATE_TRUNC('month', current_date) - interval '1 month')         AS dt_data_ingestao,
      status,
      nk_loja,
      nk_produto,
      vlr_precovenda,
      qtd_estoque,
      vlr_valorcusto,
      vlr_customedio,
      icms

    FROM {{ ref('stg_t_estoque') }}

    {% if is_incremental() %}

      WHERE current_date = trunc(DATE_TRUNC('month', current_date))

    {% endif %}

)

SELECT *
FROM estoque