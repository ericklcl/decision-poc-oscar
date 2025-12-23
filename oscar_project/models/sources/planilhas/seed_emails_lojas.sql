{{
  config(
    materialized = 'table'
  )
}}
WITH aux_emails_lojas AS (
  SELECT 
    *
    FROM {{ ref ('aux_emails_lojas') }}
)
SELECT * FROM aux_emails_lojas
