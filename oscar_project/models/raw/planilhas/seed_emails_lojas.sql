{{
  config(
    materialized = 'table'
  )
}}
WITH aux_emails_lojas AS (
  SELECT 
      filial,
      sigla,
      email_loja,
      email_gerente
    FROM {{ ref ('aux_emails_lojas') }}
)
SELECT 
  filial,
  sigla,
  email_loja,
  email_gerente
FROM aux_emails_lojas
