{{
  config(
    materialized = 'table',
    tags="daily"
  ) 
}}

WITH gestores AS (

    SELECT  
      *

    FROM {{ ref('aux_gestores') }}

)

SELECT *
FROM gestores