{{
  config(
    materialized = 'table',
    tags="daily"
  ) 
}}

WITH gestores AS (

    SELECT  
      *

    FROM {{ ref('seed_gestores') }}

)

SELECT *
FROM gestores