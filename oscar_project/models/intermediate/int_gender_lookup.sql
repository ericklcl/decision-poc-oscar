WITH base_seed AS (
    SELECT * FROM {{ ref('aux_nomes') }}
),

expanded AS (
    -- Explode a coluna alternative_names e limpa os nomes
    SELECT 
        classification,
        {{ clean_name('f.value::string') }} as nome_limpo
    FROM base_seed,
    LATERAL FLATTEN(input => SPLIT(alternative_names, '|')) f
    WHERE alternative_names IS NOT NULL
)

SELECT DISTINCT classification, nome_limpo FROM expanded
UNION
SELECT classification, {{ clean_name('first_name') }} FROM base_seed