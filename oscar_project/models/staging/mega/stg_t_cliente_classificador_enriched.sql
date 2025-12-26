WITH clientes AS (
    SELECT 
        idcliente,
        nome as nome_original,
        {{ clean_name('nome') }} as nome_processado
    FROM {{ ref('stg_t_cliente_classificador') }} -- Sua tabela de origem
),

manual_fixes AS (
    -- Regras manuais que estavam no seu script Glue
    SELECT 'cancelado' as n, 'Escrito cancelado' as c 
    UNION ALL
    SELECT 'consumidor', 'Escrito Consumidor' 
    UNION ALL
    SELECT 'cliente', 'Escrito cliente'
)

SELECT 
    c.idcliente,
    c.nome_original,
    CASE 
        WHEN LEN(c.nome_processado) <= 1 THEN 'Em branco após regex'
        WHEN REGEXP_LIKE(c.nome_processado, '^(.)\\1*$') THEN 'Em branco após regex' -- Caracteres repetidos
        WHEN m.n IS NOT NULL THEN m.c
        ELSE COALESCE(lkp.classification, 'Outros')
    END AS classificacao_genero
FROM clientes c
LEFT JOIN manual_fixes m ON c.nome_processado = m.n
LEFT JOIN {{ ref('int_gender_lookup') }} lkp ON c.nome_processado = lkp.nome_limpo