{% macro clean_name(column_name) %}
    -- 1. Converte para minúsculo e remove espaços nas pontas
    -- 2. TRANSLATE remove acentos
    -- 3. REGEXP_REPLACE remove qualquer coisa que não seja letra ou espaço
    TRIM(
        REGEXP_REPLACE(
            TRANSLATE(
                LOWER({{ column_name }}), 
                'áàâãäéèêëíìîïóòôõöúùûüç', 
                'aaaaaeeeeiiiiooooouuuuc'
            ), 
            '[^a-z ]', 
            ''
        )
    )
{% endmacro %}