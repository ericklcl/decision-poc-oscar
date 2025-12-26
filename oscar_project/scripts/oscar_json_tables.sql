-- ============================================================================
-- Scripts de Criação de Tabelas JSON para Todos os Sources
-- Padrão: OSCAR_RAW_JSON.JSN_<NOME_DA_TABELA>
-- Baseado no arquivo sources.yml
-- ============================================================================

-- 1. SHEETS_METRAGEM_LOJAS
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_SHEETS_METRAGEM_LOJAS (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 2. MSTORE_T_ESTABELECIMENTO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_ESTABELECIMENTO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 3. MSTORE_T_ENDERECO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_ENDERECO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 4. MSTORE_T_UNIDADENEGOCIO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_UNIDADENEGOCIO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 5. MSTORE_T_GRUPOF
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_GRUPOF (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 6. MSTORE_T_SITUACAOPRODUTO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_SITUACAOPRODUTO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 7. MSTORE_T_PRODUTO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_PRODUTO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 8. MSTORE_T_REFERENCIAPRODUTO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_REFERENCIAPRODUTO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 9. MSTORE_T_COR
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_COR (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 10. MSTORE_HISTORY_T_PRODUTODANOTA
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_HISTORY_T_PRODUTODANOTA (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 11. MSTORE_T_NOTAFISCAL
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_NOTAFISCAL (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 12. MSTORE_HISTORY_T_NOTAFISCAL
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_HISTORY_T_NOTAFISCAL (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 13. MSTORE_T_FORNECEDOR
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_FORNECEDOR (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 14. MSTORE_T_TAMANHO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_TAMANHO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 15. MSTORE_T_DEPARTAMENTO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_DEPARTAMENTO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 16. MSTORE_T_PRODUTOGENERICO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_PRODUTOGENERICO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 17. MSTORE_T_PRODUTOESPECIFICO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_PRODUTOESPECIFICO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 18. MSTORE_T_MARCA
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_MARCA (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 19. MSTORE_T_SECAO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_SECAO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 20. MSTORE_T_GRUPOPRODUTO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_GRUPOPRODUTO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 21. MSTORE_T_MARCAFORNECEDOR
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_MARCAFORNECEDOR (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 22. MSTORE_T_METAPRODUTOSPORTAL
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_METAPRODUTOSPORTAL (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 23. MSTORE_T_MATERIAL
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_MATERIAL (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 24. MSTORE_T_PRODUTODANOTA
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_PRODUTODANOTA (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 25. MSTORE_T_FUNCIONARIO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_FUNCIONARIO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 26. MSTORE_T_ATIVIDADE
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_ATIVIDADE (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 27. MSTORE_T_ESTADOCIVIL
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_ESTADOCIVIL (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 28. MSTORE_T_ESTOQUE
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_ESTOQUE (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 29. MSTORE_T_ORCAMENTO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_ORCAMENTO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 30. MSTORE_HISTORY_T_ORCAMENTO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_HISTORY_T_ORCAMENTO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 31. MSTORE_T_ORCAMENTOPRODUTO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_ORCAMENTOPRODUTO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 32. MSTORE_HISTORY_T_ORCAMENTOPRODUTO
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_HISTORY_T_ORCAMENTOPRODUTO (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 33. MSTORE_T_CLIENTE
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_CLIENTE (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 34. MSTORE_T_CONTATOCLIENTE
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_CONTATOCLIENTE (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 35. MSTORE_T_ORCAMENTOTROCA
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_ORCAMENTOTROCA (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 36. MSTORE_HISTORY_T_ORCAMENTOTROCA
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_HISTORY_T_ORCAMENTOTROCA (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 37. MSTORE_T_PRODUTOTROCA
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_PRODUTOTROCA (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 38. MSTORE_HISTORY_T_PRODUTOTROCA
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_HISTORY_T_PRODUTOTROCA (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 39. MSTORE_T_TROCA
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_T_TROCA (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 40. MSTORE_HISTORY_T_TROCA
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_HISTORY_T_TROCA (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 41. MSTORE_HISTORY_ESTOQUE
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_HISTORY_ESTOQUE (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- 42. MSTORE_ENRICHED_GENDER_CLASSIFIER
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_MSTORE_ENRICHED_GENDER_CLASSIFIER (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);

-- ============================================================================
-- Fim dos Scripts de Criação de Tabelas JSON
-- Total: 42 tabelas criadas no schema OSCAR_RAW_JSON
-- ============================================================================