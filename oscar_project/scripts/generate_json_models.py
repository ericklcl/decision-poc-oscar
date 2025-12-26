#!/usr/bin/env python3
"""
Script para gerar automaticamente modelos DBT que extraem colunas JSON
das tabelas OSCAR_RAW_JSON baseado no arquivo sources.yml

Este script realiza as seguintes operações:
1. Lê o arquivo sources.yml do projeto DBT
2. Extrai as definições de colunas de cada tabela
3. Gera modelos DBT que fazem a extração de campos JSON para colunas estruturadas
4. Mapeia os tipos de dados do PostgreSQL para tipos do Snowflake
5. Salva os modelos gerados na pasta oscar_raw_json

Autor: Sistema automatizado
Data: 26/12/2024
"""

# Importações necessárias
import yaml      # Para parsing do arquivo YAML sources.yml
import os        # Para operações do sistema operacional
from pathlib import Path  # Para manipulação de caminhos de arquivos de forma mais robusta

def load_sources_yaml():
    """
    Carrega e faz o parsing do arquivo sources.yml do projeto DBT.
    
    Este arquivo contém as definições de todas as tabelas fonte,
    incluindo nomes, descrições e esquemas das colunas.
    
    Returns:
        dict: Conteúdo do arquivo sources.yml parseado como dicionário Python
        
    Raises:
        FileNotFoundError: Se o arquivo sources.yml não for encontrado
        yaml.YAMLError: Se houver erro no parsing do YAML
    """
    # Caminho fixo para o arquivo sources.yml do projeto
    sources_file_path = 'sources.yml'
    
    # Abre o arquivo em modo leitura com encoding UTF-8
    with open(sources_file_path, 'r', encoding='utf-8') as f:
        # Usa yaml.safe_load para parsing seguro (evita execução de código)
        return yaml.safe_load(f)

def map_data_types(data_type):
    """
    Mapeia tipos de dados do PostgreSQL (sources.yml) para tipos do Snowflake.
    
    O sources.yml contém tipos de dados do PostgreSQL, mas os modelos DBT
    precisam usar a sintaxe de tipos do Snowflake para a extração JSON.
    
    Args:
        data_type (str): Tipo de dados do PostgreSQL (ex: 'character varying')
        
    Returns:
        str: Tipo de dados correspondente no Snowflake (ex: 'VARCHAR')
        
    Examples:
        >>> map_data_types('character varying')
        'VARCHAR'
        >>> map_data_types('numeric')
        'NUMBER'
        >>> map_data_types('timestamp without time zone')
        'TIMESTAMP_NTZ'
    """
    # Dicionário de mapeamento: PostgreSQL -> Snowflake
    type_mapping = {
        'character varying': 'VARCHAR',           # Strings de tamanho variável
        'numeric': 'NUMBER',                      # Números decimais/inteiros
        'timestamp without time zone': 'TIMESTAMP_NTZ',  # Timestamps sem timezone
        'smallint': 'NUMBER'                      # Inteiros pequenos
    }
    
    # Retorna o tipo mapeado, ou VARCHAR como padrão se não encontrar
    return type_mapping.get(data_type, 'VARCHAR')

def generate_dbt_model(table_name, columns):
    """
    Gera o conteúdo completo de um modelo DBT para extração de colunas JSON.
    
    Esta função constrói um modelo DBT que:
    1. Lê dados da tabela JSON correspondente
    2. Extrai cada campo JSON como uma coluna tipada
    3. Preserva os metadados de carga (arquivo S3, timestamps, etc.)
    
    Args:
        table_name (str): Nome da tabela fonte (ex: 'mstore_t_cliente')
        columns (list): Lista de dicionários com definições das colunas
                       Cada item deve ter 'name' e opcionalmente 'data_type'
    
    Returns:
        str: Código SQL completo do modelo DBT pronto para ser salvo em arquivo
        
    Example:
        >>> columns = [
        ...     {'name': 'id', 'data_type': 'numeric'},
        ...     {'name': 'nome', 'data_type': 'character varying'}
        ... ]
        >>> model = generate_dbt_model('minha_tabela', columns)
        >>> print(model)  # Retorna SQL completo do modelo DBT
    """
    
    # ETAPA 1: Preparação do nome da tabela JSON
    # Converte o nome da tabela fonte para o formato da tabela JSON de destino
    # Ex: 'mstore_t_cliente' -> 'JSN_MSTORE_T_CLIENTE'
    json_table_name = f"JSN_{table_name.upper()}"
    
    # ETAPA 2: Construção do cabeçalho do modelo DBT
    # Define configuração de materialização como 'table' (cria tabela física)
    # e monta a CTE inicial que seleciona os dados brutos da tabela JSON
    model_content = f"""{{{{
  config(
    materialized = 'table'
  ) 
}}}}

WITH raw_data AS (
    SELECT
        JSON_DATA,                  -- Campo VARIANT contendo o JSON completo
        S3_FILE_NAME,              -- Nome do arquivo S3 de origem
        S3_FILE_ROW_NUMBER,        -- Número da linha no arquivo original
        S3_FILE_LAST_MODIFIED,     -- Timestamp de modificação do arquivo S3
        LOAD_TIMESTAMP_UTC,        -- Timestamp de quando foi carregado no Snowflake
        RECORD_SOURCE              -- Identificador da fonte do registro
    FROM OSCAR_RAW_JSON.{json_table_name}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
"""
    
    # ETAPA 3: Geração das linhas de extração de colunas JSON
    # Para cada coluna definida no sources.yml, cria uma linha de extração
    column_lines = []
    for col in columns:
        col_name = col['name']  # Nome da coluna (ex: 'idcliente')
        
        # Mapeia o tipo PostgreSQL para Snowflake, usa 'character varying' como padrão
        col_type = map_data_types(col.get('data_type', 'character varying'))
        
        # Constrói a linha de extração usando sintaxe do Snowflake:
        # JSON_DATA:campo::TIPO AS campo
        column_lines.append(f"    JSON_DATA:{col_name}::{col_type} AS {col_name}")
    
    # ETAPA 4: Junta todas as linhas de colunas com vírgula e quebra de linha
    model_content += ",\n".join(column_lines)
    
    # ETAPA 5: Adiciona os metadados preservados ao final do SELECT
    # Estes campos são importantes para auditoria e rastreabilidade dos dados
    model_content += """,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data"""
    
    return model_content

def main():
    """
    Função principal que orquestra todo o processo de geração de modelos DBT.
    
    Fluxo de execução:
    1. Carrega e valida o arquivo sources.yml
    2. Localiza a seção 'raw_input' que contém as definições das tabelas
    3. Cria o diretório de destino para os modelos
    4. Itera sobre cada tabela definida no sources.yml
    5. Pula tabelas já criadas manualmente ou sem colunas definidas
    6. Gera e salva o modelo DBT para cada tabela válida
    
    O script é idempotente - pode ser executado múltiplas vezes sem duplicar arquivos.
    """
    
    # ETAPA 1: Carregamento dos dados do sources.yml
    print("Carregando sources.yml...")
    try:
        sources_data = load_sources_yaml()
    except Exception as e:
        print(f"Erro ao carregar sources.yml: {e}")
        return
    
    # ETAPA 2: Localização da seção 'raw_input' dentro do sources.yml
    # O arquivo sources.yml pode ter múltiplas seções, precisamos da 'raw_input'
    raw_input_source = None
    for source in sources_data['sources']:
        if source['name'] == 'raw_input':
            raw_input_source = source
            break
    
    # Validação: verifica se encontrou a seção necessária
    if not raw_input_source:
        print("Erro: Source 'raw_input' não encontrado!")
        print("Verifique se o arquivo sources.yml contém uma seção com name: raw_input")
        return
    
    # ETAPA 3: Preparação do diretório de destino
    # Cria o diretório oscar_raw_json se não existir
    output_dir = Path('oscar_raw_json')
    output_dir.mkdir(exist_ok=True)  # exist_ok=True evita erro se já existir
    
    print(f"Gerando modelos DBT em {output_dir}...")
    
    # ETAPA 4: Definição de tabelas que já foram criadas manualmente
    # Estas tabelas serão puladas para evitar sobrescrever trabalho manual
    # created_tables = {
    #     'sheets_metragem_lojas',        # Tabela de metragem das lojas
    #     'mstore_t_estabelecimento',     # Tabela de estabelecimentos
    #     'mstore_t_endereco',            # Tabela de endereços
    #     'mstore_t_produto',             # Tabela de produtos
    #     'mstore_t_cliente'              # Tabela de clientes
    # }
    created_tables = set()  # Nenhuma tabela criada manualmente por enquanto
    
    # ETAPA 5: Processamento de cada tabela definida no sources.yml
    for table in raw_input_source['tables']:
        table_name = table['name']
        
        # CONDIÇÃO 1: Pula tabelas já criadas manualmente
        if table_name in created_tables:
            print(f"  Pulando {table_name} (já existe)")
            continue
            
        # CONDIÇÃO 2: Extrai as colunas da definição da tabela
        columns = table.get('columns', [])
        
        # Pula tabelas sem colunas definidas (não conseguimos gerar o modelo)
        if not columns:
            print(f"  Pulando {table_name} (sem colunas definidas)")
            continue
        
        # ETAPA 6: Geração do modelo para a tabela atual
        print(f"  Gerando {table_name}.sql...")
        
        try:
            # Gera o conteúdo SQL do modelo DBT
            model_content = generate_dbt_model(table_name, columns)
            
            # ETAPA 7: Salvamento do arquivo
            # Constrói o caminho do arquivo de destino
            output_file = output_dir / f"{table_name}.sql"
            
            # Salva o conteúdo no arquivo
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(model_content)
                
        except Exception as e:
            print(f"  Erro ao gerar {table_name}: {e}")
            continue
    
    print("Concluído!")
    print(f"Modelos DBT gerados na pasta: {output_dir}")

# PONTO DE ENTRADA DO SCRIPT
# Só executa se o arquivo for chamado diretamente (não importado)
if __name__ == "__main__":
    main()