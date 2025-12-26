#!/usr/bin/env python3
"""
Script para gerar automaticamente o arquivo oscar_json_tables.sql
baseado no arquivo sources.yml do projeto dbt e no diretório oscar_raw_json/.

Uso:
    python generate_json_tables_complete.py [--sources-path PATH] [--output-path PATH] [--oscar-raw-json-path PATH]
"""

import yaml
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Set
import re


def load_sources_yml(file_path: Path) -> Dict[Any, Any]:
    """Carrega o arquivo sources.yml."""
    with open(file_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


def extract_table_names_from_sources(sources_data: Dict[Any, Any]) -> List[str]:
    """Extrai os nomes das tabelas do arquivo sources.yml."""
    table_names = []
    
    # Sources que queremos processar
    target_sources = ['raw_input', 'raw_data']
    
    for source in sources_data.get('sources', []):
        source_name = source.get('name')
        if source_name in target_sources:
            for table in source.get('tables', []):
                table_name = table.get('name')
                if table_name:
                    table_names.append(table_name)
    
    return table_names


def extract_table_names_from_oscar_raw_json_dir(oscar_raw_json_path: Path) -> List[str]:
    """Extrai os nomes das tabelas dos arquivos .sql no diretório oscar_raw_json."""
    table_names = []
    
    if oscar_raw_json_path.exists() and oscar_raw_json_path.is_dir():
        for sql_file in oscar_raw_json_path.glob("*.sql"):
            # Remove a extensão .sql para obter o nome da tabela
            table_name = sql_file.stem
            table_names.append(table_name)
    
    return table_names


def combine_and_deduplicate_tables(sources_tables: List[str], oscar_raw_tables: List[str]) -> List[str]:
    """Combina as listas de tabelas e remove duplicatas."""
    all_tables = set(sources_tables + oscar_raw_tables)
    return sorted(list(all_tables))


def generate_table_sql(table_name: str, index: int) -> str:
    """Gera o SQL CREATE OR REPLACE TABLE para uma tabela específica."""
    upper_table_name = table_name.upper()
    
    sql_template = f"""-- {index}. {upper_table_name}
CREATE OR REPLACE TABLE OSCAR_RAW_JSON.JSN_{upper_table_name} (
    -- Dados Brutos
    JSON_DATA VARIANT, 
    
    -- Metadados Nativos do Snowflake em UTC
    S3_FILE_NAME VARCHAR,                               
    S3_FILE_ROW_NUMBER NUMBER,                          
    S3_FILE_LAST_MODIFIED TIMESTAMP_NTZ,                
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT SYSDATE(), 
    
    -- Origem do Registro
    RECORD_SOURCE VARCHAR(16777216)
);"""
    
    return sql_template


def generate_complete_sql(table_names: List[str], sources_count: int = 0, oscar_raw_count: int = 0) -> str:
    """Gera o arquivo SQL completo com todas as tabelas."""
    
    # Cabeçalho
    header = f"""-- ============================================================================
-- Scripts de Criação de Tabelas JSON para Todos os Sources
-- Padrão: OSCAR_RAW_JSON.JSN_<NOME_DA_TABELA>
-- Baseado no arquivo sources.yml e arquivos do diretório oscar_raw_json/
-- Gerado automaticamente em {datetime.now().strftime('%d/%m/%Y às %H:%M:%S')}
-- Sources: {sources_count} tabelas do sources.yml + {oscar_raw_count} do oscar_raw_json/
-- ============================================================================"""
    
    # Corpo com todas as tabelas
    table_sqls = []
    for i, table_name in enumerate(table_names, 1):
        table_sql = generate_table_sql(table_name, i)
        table_sqls.append(table_sql)
    
    # Rodapé
    footer = f"""-- ============================================================================
-- Fim dos Scripts de Criação de Tabelas JSON
-- Total: {len(table_names)} tabelas criadas no schema OSCAR_RAW_JSON
-- ============================================================================"""
    
    # Unindo tudo
    complete_sql = header + "\n\n" + "\n\n".join(table_sqls) + "\n\n" + footer
    
    return complete_sql


def main():
    """Função principal do script."""
    parser = argparse.ArgumentParser(
        description="Gera automaticamente o arquivo oscar_json_tables.sql"
    )
    parser.add_argument(
        '--sources-path',
        type=Path,
        default=Path('sources.yml'),
        help='Caminho para o arquivo sources.yml (padrão: sources.yml)'
    )
    parser.add_argument(
        '--oscar-raw-json-path',
        type=Path,
        default=Path('oscar_raw_json'),
        help='Caminho para o diretório oscar_raw_json/ (padrão: oscar_raw_json)'
    )
    parser.add_argument(
        '--output-path', 
        type=Path,
        default=Path('oscar_json_tables.sql'),
        help='Caminho para o arquivo SQL de saída (padrão: oscar_json_tables.sql)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Exibir informações detalhadas durante a execução'
    )
    
    args = parser.parse_args()
    
    try:
        sources_tables = []
        oscar_raw_tables = []
        
        # Extrair tabelas do sources.yml
        if args.sources_path.exists():
            if args.verbose:
                print(f"📖 Carregando arquivo: {args.sources_path}")
            sources_data = load_sources_yml(args.sources_path)
            sources_tables = extract_table_names_from_sources(sources_data)
            if args.verbose:
                print(f"📋 Encontradas {len(sources_tables)} tabelas no sources.yml")
        else:
            print(f"⚠️  Aviso: Arquivo {args.sources_path} não encontrado!")
        
        # Extrair tabelas do diretório oscar_raw_json
        if args.oscar_raw_json_path.exists():
            if args.verbose:
                print(f"📁 Examinando diretório: {args.oscar_raw_json_path}")
            oscar_raw_tables = extract_table_names_from_oscar_raw_json_dir(args.oscar_raw_json_path)
            if args.verbose:
                print(f"📋 Encontradas {len(oscar_raw_tables)} tabelas no diretório oscar_raw_json/")
        else:
            print(f"⚠️  Aviso: Diretório {args.oscar_raw_json_path} não encontrado!")
        
        # Combinar e remover duplicatas
        all_tables = combine_and_deduplicate_tables(sources_tables, oscar_raw_tables)
        
        if not all_tables:
            print("❌ Erro: Nenhuma tabela foi encontrada!")
            return 1
        
        if args.verbose:
            print(f"\\n📊 Total de tabelas únicas encontradas: {len(all_tables)}")
            for i, name in enumerate(all_tables, 1):
                source = "📄 sources.yml" if name in sources_tables else ""
                oscar_raw = "📁 oscar_raw_json/" if name in oscar_raw_tables else ""
                sources_text = f" ({source}{' + ' if source and oscar_raw else ''}{oscar_raw})"
                print(f"   {i:2d}. {name}{sources_text}")
        
        # Gerar SQL completo
        complete_sql = generate_complete_sql(
            all_tables, 
            len(sources_tables), 
            len(oscar_raw_tables)
        )
        
        # Criar diretório de saída se não existir
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Escrever arquivo de saída
        with open(args.output_path, 'w', encoding='utf-8') as output_file:
            output_file.write(complete_sql)
        
        print(f"✅ Arquivo gerado com sucesso: {args.output_path}")
        print(f"📊 Total de tabelas processadas: {len(all_tables)}")
        print(f"   📄 Do sources.yml: {len(sources_tables)}")
        print(f"   📁 Do oscar_raw_json/: {len(oscar_raw_tables)}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Erro durante a execução: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())