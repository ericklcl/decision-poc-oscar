#!/usr/bin/env python3
"""
Script para identificar sources sem colunas definidas no sources.yml
"""

import yaml

def find_sources_without_columns():
    """Encontra todas as tabelas no sources.yml que não têm colunas definidas"""
    
    # Carrega o arquivo sources.yml
    with open('sources.yml', 'r') as f:
        sources_data = yaml.safe_load(f)
    
    # Encontra a seção raw_input
    raw_input_source = None
    for source in sources_data['sources']:
        if source['name'] == 'raw_input':
            raw_input_source = source
            break
    
    if not raw_input_source:
        print("Source 'raw_input' não encontrado!")
        return
    
    tables_without_columns = []
    tables_with_empty_columns = []
    
    # Verifica cada tabela
    for table in raw_input_source['tables']:
        table_name = table['name']
        columns = table.get('columns', [])
        
        if 'columns' not in table:
            tables_without_columns.append(table_name)
        elif len(columns) == 0:
            tables_with_empty_columns.append(table_name)
    
    print("=== SOURCES SEM COLUNAS DEFINIDAS ===\n")
    
    if tables_without_columns:
        print("📋 Tabelas SEM a seção 'columns':")
        for i, table in enumerate(tables_without_columns, 1):
            print(f"  {i:2d}. {table}")
        print()
    
    if tables_with_empty_columns:
        print("📋 Tabelas COM seção 'columns' VAZIA:")
        for i, table in enumerate(tables_with_empty_columns, 1):
            print(f"  {i:2d}. {table}")
        print()
    
    total_without_columns = len(tables_without_columns) + len(tables_with_empty_columns)
    total_tables = len(raw_input_source['tables'])
    
    print(f"📊 RESUMO:")
    print(f"   Total de tabelas: {total_tables}")
    print(f"   Tabelas sem colunas: {total_without_columns}")
    print(f"   Tabelas com colunas: {total_tables - total_without_columns}")
    print(f"   Percentual sem colunas: {(total_without_columns/total_tables)*100:.1f}%")

if __name__ == "__main__":
    find_sources_without_columns()