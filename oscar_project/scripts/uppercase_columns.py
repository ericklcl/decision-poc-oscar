#!/usr/bin/env python3
"""
Script para converter todos os nomes de colunas em maiúsculo nos modelos SQL da pasta raw/mega.
"""

import os
import re
import glob

def process_sql_file(file_path):
    """
    Processa um arquivo SQL convertendo os nomes das colunas para maiúsculo.
    """
    print(f"Processando: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    # Padrão para capturar as cláusulas JSON_DATA:campo::TIPO AS nome_coluna
    # Primeiro, vamos capturar o padrão completo e dividir em partes
    pattern = r'JSON_DATA:([a-z_][a-z0-9_]*)(::[A-Z_]+\s+AS\s+)([A-Z_]+)'
    
    def uppercase_json_key(match):
        json_key = match.group(1).upper()  # campo -> CAMPO
        middle_part = match.group(2)  # ::TIPO AS 
        column_name = match.group(3)  # NOME_COLUNA (já maiúsculo)
        return f"JSON_DATA:{json_key}{middle_part}{column_name}"
    
    # Aplica a conversão para maiúsculo na chave JSON
    content = re.sub(pattern, uppercase_json_key, content)
    
    # Verifica se houve alterações
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✓ Arquivo atualizado: {file_path}")
        return True
    else:
        print(f"- Nenhuma alteração necessária: {file_path}")
        return False

def main():
    """
    Função principal que processa todos os arquivos SQL na pasta raw/mstore_history.
    """
    # Caminho para a pasta raw/mstore_history
    mstore_history_path = "/home/elima/Documents/learning/decisions/poc/oscar_project/models/raw/mstore_history"
    
    # Busca todos os arquivos .sql na pasta
    sql_files = glob.glob(os.path.join(mstore_history_path, "*.sql"))
    
    print(f"Encontrados {len(sql_files)} arquivos SQL para processar...")
    print("=" * 60)
    
    updated_files = []
    
    for sql_file in sql_files:
        if process_sql_file(sql_file):
            updated_files.append(os.path.basename(sql_file))
    
    print("=" * 60)
    print(f"Processamento concluído!")
    print(f"Total de arquivos processados: {len(sql_files)}")
    print(f"Arquivos atualizados: {len(updated_files)}")
    
    if updated_files:
        print("\nArquivos que foram atualizados:")
        for file in updated_files:
            print(f"  - {file}")

if __name__ == "__main__":
    main()