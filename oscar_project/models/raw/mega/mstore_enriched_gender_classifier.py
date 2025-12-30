import pandas as pd
import re
import unicodedata

def remove_caracteres_nao_alfanumericos(text):
    if text is None:
        return ""
    # Remove acentos e normaliza
    normalized_text = unicodedata.normalize('NFD', str(text))
    no_accent_text = ''.join(c for c in normalized_text if unicodedata.category(c) != 'Mn')
    
    # Mantém apenas letras e converte para minúsculo
    pattern = re.compile(r'[^a-zA-Z\s]')
    cleaned_text = pattern.sub('', no_accent_text).strip().lower()
    
    # Regras de negócio para nomes inválidos
    if len(cleaned_text) <= 1 or len(set(cleaned_text)) == 1:
        return ""
    return cleaned_text

def model(dbt, session):
    # Configurações do dbt
    dbt.config(
        materialized="table",
        packages=["pandas"] # O Snowflake carregará o pandas no warehouse
    )
    
    # Fonte: mstore_t_cliente contém idcliente, nome, _dms_loaded_at
    df_clientes = dbt.ref("mstore_t_cliente").to_pandas()

    # 1) Regra equivalente ao SPLIT_PART(nome, ' ', 1)
    #    (trata null e espaços extras)
    df_clientes["PRIMEIRO_NOME"] = (
        df_clientes["nome"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.split(r"\s+", n=1, expand=False)
        .str[0]
    )

    # 2) Regra equivalente ao TO_CHAR(_dms_loaded_at, 'YYYY-MM-DD')
    #    (garante datetime e formata)
    df_clientes["_dms_loaded_at"] = pd.to_datetime(
        df_clientes["_dms_loaded_at"],
        errors="coerce"
    )
    df_clientes["dt_carga"] = df_clientes["_dms_loaded_at"].dt.strftime("%Y-%m-%d")

    # Carregando a tabela auxiliar de nomes do IBGE
    df_ibge = dbt.ref("aux_nomes").to_pandas()

    # Montando o dicionário de classificação
    lookup_dict = {}
    for _, row in df_ibge.iterrows():
        classif = row['CLASSIFICATION']
        # Mapeia nome principal e grupo
        lookup_dict[remove_caracteres_nao_alfanumericos(row['FIRST_NAME'])] = classif
        lookup_dict[remove_caracteres_nao_alfanumericos(row['GROUP_NAME'])] = classif
        
        # Mapeia nomes alternativos
        alt_names = str(row['ALTERNATIVE_NAMES'])
        if alt_names.lower() != 'nan':
            for nome in alt_names.split('|'):
                lookup_dict[remove_caracteres_nao_alfanumericos(nome)] = classif

    # Adicionando exceções manuais
    lookup_dict.update({
        'cancelado': 'Escrito cancelado',
        'consumidor': 'Escrito Consumidor',
        'calcados': 'Escrito Calçados',
        '': 'Em branco após regex',
        'cliente': 'Escrito cliente'
    })

    # Aplicando a lógica
    df_clientes['NOME_CLEAN'] = df_clientes['PRIMEIRO_NOME'].apply(remove_caracteres_nao_alfanumericos)
    df_clientes['CLASSIFICACAO'] = df_clientes['NOME_CLEAN'].apply(lambda x: lookup_dict.get(x, 'Outros'))

    # Retornando o DataFrame final (sem a coluna temporária)
    return df_clientes.drop(columns=['NOME_CLEAN'])