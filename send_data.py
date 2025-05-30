import pandas as pd
import psycopg2
from io import StringIO
import csv
from psycopg2 import sql

# Configurações do PostgreSQL
DB_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin'
}

# Função para ler o arquivo CSV
def read_csv(file_path):
    csv_file = file_path
    
    # Lendo o CSV com pandas
    df = pd.read_csv(csv_file, sep=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    
    # Convertendo colunas numéricas para tipos apropriados
    numeric_cols = ['CO_ANO', 'CO_MES', 'QT_ESTAT', 'KG_LIQUIDO', 'VL_FOB']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

# Função para criar a tabela no PostgreSQL
def create_table(conn, df, data_type):
    cursor = conn.cursor()
    
    # Gerando os tipos de colunas automaticamente
    columns_with_types = []
    for col in df.columns:
        dtype = df[col].dtype
        if dtype == 'int64':
            pg_type = 'BIGINT'
        elif dtype == 'float64':
            pg_type = 'FLOAT'
        else:
            pg_type = 'VARCHAR(255)'
        columns_with_types.append(f'"{col}" {pg_type}')
    
    # Identificar os nomes exatos das colunas de chave no DataFrame
    key_column_names = ['CO_ANO', 'CO_MES', 'CO_NCM', 'CO_UNID', 'CO_PAIS', 'SG_UF_NCM', 'CO_VIA', 'CO_URF']
    existing_key_columns = []
    
    for key_col in key_column_names:
        # Procurar a coluna no DataFrame (pode ser maiúscula, minúscula ou mista)
        matching_cols = [col for col in df.columns if col.upper() == key_col.upper()]
        if matching_cols:
            existing_key_columns.append(f'"{matching_cols[0]}"')
        
    # Somente incluir a constraint se todas as colunas da chave existirem
    constraint_clause = ""
    if len(existing_key_columns) == len(key_column_names):
        constraint_name = "export_data_unique" if data_type == 'E' else "import_data_unique"
        constraint_clause = f", CONSTRAINT {constraint_name} UNIQUE ({', '.join(existing_key_columns)})"
    
    if data_type == 'E':
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS export_data (
            {', '.join(columns_with_types)}
            {constraint_clause}
        );
        """
    if data_type == 'I':
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS import_data (
            {', '.join(columns_with_types)}
            {constraint_clause}
        );
        """
    
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()

# Função para verificar se os dados já existem
def check_existing_data(conn, df, data_type):
    cursor = conn.cursor()
    
    # Identificar colunas que compõem a chave única
    key_columns = ['CO_ANO', 'CO_MES', 'CO_NCM', 'CO_UNID', 'CO_PAIS', 'SG_UF_NCM', 'CO_VIA', 'CO_URF']
    
    # Verificar se todas as colunas necessárias estão presentes
    missing_cols = [col for col in key_columns if col not in df.columns]
    if missing_cols:
        print(f"Aviso: As seguintes colunas necessárias para verificação não estão presentes: {missing_cols}")
        return False
    
    # Construir consulta para verificar registros existentes
    table_name = 'export_data' if data_type == 'E' else 'import_data'
    
    # Criar uma string com os valores das colunas-chave para comparação
    df['key_values'] = df[key_columns].astype(str).apply(lambda row: '|'.join(row), axis=1)
    
    # Consultar os registros existentes
    existing_query = sql.SQL("SELECT {cols} FROM {table}").format(
        cols=sql.SQL(',').join(map(sql.Identifier, key_columns)),
        table=sql.Identifier(table_name)
    )
    
    cursor.execute(existing_query)
    existing_records = cursor.fetchall()
    
    # Criar conjunto de chaves existentes
    existing_keys = set('|'.join(str(value) for value in row) for row in existing_records)
    
    # Filtrar DataFrame para manter apenas registros não existentes
    new_records = df[~df['key_values'].isin(existing_keys)].copy()
    
    cursor.close()
    
    if len(new_records) == 0:
        print("Todos os registros já existem no banco de dados.")
        return None
    elif len(new_records) < len(df):
        print(f"{len(df) - len(new_records)} registros já existiam e serão ignorados.")
    
    return new_records.drop(columns=['key_values'])

# Função para inserir dados no PostgreSQL
def insert_data(conn, df, data_type):
    cursor = conn.cursor()
    
    # Verificar dados existentes antes de inserir
    df = check_existing_data(conn, df, data_type)
    if df is None:
        return  # Todos os dados já existem
    elif len(df) == 0:
        print("Nenhum novo registro para inserir.")
        return
    
    output = StringIO()
    # Escrever os dados no buffer no formato que o PostgreSQL espera
    df.to_csv(output, sep='\t', header=False, index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
    output.seek(0)
    
    # Copiando os dados para o PostgreSQL
    try:
        if data_type == 'E':
            cursor.copy_expert("COPY export_data FROM STDIN WITH NULL AS ''", output)
        elif data_type == 'I':
            cursor.copy_expert("COPY import_data FROM STDIN WITH NULL AS ''", output)
        conn.commit()
        print(f"{len(df)} registros inseridos com sucesso!")
    except Exception as e:
        conn.rollback()
        print(f"Erro ao inserir dados: {e}")
    finally:
        cursor.close()

# Função principal
def main():
    # Substitua pelo conteúdo real do arquivo ou leia de um arquivo real
    file_content = input("Digite o caminho do arquivo CSV: ")

    data_type = input("Os dados são de exportação (E) ou importação (I)? ").strip().upper()
    while data_type not in ['E', 'I']:
        print("Por favor, digite 'E' para exportação ou 'I' para importação.")
        data_type = input("Os dados são de exportação (E) ou importação (I)? ").strip().upper()
    
    try:
        # Ler o CSV
        df = read_csv(file_content)
        print("Dados lidos do CSV com sucesso!")
        
        # Conectar ao PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        print("Conexão com PostgreSQL estabelecida!")
        
        # Criar tabela (com constraint de unicidade)
        create_table(conn, df, data_type)
        print("Tabela criada ou verificada com sucesso!")
        
        # Inserir dados (com verificação de duplicados)
        insert_data(conn, df, data_type)
        
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()