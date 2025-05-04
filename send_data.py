import pandas as pd
import psycopg2
from io import StringIO
import csv

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
    
    if data_type == 'E':
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS export_data (
            {', '.join(columns_with_types)}
        );
        """
    if data_type == 'I':
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS import_data (
            {', '.join(columns_with_types)}
        );
        """
    
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()

# Função para inserir dados no PostgreSQL
def insert_data(conn, df, data_type):
    cursor = conn.cursor()
    
    # Preparando os dados para inserção
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
    
    try:
        # Ler o CSV
        df = read_csv(file_content)
        print("Dados lidos do CSV com sucesso!")
        
        # Conectar ao PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        print("Conexão com PostgreSQL estabelecida!")
        
        # Criar tabela
        create_table(conn, df, data_type)
        print("Tabela criada ou verificada com sucesso!")
        
        # Inserir dados
        insert_data(conn, df, data_type)
        print("Dados inseridos com sucesso!")
        
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()