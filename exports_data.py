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
def create_table(conn, df):
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
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS exp (
        {', '.join(columns_with_types)}
    );
    """
    
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()

