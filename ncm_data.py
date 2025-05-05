import pandas as pd
import psycopg2
import chardet

# Configurações do PostgreSQL
DB_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin'
}

def detect_encoding(file_path):
    """Detecta a codificação do arquivo"""
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

def read_ncm_mapping(file_path):
    """Lê o arquivo CSV com o mapeamento NCM"""
    try:
        encoding = detect_encoding(file_path)
        print(f"Detectada codificação: {encoding}")
        
        df = pd.read_csv(
            file_path, 
            sep=';', 
            dtype={'CO_NCM': str},
            encoding=encoding
        )
        
        if 'CO_NCM' not in df.columns or 'NO_NCM_POR' not in df.columns:
            print("Erro: Arquivo deve conter colunas CO_NCM e NO_NCM_POR")
            return None
            
        df['CO_NCM'] = df['CO_NCM'].str.zfill(8)
        return df[['CO_NCM', 'NO_NCM_POR']].drop_duplicates()
        
    except Exception as e:
        print(f"Erro ao ler arquivo: {e}")
        return None

def update_table_with_ncm(conn, table_name, ncm_mapping, temp_table_suffix=""):
    """Atualiza uma tabela específica com descrições NCM"""
    cursor = conn.cursor()
    temp_table_name = f"temp_ncm_{temp_table_suffix}" if temp_table_suffix else "temp_ncm"
    
    try:
        # Verificar se a tabela existe
        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name.lower()}')")
        if not cursor.fetchone()[0]:
            print(f"Tabela {table_name} não existe. Pulando...")
            return

        # Adicionar coluna de descrição se não existir
        cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS no_ncm_por TEXT')

        # Criar tabela temporária com tipo bigint para compatibilidade
        cursor.execute(f"""
            CREATE TEMPORARY TABLE {temp_table_name} (
                co_ncm BIGINT PRIMARY KEY,
                no_ncm_por TEXT
            ) ON COMMIT DROP
        """)

        # Inserir dados na temporária, convertendo para bigint
        for _, row in ncm_mapping.iterrows():
            try:
                ncm_code = int(row['CO_NCM'])
                cursor.execute(
                    f"INSERT INTO {temp_table_name} (co_ncm, no_ncm_por) VALUES (%s, %s)",
                    (ncm_code, row['NO_NCM_POR'])
                )
            except ValueError:
                print(f"Aviso: Código NCM inválido ignorado: {row['CO_NCM']}")

        # Atualizar tabela principal
        update_query = f"""
            UPDATE "{table_name}" 
            SET no_ncm_por = t.no_ncm_por
            FROM {temp_table_name} t
            WHERE "{table_name}"."CO_NCM" = t.co_ncm
        """
        
        cursor.execute(update_query)
        print(f"Tabela {table_name} atualizada com sucesso!")
        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Erro ao atualizar {table_name}: {e}")
    finally:
        cursor.close()

def main():
    ncm_file_path = "./src/ncm_data/NCM.csv"
    
    try:
        ncm_mapping = read_ncm_mapping(ncm_file_path)
        if ncm_mapping is None:
            return
        
        print(f"{len(ncm_mapping)} mapeamentos carregados.")
        
        conn = psycopg2.connect(**DB_CONFIG)
        print("Conectado ao PostgreSQL!")
        
        # Atualizar ambas as tabelas com tabelas temporárias diferentes
        update_table_with_ncm(conn, 'export_data', ncm_mapping, "export")
        update_table_with_ncm(conn, 'import_data', ncm_mapping, "import")
        
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()