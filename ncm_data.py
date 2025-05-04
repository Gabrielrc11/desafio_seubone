import csv
import psycopg2
from psycopg2 import sql
import chardet

# Configurações do banco de dados PostgreSQL
DB_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin'
}

# Nome da tabela no PostgreSQL
TABLE_NAME = 'ncm_codes'

def create_table(conn):
    """Cria a tabela no PostgreSQL se ela não existir"""
    with conn.cursor() as cursor:
        cursor.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                co_ncm VARCHAR(20) NOT NULL,
                no_ncm_por TEXT NOT NULL,
                UNIQUE(co_ncm)
            )
        """).format(sql.Identifier(TABLE_NAME)))
    conn.commit()

def process_csv_and_insert_data(csv_file, conn):
    """Processa o arquivo CSV e insere os dados no PostgreSQL"""
    # Detecta a codificação do arquivo
    
    with open(csv_file, mode='r', encoding='iso-8859-1') as file:
        try:
            # Tenta detectar o dialeto do CSV
            dialect = csv.Sniffer().sniff(file.read(1024))
            file.seek(0)
        except csv.Error:
            # Se não conseguir detectar, usa configurações padrão
            dialect = csv.excel()
            file.seek(0)
        
        reader = csv.DictReader(file, dialect=dialect)
        
        with conn.cursor() as cursor:
            for row_num, row in enumerate(reader, 1):
                try:
                    # Extrai as colunas desejadas (com tratamento para nomes de colunas diferentes)
                    co_ncm = row.get('CO_NCM') or row.get('"CO_NCM"') or ''
                    no_ncm_por = row.get('NO_NCM_POR') or row.get('"NO_NCM_POR"') or ''
                    
                    # Remove aspas extras se existirem
                    co_ncm = str(co_ncm).strip('"').strip()
                    no_ncm_por = str(no_ncm_por).strip('"').strip()
                    
                    if not co_ncm or not no_ncm_por:
                        print(f"Aviso: Linha {row_num} está vazia ou faltando dados - CO_NCM: {co_ncm}, NO_NCM_POR: {no_ncm_por}")
                        continue
                    
                    # Insere os dados na tabela
                    cursor.execute(sql.SQL("""
                        INSERT INTO {} (co_ncm, no_ncm_por)
                        VALUES (%s, %s)
                        ON CONFLICT (co_ncm) DO UPDATE 
                        SET no_ncm_por = EXCLUDED.no_ncm_por
                    """).format(sql.Identifier(TABLE_NAME)), 
                    (co_ncm, no_ncm_por))
                    
                except Exception as e:
                    print(f"Erro ao processar linha {row_num}: {e}")
                    print(f"Conteúdo da linha: {row}")
            
            conn.commit()

def main():
    # Nome do arquivo CSV
    csv_file = './src/ncm_data/NCM.csv'
    
    try:
        # Conecta ao banco de dados
        with psycopg2.connect(**DB_CONFIG) as conn:
            print("Conexão com o PostgreSQL estabelecida com sucesso!")
            
            # Cria a tabela
            create_table(conn)
            print(f"Tabela {TABLE_NAME} criada/verificada com sucesso!")
            
            # Processa o CSV e insere os dados
            process_csv_and_insert_data(csv_file, conn)
            print("Dados inseridos/atualizados com sucesso!")
            
    except Exception as e:
        print(f"Erro ao conectar ou processar dados: {e}")
    finally:
        print("Processo concluído.")

if __name__ == '__main__':
    main()