# Desafio SeuBoné

Este projeto consiste em um sistema para baixar, processar e visualizar dados de importação e exportação do Brasil

## 📋 Visão Geral

O sistema permite:
- Baixar bases de dados de importação e exportação do site oficial do governo
- Processar e carregar os dados em um banco de dados PostgreSQL
- Enriquecer os dados com descrições de produtos usando códigos NCM
- Visualizar análises através de um dashboard interativo

## 🛠️ Tecnologias Utilizadas

- **Python**: Linguagem principal do projeto
- **PostgreSQL**: Banco de dados relacional
- **Streamlit**: Framework para criação do dashboard interativo
- **Pandas**: Manipulação e análise de dados
- **Plotly**: Visualização de dados interativa
- **Requests/BeautifulSoup**: Web scraping para download de dados

## 🚀 Como Configurar e Executar

### Pré-requisitos

- Python 3.6+
- PostgreSQL
- Dependências Python (instale usando o comando abaixo):

```bash
pip install -r requirements.txt
```

### Configuração do Banco de Dados

O projeto está configurado para conectar a um banco de dados PostgreSQL com as seguintes configurações:

```python
DB_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin'
}
```

Ajuste estas configurações conforme necessário nos arquivos `send_data.py`, `ncm_data.py` e `dashboard.py`.

### Passos para Execução

1. **Baixar dados NCM(Baixe os dados de importação e exportação de 2020 e 2021)**:
   ```bash
   python download_data.py
   ```
   Este script permite baixar os dados de NCM do site oficial do governo.

2. **Carregar dados para o PostgreSQL(Envie os dados de importação e exportação de 2020 e 2021)**:
   ```bash
   python send_data.py
   ```
   Você será solicitado a fornecer o caminho do arquivo CSV e informar se os dados são de exportação (E) ou importação (I).

3. **Enriquecer dados com descrições NCM (Execute apenas depois carregar todos os dados)**:
   ```bash
   python ncm_data.py
   ```
   Este script adiciona as descrições dos produtos aos registros no banco de dados. Dados de importação geralmente vem nomeados com a inicial IMP e exportação EXP.

4. **Iniciar o dashboard**:
   ```bash
   streamlit run dashboard.py
   ```
   O dashboard será iniciado e estará disponível no navegador, normalmente em http://localhost:8501.

## 📊 Funcionalidades do Dashboard

Para funcionar corretamente baixe e envie ao banco de dados com o `send_data.py` e `download_data.py` os dados de exportação e importação dos anos de 2020 e 2021

O dashboard oferece as seguintes visualizações:

- **Exportações 2020-2021**: Top 3 produtos mais exportados para cada estado nos anos de 2020 e 2021
- **Importações 2020-2021**: Top 3 produtos mais importados para cada estado nos anos de 2020 e 2021
- **Exportações Mensais 2021**: Análise mensal dos top 3 produtos exportados para cada estado em 2021

## 🔧 Notas de Uso

- Os scripts de processamento de dados são interativos e solicitarão informações conforme necessário.
- É necessário ter acesso à internet para baixar os dados do site do governo.
- Ao carregar dados, é possível optar por limpar os dados existentes ou apenas adicionar novos registros.