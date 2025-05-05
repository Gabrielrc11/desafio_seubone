import streamlit as st
import pandas as pd
import psycopg2
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
from psycopg2 import sql

# Configurações do PostgreSQL (mesmas usadas no send_data.py)
DB_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin'
}

# Função para conectar ao banco de dados
@st.cache_resource
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# Função para executar consultas SQL e retornar os resultados como DataFrame
@st.cache_data
def run_query(query):
    conn = get_connection()
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Erro ao executar consulta: {e}")
        return pd.DataFrame()

# Função para obter a lista de estados disponíveis no banco
@st.cache_data
def get_estados():
    query = """
    SELECT DISTINCT "SG_UF_NCM" 
    FROM export_data
    WHERE "SG_UF_NCM" IS NOT NULL
    ORDER BY "SG_UF_NCM"
    """
    df = run_query(query)
    estados = df["SG_UF_NCM"].tolist()
    # Adicionar a opção "Todos" no início da lista
    estados.insert(0, "Todos")
    return estados

# Função para obter top 3 produtos exportados por estado nos anos 2020 e 2021
def get_top_exportacoes(estado, ano):
    # Condição WHERE para estados específicos ou para todos
    where_estado = f"\"SG_UF_NCM\" = '{estado}'" if estado != "Todos" else "1=1"
    
    query = f"""
    SELECT no_ncm_por as produto, 
           SUM("KG_LIQUIDO") as quantidade_total
    FROM export_data
    WHERE {where_estado}
      AND "CO_ANO" = {ano}
      AND no_ncm_por IS NOT NULL
    GROUP BY no_ncm_por
    ORDER BY quantidade_total DESC
    LIMIT 3
    """
    return run_query(query)

# Função para obter top 3 produtos importados por estado nos anos 2020 e 2021
def get_top_importacoes(estado, ano):
    # Condição WHERE para estados específicos ou para todos
    where_estado = f"\"SG_UF_NCM\" = '{estado}'" if estado != "Todos" else "1=1"
    
    query = f"""
    SELECT no_ncm_por as produto,
           SUM("KG_LIQUIDO") as quantidade_total
    FROM import_data
    WHERE {where_estado}
      AND "CO_ANO" = {ano}
      AND no_ncm_por IS NOT NULL
    GROUP BY no_ncm_por
    ORDER BY quantidade_total DESC
    LIMIT 3
    """
    return run_query(query)

# Função para obter top 3 produtos exportados por estado em cada mês de 2021
def get_top_exportacoes_mes(estado, mes):
    # Condição WHERE para estados específicos ou para todos
    where_estado = f"\"SG_UF_NCM\" = '{estado}'" if estado != "Todos" else "1=1"
    
    query = f"""
    SELECT no_ncm_por as produto, 
           SUM("KG_LIQUIDO") as quantidade_total
    FROM export_data
    WHERE {where_estado}
      AND "CO_ANO" = 2021
      AND "CO_MES" = {mes}
      AND no_ncm_por IS NOT NULL
    GROUP BY no_ncm_por
    ORDER BY quantidade_total DESC
    LIMIT 3
    """
    return run_query(query)

# Layout do Dashboard
st.set_page_config(page_title="Desafio SeuBoné", page_icon="📊", layout="wide")

# Título do Dashboard
st.title("Dashboard para o Desafio SeuBoné")

# Verificar conexão com o banco
try:
    conn = get_connection()
    st.success("Conexão com o banco de dados estabelecida com sucesso!")
except Exception as e:
    st.error(f"Erro ao conectar ao banco de dados: {e}")
    st.stop()

# Barra lateral para filtros
st.sidebar.header("Filtros")

# Seletor de estados
estados = get_estados()
if not estados:
    st.warning("Não foram encontrados estados nos dados. Verifique se os dados foram carregados corretamente.")
    st.stop()

estado_selecionado = st.sidebar.selectbox("Selecione o estado:", estados)

# Abas para diferentes visualizações
tab1, tab2, tab3 = st.tabs(["Exportações 2020-2021", "Importações 2020-2021", "Exportações Mensais 2021"])

# Tab 1: Exportações 2020-2021
with tab1:
    st.header(f"Top 3 Produtos Mais Exportados - {estado_selecionado}")
    
    col1, col2 = st.columns(2)
    
    # Dados de 2020
    with col1:
        st.subheader("2020")
        df_exp_2020 = get_top_exportacoes(estado_selecionado, 2020)
        
        if not df_exp_2020.empty:
            # Criando um gráfico de barras com Plotly
            fig = px.bar(
                df_exp_2020, 
                x='produto', 
                y='quantidade_total',
                title=f"Top 3 Produtos Exportados - {estado_selecionado} - 2020",
                labels={"quantidade_total": "Quantidade (KG)", "produto": ""},
                text_auto=True
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Exibindo os dados em formato tabular
            st.dataframe(
                df_exp_2020.style.format({"quantidade_total": "{:,.2f} KG"})
            )
        else:
            st.info(f"Não há dados de exportação para {estado_selecionado} em 2020.")
    
    # Dados de 2021
    with col2:
        st.subheader("2021")
        df_exp_2021 = get_top_exportacoes(estado_selecionado, 2021)
        
        if not df_exp_2021.empty:
            # Criando um gráfico de barras com Plotly
            fig = px.bar(
                df_exp_2021, 
                x='produto', 
                y='quantidade_total',
                title=f"Top 3 Produtos Exportados - {estado_selecionado} - 2021",
                labels={"quantidade_total": "Quantidade (KG)", "produto": ""},
                text_auto=True
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Exibindo os dados em formato tabular
            st.dataframe(
                df_exp_2021.style.format({"quantidade_total": "{:,.2f} KG"})
            )
        else:
            st.info(f"Não há dados de exportação para {estado_selecionado} em 2021.")

# Tab 2: Importações 2020-2021
with tab2:
    st.header(f"Top 3 Produtos Mais Importados - {estado_selecionado}")
    
    col1, col2 = st.columns(2)
    
    # Dados de 2020
    with col1:
        st.subheader("2020")
        df_imp_2020 = get_top_importacoes(estado_selecionado, 2020)
        
        if not df_imp_2020.empty:
            # Criando um gráfico de barras com Plotly
            fig = px.bar(
                df_imp_2020, 
                x='produto', 
                y='quantidade_total',
                title=f"Top 3 Produtos Importados - {estado_selecionado} - 2020",
                labels={"quantidade_total": "Quantidade (KG)", "produto": ""},
                text_auto=True
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Exibindo os dados em formato tabular
            st.dataframe(
                df_imp_2020.style.format({"quantidade_total": "{:,.2f} KG"})
            )
        else:
            st.info(f"Não há dados de importação para {estado_selecionado} em 2020.")
    
    # Dados de 2021
    with col2:
        st.subheader("2021")
        df_imp_2021 = get_top_importacoes(estado_selecionado, 2021)
        
        if not df_imp_2021.empty:
            # Criando um gráfico de barras com Plotly
            fig = px.bar(
                df_imp_2021, 
                x='produto', 
                y='quantidade_total',
                title=f"Top 3 Produtos Importados - {estado_selecionado} - 2021",
                labels={"quantidade_total": "Quantidade (KG)", "produto": ""},
                text_auto=True
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Exibindo os dados em formato tabular
            st.dataframe(
                df_imp_2021.style.format({"quantidade_total": "{:,.2f} KG"})
            )
        else:
            st.info(f"Não há dados de importação para {estado_selecionado} em 2021.")

# Tab 3: Exportações Mensais 2021
with tab3:
    st.header(f"Top 3 Produtos Exportados por Mês em 2021 - {estado_selecionado}")
    
    nomes_meses = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }

    # Exibir todos os meses em um grid
    st.subheader("Visão Geral de Todos os Meses")
    
    # Criando um layout com 3 colunas
    cols = st.columns(3)
    
    for mes in range(1, 13):
        df_mes = get_top_exportacoes_mes(estado_selecionado, mes)
        col_index = (mes - 1) % 3
        
        with cols[col_index]:
            st.write(f"**{nomes_meses[mes]}**")
            
            if not df_mes.empty:
                # Mostrando uma tabela simplificada para cada mês
                st.dataframe(
                    df_mes.style.format({"quantidade_total": "{:,.2f} KG"})
                )
            else:
                st.info(f"Sem dados")

# Rodapé
st.markdown("---")
st.caption("Dashboard criado por Gabriel Henrique Rocha de Carvalho para o Desafio SeuBoné.")
st.caption("Repositorio do projeto: [github.com/Gabrielrc11/desafio_seubone](https://github.com/Gabrielrc11/desafio_seubone)")