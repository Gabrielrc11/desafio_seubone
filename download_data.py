"""
Script para baixar a base de dados
"""

import os
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from tqdm import tqdm

def get_user_year():
    """Permite que o usuário escolha o ano da base de dados."""
    current_year = datetime.now().year
    while True:
        try:
            year_input = input(f"Digite o ano da base de dados NCM (1997-{current_year}) ou pressione Enter para usar {current_year}: ")
            
            # Se o usuário apenas pressionar Enter, use o ano atual
            if not year_input.strip():
                return current_year
            
            year = int(year_input)
            if 1997 <= year <= current_year:
                return year
            else:
                print(f"Por favor, insira um ano entre 1997 e {current_year}.")
        except ValueError:
            print("Por favor, insira um número válido para o ano.")

def download_file(url, destination):
    """
    Baixa um arquivo
    """
    response = requests.get(url, stream=True)
    
    if response.status_code != 200:
        print(f"Erro ao baixar o arquivo: {response.status_code}")
        return False
    
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024  # 1 KB
    
    with open(destination, 'wb') as file, tqdm(
            desc=os.path.basename(destination),
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
        for data in response.iter_content(block_size):
            file.write(data)
            bar.update(len(data))
    
    return True

def main():
    # Configurações
    base_url = "https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta"
    output_directory = "src/ncm_data"
    
    # Solicitar ao usuário que escolha o ano
    selected_year = get_user_year()
    
    # Criar diretório de saída se não existir
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    
    # Fazer requisição ao site
    print(f"Acessando {base_url}...")
    try:
        response = requests.get(base_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar o site: {e}")
        return
    
    # Analisar o HTML da página
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Buscar links para a base de dados NCM do ano selecionado
    print(f"Procurando base de dados NCM para o ano {selected_year}...")
    # Ajustado para buscar arquivos sem a extensão .zip específica
    pattern = re.compile(f".*NCM.*{selected_year}.*", re.IGNORECASE)
    
    # Encontrar todos os links na página
    links = soup.find_all('a', href=True)
    
    # Filtrar links que correspondem ao padrão da base de dados NCM do ano atual
    ncm_links = []
    for link in links:
        if pattern.search(link.text) or (link.get('href') and pattern.search(link.get('href'))):
            href = link.get('href')
            if not href.startswith(('http://', 'https://')):
                # Se o link for relativo, convertê-lo para absoluto
                if href.startswith('/'):
                    href = f"https://www.gov.br{href}"
                else:
                    href = f"{base_url}/{href}"
            ncm_links.append((link.text.strip(), href))
    
    if not ncm_links:
        print(f"Não foi encontrado nenhum link para a base de dados de {selected_year}.")
        return
    
    # Mostrar os links encontrados
    print(f"Encontrados {len(ncm_links)} links relacionados à base de dados de {selected_year}:")
    for i, (text, url) in enumerate(ncm_links, 1):
        print(f"{i}. {text} - {url}")
    
    # Se houver múltiplos links, permitir que o usuário escolha
    if len(ncm_links) > 1:
        try:
            choice = int(input(f"Escolha o número do link para download (1-{len(ncm_links)}): "))
            if 1 <= choice <= len(ncm_links):
                selected_link = ncm_links[choice-1][1]
            else:
                print("Opção inválida. Usando o primeiro link.")
                selected_link = ncm_links[0][1]
        except ValueError:
            print("Entrada inválida. Usando o primeiro link.")
            selected_link = ncm_links[0][1]
    else:
        selected_link = ncm_links[0][1]
    
    # Baixar o arquivo
    file_name = os.path.basename(selected_link) if selected_link.split('/')[-1] else f"ncm_data_{selected_year}"
    output_path = os.path.join(output_directory, file_name)
    
    print(f"Baixando {selected_link} para {output_path}...")
    if download_file(selected_link, output_path):
        print(f"Download concluído: {output_path}")
        print(f"Arquivo salvo em: {os.path.abspath(output_path)}")
    else:
        print("Falha no download do arquivo.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperação cancelada pelo usuário.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")