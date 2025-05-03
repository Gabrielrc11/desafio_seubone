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
