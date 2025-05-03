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

