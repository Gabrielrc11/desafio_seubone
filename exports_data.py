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
