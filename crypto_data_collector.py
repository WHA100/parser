import requests
import pandas as pd
import yfinance as yf
import os
import time
from tqdm import tqdm
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor

# Путь для сохранения данных
data_dir = "/home/wha10/neiro/"
os.makedirs(data_dir, exist_ok=True)

# Настройка базы данных SQLite
db_path = os.path.join(data_dir, "complete_crypto_data.db")
engine = create_engine(f"sqlite:///{db_path}")

# Файлы логов
log_file = os.path.join(data_dir, "error_log.txt")
delisted_log_file = os.path.join(data_dir, "delisted_log.txt")
completed_log_file = os.path.join(data_dir, "completed_log.txt")

# Функция для записи ошибок в лог
def log_error(message, log_type="error"):
    log_path = delisted_log_file if log_type == "delisted" else log_file
    with open(log_path, "a") as f:
        f.write(message + "\n")

# Функция для записи успешно загруженных данных
def log_completed(symbol):
    with open(completed_log_file, "a") as f:
        f.write(symbol + "\n")

# Чтение логов исключенных и завершенных тикеров
def get_processed_symbols():
    completed = set()
    delisted = set()

    if os.path.exists(completed_log_file):
        with open(completed_log_file, "r") as f:
            completed.update(line.strip() for line in f)

    if os.path.exists(delisted_log_file):
        with open(delisted_log_file, "r") as f:
            delisted.update(line.strip().split(":")[0] for line in f)

    return completed, delisted

# Функция для получения списка всех криптовалют с CoinGecko API
def get_all_cryptocurrencies():
    try:
        url = "https://api.coingecko.com/api/v3/coins/list"
        response = requests.get(url)

        if response.status_code == 200:
            coins = response.json()
            return sorted([(coin['id'], f"{coin['id'].upper()}-USD") for coin in coins])
        else:
            log_error("Ошибка при получении списка криптовалют")
            return []
    except Exception as e:
        log_error(f"Ошибка при запросе списка криптовалют: {e}")
        return []

# Функция для проверки наличия таблицы в базе данных
def is_table_exists(symbol):
    try:
        tables = engine.table_names()
        return symbol in tables
    except Exception as e:
        log_error(f"Ошибка при проверке таблицы {symbol}: {e}")
        return False

# Функция для сбора данных с Yahoo Finance
def fetch_yahoo_data(symbol, name):
    try:
        print(f"Сбор данных для {name} ({symbol})...")
        ticker = yf.Ticker(symbol)

        # Попробуем загрузить все доступные данные
        hist = ticker.history(period="max", interval="1d")
        if hist.empty:
            hist = ticker.history(period="5y", interval="1d")

        if hist.empty:
            log_error(f"{symbol}: no data found (may be delisted)", log_type="delisted")
            return

        hist.reset_index(inplace=True)
        hist['symbol'] = name
        hist.to_sql(symbol, engine, if_exists='replace', index=False)
        log_completed(symbol)
        print(f"Данные для {name} сохранены в базу данных")

    except Exception as e:
        log_error(f"Ошибка при сборе данных для {name} ({symbol}): {e}")

# Получение списка всех криптовалют
crypto_list = get_all_cryptocurrencies()
completed_symbols, delisted_symbols = get_processed_symbols()

# Функция для обработки одной криптовалюты
def process_crypto(crypto_id, symbol):
    if symbol in completed_symbols:
        print(f"Пропуск {crypto_id} ({symbol}) — уже загружено")
        return

    if symbol in delisted_symbols:
        print(f"Пропуск {crypto_id} ({symbol}) — делистинг")
        return

    if is_table_exists(symbol):
        print(f"Пропуск {crypto_id} ({symbol}) — данные уже в базе")
        log_completed(symbol)
        return

    fetch_yahoo_data(symbol, crypto_id)

# Используем многопоточность для ускорения загрузки
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(process_crypto, crypto_id, symbol) for crypto_id, symbol in crypto_list]

print("Сбор данных завершен.")
