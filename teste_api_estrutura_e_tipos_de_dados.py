# Estrutura e Tipos de Dados
import yfinance as yf
import pandas as pd

def inspect_structure_crypto(ticker="BTC-USD", period="7d", interval="1h"):
    print(f"Analisando estrutura de: {ticker} | período: {period} | intervalo: {interval}")
    try:
        data = yf.download(ticker, period=period, interval=interval, progress=False)
        if data.empty:
            print("Dados vazios retornados.")
            return

        print("\n=== Estrutura do DataFrame ===")
        print(data.info())
        print("\n=== Primeiras linhas ===")
        print(data.head(3))
        print("\n=== Estatísticas descritivas ===")
        print(data.describe(include='all'))

    except Exception as e:
        print(f"Erro ao baixar dados: {e}")

# --- EXECUTAR ---
inspect_structure_crypto("BTC-USD", "7d", "1h")