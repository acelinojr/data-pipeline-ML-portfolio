# Qualidade: Ausentes, Outliers, Gaps
import yfinance as yf
import pandas as pd
import numpy as np

def detect_outliers_iqr(series):
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return series[(series < lower_bound) | (series > upper_bound)]

def inspect_quality_crypto(ticker="BTC-USD", period="7d", interval="1h"):
    print(f"Verificando qualidade de: {ticker} | período: {period} | intervalo: {interval}")
    try:
        data = yf.download(ticker, period=period, interval=interval, progress=False)
        if data.empty:
            print("Dados vazios retornados.")
            return

        data.index = pd.to_datetime(data.index)

        # 2.1 — Valores ausentes
        print("\n=== Valores ausentes por coluna ===")
        print(data.isnull().sum())

        # 2.2 — Outliers
        print("\n=== Outliers detectados (IQR) ===")
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            if col in data.columns:
                outliers = detect_outliers_iqr(data[col])
                if not outliers.empty:
                    print(f"{col}: {len(outliers)} outliers encontrados")
                    print(outliers.head())
                else:
                    print(f"{col}: nenhum outlier detectado")

        # 2.3 — Gaps no índice (espera-se 1h contínuo, 24/7)
        full_range = pd.date_range(start=data.index.min(), end=data.index.max(), freq='1H')
        missing_dates = full_range.difference(data.index)
        print(f"\n=== Datas ausentes no índice (gaps horários) ===")
        print(f"Total de gaps: {len(missing_dates)}")
        if len(missing_dates) > 0:
            print("Exemplo de gaps (primeiros 5):")
            print(missing_dates[:5])
            print("Criptomoedas devem ter dados 24/7 — gaps podem indicar falha na API ou suspensão.")

    except Exception as e:
        print(f"Erro ao analisar qualidade: {e}")

# --- EXECUTAR ---
inspect_quality_crypto("BTC-USD", "7d", "1h")