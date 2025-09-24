# validate_recommended_tickers.py
import yfinance as yf
import pandas as pd

RECOMMENDED_CRYPTO_TICKERS = [
    "BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD",
    "ADA-USD", "DOGE-USD", "AVAX-USD", "LINK-USD", "MATIC-USD",
    "DOT-USD", "LTC-USD", "ATOM-USD", "UNI-USD", "SHIB-USD"
]

def validate_tickers(tickers, period="360d", interval="1d"):
    print(f"🔍 Validando {len(tickers)} tickers...")
    valid = []
    invalid = []

    for ticker in tickers:
        try:
            df = yf.download(ticker, period=period, interval=interval, progress=False, threads=False)
            if len(df) >= 300:  # aceita até 60 dias de gap (improvável em cripto)
                print(f"{ticker}: {len(df)} linhas")
                valid.append(ticker)
            else:
                print(f"{ticker}: apenas {len(df)} linhas (esperado ~360)")
                invalid.append(ticker)
        except Exception as e:
            print(f"{ticker}: erro — {e}")
            invalid.append(ticker)

    print(f"\nResultado: {len(valid)} válidos, {len(invalid)} inválidos ou incompletos")
    if invalid:
        print("Ticker(s) problemático(s):", invalid)

    return valid, invalid

# --- EXECUTAR ---
valid_tickers, invalid_tickers = validate_tickers(RECOMMENDED_CRYPTO_TICKERS)