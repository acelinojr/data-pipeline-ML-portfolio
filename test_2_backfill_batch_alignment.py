# test_2_batch_alignment.py
import yfinance as yf
import pandas as pd

def validate_batch_alignment(tickers=None, period="360d", interval="1d"):
    if tickers is None:
        tickers = [
            "BTC-USD", "ETH-USD", "XRP-USD", "BNB-USD", "SOL-USD",
            "DOGE-USD", "ADA-USD", "LINK-USD", "AVAX-USD", "TRX-USD"
        ]

    print(f"Validando alinhamento temporal de {len(tickers)} tickers...")
    all_data = {}
    failed = []

    for ticker in tickers:
        try:
            df = yf.download(ticker, period=period, interval=interval, progress=False, threads=False)
            if df.empty:
                print(f"{ticker}: vazio")
                failed.append(ticker)
                continue
            df.index = pd.to_datetime(df.index)
            all_data[ticker] = df
            print(f"{ticker}: {len(df)} linhas")
        except Exception as e:
            print(f"{ticker}: erro — {e}")
            failed.append(ticker)

    if len(all_data) == 0:
        print("Nenhum dado válido coletado.")
        return None, failed

    # Encontra a interseção de datas (datas comuns a todos os ativos)
    common_dates = None
    for df in all_data.values():
        if common_dates is None:
            common_dates = set(df.index)
        else:
            common_dates = common_dates.intersection(set(df.index))

    print(f"\nTotal de datas comuns entre todos os tickers: {len(common_dates)}")
    if len(common_dates) == 0:
        print("NENHUMA data em comum — impossível alinhar os dados.")
        return None, failed

    # Cria um índice comum e realinha todos
    common_index = sorted(common_dates)
    realigned = {}
    for ticker, df in all_data.items():
        df_aligned = df.reindex(common_index)
        realigned[ticker] = df_aligned
        gaps_introduced = int(df_aligned['Close'].isnull().sum())
        if gaps_introduced > 0:
            print(f"⚠️ {ticker} : {gaps_introduced} gaps introduzidos ao realinhar")

    print(f"Todos os tickers realinhados para {len(common_index)} datas comuns.")
    return realigned, failed

# --- EXECUTAR ---
CRYPTO_TICKERS = [
    "BTC-USD", "ETH-USD", "XRP-USD", "BNB-USD", "SOL-USD",
    "DOGE-USD", "ADA-USD", "LINK-USD", "AVAX-USD", "TRX-USD"
]
aligned_data, failed_tickers = validate_batch_alignment(CRYPTO_TICKERS, "360d", "1d")