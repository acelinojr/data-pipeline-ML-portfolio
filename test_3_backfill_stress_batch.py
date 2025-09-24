# test_3_stress_batch.py
import yfinance as yf
import time

def stress_test_batch_download(tickers=None, period="360d", interval="1d"):
    if tickers is None:
        tickers = [
            "BTC-USD", "ETH-USD", "XRP-USD", "BNB-USD", "SOL-USD",
            "DOGE-USD", "ADA-USD", "LINK-USD", "AVAX-USD", "TRX-USD"
        ]

    print(f"Iniciando stress test: {len(tickers)} tickers | {period} | {interval}")
    start_time = time.time()
    results = {}

    for i, ticker in enumerate(tickers):
        try:
            df = yf.download(ticker, period=period, interval=interval, progress=False, threads=False)
            results[ticker] = len(df) if not df.empty else 0
            print(f"{i+1}/{len(tickers)}: {ticker} → {results[ticker]} linhas")
        except Exception as e:
            results[ticker] = f"ERROR: {str(e)[:60]}"
            print(f"{i+1}/{len(tickers)}: {ticker} → erro")

        # Evita throttling — opcional, mas recomendado
        time.sleep(0.3)

    elapsed = time.time() - start_time
    print(f"\n⏱Tempo total: {elapsed:.2f}s | {len(tickers)/elapsed:.2f} tickers/segundo")
    print(f"Sucesso: {sum(1 for x in results.values() if isinstance(x, int) and x > 0)}")
    print(f"Falhas: {sum(1 for x in results.values() if isinstance(x, str))}")

    return results

# --- EXECUTAR ---
stress_test_batch_download()