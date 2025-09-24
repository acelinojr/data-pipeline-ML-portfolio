# Performance e limites da API
import yfinance as yf
import time
import warnings

warnings.filterwarnings('ignore')

def stress_test_crypto_api(tickers=None, period="7d", interval="1h", max_requests=10):
    if tickers is None:
        tickers = [
            "BTC-USD", "ETH-USD", "XRP-USD", "BNB-USD", "SOL-USD",
            "DOGE-USD", "ADA-USD", "LINK-USD", "AVAX-USD", "TRX-USD"
        ]
    tickers = tickers[:max_requests]

    print(f"Iniciando teste de performance — {len(tickers)} criptomoedas | período: {period} | intervalo: {interval}")
    start_time = time.time()

    results = {}
    for i, ticker in enumerate(tickers):
        try:
            df = yf.download(ticker, period=period, interval=interval, progress=False)
            results[ticker] = len(df) if not df.empty else 0
        except Exception as e:
            results[ticker] = f"ERROR: {str(e)[:50]}..."
        if (i + 1) % 5 == 0 or i + 1 == len(tickers):
            print(f"Progresso: {i+1}/{len(tickers)}")

    elapsed = time.time() - start_time
    rps = len(tickers) / elapsed if elapsed > 0 else 0

    print(f"\nTempo total: {elapsed:.2f} segundos")
    print(f"Requisições por segundo: {rps:.2f}")

    # Resumo de falhas
    errors = {k: v for k, v in results.items() if isinstance(v, str) and "ERROR" in v}
    empty = {k: v for k, v in results.items() if isinstance(v, int) and v == 0}

    print(f"\nResultados:")
    print(f"   Sucesso: {len(tickers) - len(errors) - len(empty)}")
    print(f"   Vazios: {len(empty)} → Possível ticker inválido ou sem dados no período")
    print(f"   Erros: {len(errors)}")
    if errors:
        print("   Exemplo de erro:", list(errors.items())[0])

# --- EXECUTAR ---
CRYPTO_TICKERS = [
    "BTC-USD", "ETH-USD", "XRP-USD", "USDT-USD", "BNB-USD",
    "SOL-USD", "USDC-USD", "DOGE-USD", "ADA-USD", "TRX-USD"
]
stress_test_crypto_api(CRYPTO_TICKERS, "7d", "1h", max_requests=10)