# Índice: Granularidade, Timezone, Consistência
import yfinance as yf
import pandas as pd

def inspect_index_crypto(ticker="BTC-USD", period="7d", interval="1h"):
    print(f"Verificando índice temporal de: {ticker} | período: {period} | intervalo: {interval}")
    try:
        data = yf.download(ticker, period=period, interval=interval, progress=False)
        if data.empty:
            print("Dados vazios retornados.")
            return

        data.index = pd.to_datetime(data.index)

        # 3.1 — Granularidade
        inferred_freq = pd.infer_freq(data.index)
        print(f"\n=== Frequência inferida do índice: {inferred_freq or 'NÃO INFERIDA'}")
        if inferred_freq != 'H':
            print("Frequência esperada: 'H' (hora cheia) — verifique se há gaps ou duplicatas.")

        # 3.2 — Duplicatas
        duplicated = data.index.duplicated()
        if duplicated.any():
            print(f"{duplicated.sum()} timestamps duplicados encontrados!")
            print(data.index[duplicated])
        else:
            print("Sem duplicatas no índice.")

        # 3.3 — Timezone
        tz = data.index.tz
        print(f"Timezone do índice: {tz or 'naive (sem fuso)'}")
        if tz is None:
            print("Dados geralmente retornados em UTC, mas sem tz-aware — converta se necessário.")

        # 3.4 — Monotonicidade
        is_monotonic = data.index.is_monotonic_increasing
        print(f"Índice monotônico crescente: {'Sim' if is_monotonic else 'NÃO'}")
        if not is_monotonic:
            print("Índice fora de ordem — risco de análise incorreta!")

    except Exception as e:
        print(f"Erro ao inspecionar índice: {e}")

# --- EXECUTAR ---
inspect_index_crypto("BTC-USD", "7d", "1h")