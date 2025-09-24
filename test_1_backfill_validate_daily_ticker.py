# validate_daily_ticker_fixed_v2.py
import yfinance as yf
import pandas as pd

def validate_daily_ticker(ticker="BTC-USD", period="360d", interval="1d"):
    print(f"🔍 Validando: {ticker} | {period} | {interval}")
    try:
        # Baixa os dados
        data = yf.download(ticker, period=period, interval=interval, progress=False, threads=False)
        
        # Verifica se é DataFrame
        if not isinstance(data, pd.DataFrame):
            print(f"❌ {ticker}: retorno inesperado (não é DataFrame) — tipo: {type(data)}")
            return False, None

        if data.empty:
            print(f"❌ {ticker}: DataFrame vazio")
            return False, None

        # Flatten MultiIndex columns (caso exista)
        if isinstance(data.columns, pd.MultiIndex):
            print("⚠️  Detectado MultiIndex nas colunas — convertendo para colunas simples...")
            data.columns = [col[0] for col in data.columns]  # Mantém apenas o nome da métrica (Close, High, etc.)

        # Verifica se colunas essenciais existem
        required_cols = ['Close', 'Volume']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            print(f"❌ {ticker}: colunas ausentes — {missing_cols}")
            print(f"Colunas disponíveis: {list(data.columns)}")
            return False, None

        print(f"✅ {ticker}: {len(data)} linhas retornadas")

        # Verifica ausentes
        missing_close = data['Close'].isnull().sum()
        missing_volume = data['Volume'].isnull().sum()
        if missing_close > 0 or missing_volume > 0:
            print(f"⚠️  {ticker}: {missing_close} Close NaN, {missing_volume} Volume NaN")

        # Verifica índice
        try:
            data.index = pd.to_datetime(data.index)
        except Exception as e:
            print(f"⚠️  {ticker}: falha ao converter índice para datetime — {e}")
            return False, None

        is_monotonic = data.index.is_monotonic_increasing
        has_duplicates = data.index.duplicated().any()
        if not is_monotonic:
            print(f"⚠️  {ticker}: índice não monotônico")
        if has_duplicates:
            print(f"⚠️  {ticker}: índice com duplicatas")

        # Verifica gaps (cripto = 24/7, então não deveria ter gaps)
        full_range = pd.date_range(start=data.index.min(), end=data.index.max(), freq='D')
        missing_dates = full_range.difference(data.index)
        if len(missing_dates) > 0:
            print(f"⚠️  {ticker}: {len(missing_dates)} gaps no índice (ex: {missing_dates[:3].tolist()})")

        return True, data

    except Exception as e:
        print(f"❌ {ticker}: Erro — {e}")
        return False, None

# --- EXECUTAR ---
validate_daily_ticker("BTC-USD", "360d", "1d")




import yfinance as yf
import pandas as pd

ticker = "BTC-USD"
data = yf.download(ticker, period="360d", interval="1d", threads=False, progress=False)

print("Tipo do retorno:", type(data))
if isinstance(data, pd.DataFrame):
    print("É DataFrame. Vazio?", data.empty)
    print("Colunas:", list(data.columns))
    print("Linhas:", len(data))
else:
    print("Conteúdo:", data)