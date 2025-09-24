import yfinance as yf
df = yf.download("BTC-USD", period="2d", interval="1h")
print("Últimas 10 linhas com Volume:")
print(df[['Close', 'Volume']].tail(10))
print(f"\nVolume zero count: {(df['Volume'] == 0).sum()}")
print(f"Volume total count: {len(df)}")


import yfinance as yf

tickers = "BTC-USD ETH-USD ADA-USD SOL-USD"
df = yf.download(tickers=tickers, period="360d", interval="1d", group_by="ticker")

print(df.columns)
print(df.head())