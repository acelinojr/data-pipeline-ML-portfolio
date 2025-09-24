#!/usr/bin/env python3
"""
yahoo_scraper.py

Versão simplificada do scraper:
- Sem Prometheus / métricas externas
- Retry/backoff no fetch (yfinance)
- Trunca timestamps para hora (idempotência)
- Validação básica + quality_flags armazenadas (JSON)
- Upsert em lote para `raw_crypto`
- Config via ENV vars

Requisitos mínimos:
pip install pandas yfinance mysql-connector-python python-dateutil
"""

import os
import time
import uuid
import json
import logging
import requests
from datetime import datetime
from typing import List, Tuple, Dict

import pandas as pd
import yfinance as yf
import mysql.connector
from mysql.connector import pooling, Error

# -----------------------
# Config (via ENV)
# -----------------------
DB_HOST = os.getenv("MYSQL_HOST", "db")
DB_PORT = int(os.getenv("MYSQL_PORT", "3306"))
DB_USER = os.getenv("MYSQL_USER", "Acelino")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD", "senha123")
DB_NAME = os.getenv("MYSQL_DB", "projet_crypto")

REQUESTS_PER_SECOND = float(os.getenv("RPS", "2.0"))
RETRY_MAX = int(os.getenv("RETRY_MAX", "3"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
# -----------------------

# ---------- Logging ----------
logger = logging.getLogger("yahoo_scraper")
logger.setLevel(LOG_LEVEL)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)

# ---------- DB pool ----------
POOL = pooling.MySQLConnectionPool(
    pool_name="crypto_pool_simple",
    pool_size=3,
    host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, autocommit=False
)

# ---------- Util helpers ----------
def make_scrape_id() -> str:
    return uuid.uuid4().hex

def _sleep_rate_control(last_ts: float, rps: float) -> float:
    min_interval = 1.0 / max(1.0, rps)
    elapsed = time.time() - last_ts
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
    return time.time()

def truncate_to_hour(ts: pd.Timestamp) -> datetime:
    """
    Recebe um pandas Timestamp (tz-aware ou naive), converte para UTC e trunca para hora.
    Retorna datetime (naive UTC) adequado para gravar em DATETIME(6).
    """
    if not isinstance(ts, pd.Timestamp):
        ts = pd.Timestamp(ts)
    # garantir tz-aware em UTC
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    ts = ts.replace(minute=0, second=0, microsecond=0)
    return ts.to_pydatetime().replace(tzinfo=None)

# ---------- Fetch with retries ----------
def fetch_ticker_df(ticker: str, period: str = "7d", interval: str = "1h", retry_max: int = RETRY_MAX) -> pd.DataFrame:
    attempt = 0
    last_call = 0.0
    while attempt <= retry_max:
        attempt += 1
        try:
            last_call = _sleep_rate_control(last_call, REQUESTS_PER_SECOND)
            logger.debug("fetching %s (period=%s interval=%s) attempt=%d", ticker, period, interval, attempt)
            df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, threads=False, progress=False)
            if df is None or df.empty:
                logger.warning("Empty result for %s (attempt %d)", ticker, attempt)
                if attempt <= retry_max:
                    time.sleep(2 ** attempt)
                    continue
                return pd.DataFrame()
            # Normalize columns
            if isinstance(df.columns, pd.MultiIndex):
                out = pd.DataFrame(index=df.index)
                for col in ["Open", "High", "Low", "Close", "Volume"]:
                    matches = [c for c in df.columns if c[0] == col]
                    out[col] = df[matches[0]] if matches else pd.NA
                df = out
            else:
                df = df.rename(columns=lambda s: s.capitalize())
            # ensure timezone UTC
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            else:
                df.index = df.index.tz_convert("UTC")
            # coerce types
            for c in ["Open", "High", "Low", "Close"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            if "Volume" in df.columns:
                df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype("Int64")
            return df
        except Exception as e:
            logger.exception("Error fetching %s: %s", ticker, e)
            if attempt <= retry_max:
                wait = min(60, 2 ** attempt)
                logger.info("Retrying %s in %ds", ticker, wait)
                time.sleep(wait)
            else:
                logger.error("Giving up fetching %s after %d attempts", ticker, attempt)
                return pd.DataFrame()
    return pd.DataFrame()

# ---------- Basic quality checks ----------
def compute_quality_flags(df: pd.DataFrame) -> Dict:
    flags = {"n_rows": int(len(df))}
    if df.empty:
        flags["empty"] = True
        return flags
    flags["n_nulls"] = int(df.isna().sum().sum())
    freq = pd.infer_freq(df.index)
    flags["freq"] = freq if freq else None
    if freq:
        full = pd.date_range(start=df.index.min(), end=df.index.max(), freq=freq)
        flags["gaps"] = int(len(full) - len(df.index.unique()))
    else:
        flags["gaps"] = None
    flags["zero_volume_rows"] = int((df.get("Volume", pd.Series([], dtype="Int64")) == 0).sum())
    return flags

# ---------- Upsert (batch) ----------
UPSERT_SQL = """
INSERT INTO raw_crypto
(symbol, name, price_usd, change_24h_percent, volume_24h_usd, timestamp, source, scrape_id, is_valid, quality_flags, created_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    price_usd = VALUES(price_usd),
    change_24h_percent = VALUES(change_24h_percent),
    volume_24h_usd = VALUES(volume_24h_usd),
    source = VALUES(source),
    scrape_id = VALUES(scrape_id),
    is_valid = VALUES(is_valid),
    quality_flags = VALUES(quality_flags),
    created_at = VALUES(created_at)
;
"""

def upsert_dataframe_to_raw(ticker: str, name: str, df: pd.DataFrame, scrape_id: str, source: str = "yahoo_finance") -> Tuple[int,int]:
    """Converte DataFrame em linhas com timestamp truncado e faz upsert em lote.
    Usa transação explícita para garantir atomicidade por execução.
    Retorna (rows_processed, error_count).
    """
    if df is None or df.empty:
        return 0, 0

    rows = []
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    for ts, row in df.iterrows():
        try:
            ts_trunc = truncate_to_hour(ts)  # datetime naive UTC truncated to hour
            price = None if pd.isna(row.get("Close")) else float(row.get("Close"))
            volume = 0 if pd.isna(row.get("Volume")) else int(row.get("Volume"))
            change_pct = None  # calcular no ETL downstream
            quality = {}  # por linha, se quiser popular
            rows.append((ticker, name, price, change_pct, volume, ts_trunc, source, scrape_id, True, json.dumps(quality), now))
        except Exception as e:
            logger.exception("Row prepare error for %s %s: %s", ticker, ts, e)
            # não aborta a preparação; apenas ignora linha que deu problema
            continue

    if not rows:
        return 0, 0

    conn = POOL.get_connection()
    # garantir que autocommit está DESLIGADO (pool foi criado com autocommit=False, mas checamos)
    try:
        conn.autocommit = False
    except Exception:
        # se pool/connector não suportar esse atributo, ignore; usaremos start_transaction abaixo
        pass

    cursor = conn.cursor()
    inserted = 0
    errors = 0

    try:
        # Inicia transação explícita que cobrirá todos os batches
        conn.start_transaction()
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i+BATCH_SIZE]
            cursor.executemany(UPSERT_SQL, batch)
            # NÃO comitar por batch — mantemos tudo na mesma transação para atomicidade
            inserted += cursor.rowcount

        # tudo ok: comitar a transação inteira
        conn.commit()
    except Error as e:
        logger.exception("DB error during upsert (transaction will be rolled back): %s", e)
        try:
            conn.rollback()
        except Exception as e2:
            logger.exception("Error during rollback: %s", e2)
        errors = 1
        inserted = 0  # opcional: considerar que nada foi aplicado / contabilizar como 0
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    return inserted, errors


# ---------- Main flow ----------
def scrape_and_store(tickers: List[str], period: str = "7d", interval: str = "1h") -> Dict:
    start_time = time.time()  # mede o início da execução
    scrape_id = make_scrape_id()
    logger.info("Starting scrape id=%s tickers=%s period=%s interval=%s",
                scrape_id, tickers, period, interval)

    stats = {"success": 0, "empty": 0, "errors": 0, "rows": 0}

    for t in tickers:
        try:
            df = fetch_ticker_df(t, period=period, interval=interval)
            flags = compute_quality_flags(df)

            if df.empty:
                logger.warning("Ticker %s returned empty df. flags=%s", t, flags)
                stats["empty"] += 1
                continue

            inserted, errs = upsert_dataframe_to_raw(
                ticker=t, name=t, df=df, scrape_id=scrape_id
            )
            stats["rows"] += inserted

            if errs == 0:
                stats["success"] += 1
            else:
                stats["errors"] += 1

            logger.info("Ticker %s inserted=%d errs=%d flags=%s", t, inserted, errs, flags)

        except Exception as e:
            logger.exception("Unhandled error for %s: %s", t, e)
            stats["errors"] += 1

    # calcula duração em ms
    duration_sec = time.time() - start_time
    latencia_ms = duration_sec * 1000

    # monta payload para o NiFi
    payload = {
        "flow_name": "yahoo_scraper",
        "symbol": ",".join(tickers),  # ou um por vez, se preferir granularidade
        "records_total": stats["rows"],
        "errors": stats["errors"],
        "latencia_ms": latencia_ms,
        "status": "success" if stats["errors"] == 0 else "failure",
        "error_type": "none" if stats["errors"] == 0 else "scraper_error",
        "regiao_origem": "scraper"
    }

    # envia direto para o NiFi via HTTP POST
    try:
        resp = requests.post(
            "http://nifi:8080/contentListener",  # ListenHTTP em HTTP
            json=payload,
            timeout=5
        )   
        if resp.status_code != 200:
            logger.warning("Falha ao enviar métricas para NiFi: %s", resp.text)
    except Exception as e:
        logger.error("Erro ao conectar ao NiFi: %s", e)

    logger.info("Scrape finished id=%s stats=%s", scrape_id, stats)
    return {"scrape_id": scrape_id, **stats}

# ---------------- CLI ----------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Simplified crypto scraper (yfinance -> MySQL raw_crypto)")
    parser.add_argument("--tickers", required=True, help="Comma-separated e.g. BTC-USD,ETH-USD")
    parser.add_argument("--period", default="7d")
    parser.add_argument("--interval", default="1h")
    args = parser.parse_args()

    tickers = [s.strip() for s in args.tickers.split(",") if s.strip()]
    result = scrape_and_store(tickers, period=args.period, interval=args.interval)
    print(result)
