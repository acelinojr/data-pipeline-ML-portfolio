#!/usr/bin/env python3
"""
backfill_run_once.py

Run-once backfill script:
 - Backfill N dias (default 360) até `--end` (default hoje UTC)
 - Intervalo diário ('1d')
 - Upsert por dia (timestamp truncado para 00:00:00 UTC)
 - Grava scrape_id e quality_flags por ticker
"""

import os
import sys
import time
import json
import uuid
import logging
from datetime import datetime, date, timedelta

import pandas as pd
import yfinance as yf
import mysql.connector
from mysql.connector import pooling, Error

# -------------------- Config (ENV-friendly) --------------------
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3307"))
DB_USER = os.getenv("DB_USER", "Acelino")
DB_PASSWORD = os.getenv("DB_PASSWORD", "senha123")
DB_NAME = os.getenv("DB_NAME", "projeto_crypto")

REQUESTS_PER_SECOND = float(os.getenv("RPS", "1.0"))  # diário, pode ser 1
RETRY_MAX = int(os.getenv("RETRY_MAX", "3"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
# --------------------------------------------------------------

logger = logging.getLogger("backfill")
logger.setLevel(LOG_LEVEL)
h = logging.StreamHandler()
h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(h)

# DB pool (pequeno pool para um run_once)
POOL = pooling.MySQLConnectionPool(
    pool_name="backfill_pool",
    pool_size=3,
    host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, autocommit=False
)

# UPSERT SQL (compatível com seu schema raw_crypto)
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

def make_scrape_id():
    return uuid.uuid4().hex

def sleep_rate_control(last_ts: float, rps: float) -> float:
    min_interval = 1.0 / max(0.1, rps)
    elapsed = time.time() - last_ts
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
    return time.time()

def truncate_to_day(dt: pd.Timestamp) -> datetime:
    """Trunca para 00:00:00 UTC e retorna datetime (naive UTC)"""
    if not isinstance(dt, pd.Timestamp):
        dt = pd.Timestamp(dt)
    if dt.tz is None:
        dt = dt.tz_localize("UTC")
    else:
        dt = dt.tz_convert("UTC")
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    # devolver naive (DATETIME em UTC)
    return dt.to_pydatetime().replace(tzinfo=None)

def fetch_daily(ticker: str, start_date: date, end_date_inclusive: date, retry_max: int = RETRY_MAX, rps: float = REQUESTS_PER_SECOND) -> pd.DataFrame:
    """
    Baixa dados diários para ticker entre start (inclusive) e end_date_inclusive (inclusive).
    Usa yf.download(start=..., end=...) onde end é exclusivo, então passa end+1.
    Retorna DataFrame com colunas Open/High/Low/Close/Volume e index como DatetimeIndex tz-aware UTC.
    """
    start_str = start_date.strftime("%Y-%m-%d")
    end_excl = end_date_inclusive + timedelta(days=1)
    end_str = end_excl.strftime("%Y-%m-%d")
    attempt = 0
    last_call = 0.0
    while attempt <= retry_max:
        attempt += 1
        try:
            last_call = sleep_rate_control(last_call, rps)
            logger.info("Fetching %s start=%s end=%s (attempt %d)", ticker, start_str, end_str, attempt)
            df = yf.download(ticker, start=start_str, end=end_str, interval="1d", auto_adjust=False, threads=False, progress=False)
            if df is None or df.empty:
                logger.warning("Empty daily DF for %s (attempt %d)", ticker, attempt)
                if attempt <= retry_max:
                    time.sleep(2 ** attempt)
                    continue
                return pd.DataFrame()
            # Normalize columns to capitalized names
            if isinstance(df.columns, pd.MultiIndex):
                out = pd.DataFrame(index=df.index)
                for col in ["Open","High","Low","Close","Volume"]:
                    matches = [c for c in df.columns if c[0]==col]
                    out[col] = df[matches[0]] if matches else pd.NA
                df = out
            else:
                df = df.rename(columns=lambda s: s.capitalize())
            # Ensure timezone aware UTC (daily often comes tz-naive)
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            else:
                df.index = df.index.tz_convert("UTC")
            # types
            for c in ["Open","High","Low","Close"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            if "Volume" in df.columns:
                df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype("Int64")
            return df
        except Exception as e:
            logger.exception("Error fetching %s: %s", ticker, e)
            if attempt <= retry_max:
                wait = min(60, 2 ** attempt)
                logger.info("Retry in %ds", wait)
                time.sleep(wait)
            else:
                logger.error("Giving up fetch %s after %d attempts", ticker, attempt)
                return pd.DataFrame()

def compute_quality(df: pd.DataFrame) -> dict:
    flags = {"n_rows": int(len(df))}
    if df.empty:
        flags["empty"] = True
        return flags
    flags["n_nulls"] = int(df.isna().sum().sum())
    freq = pd.infer_freq(df.index)
    flags["freq"] = str(freq) if freq else None
    # gaps daily
    if freq:
        full = pd.date_range(start=df.index.min(), end=df.index.max(), freq=freq)
        flags["gaps"] = int(len(full) - len(df.index.unique()))
    else:
        flags["gaps"] = None
    flags["zero_volume_rows"] = int((df.get("Volume", pd.Series([], dtype="Int64"))==0).sum())
    return flags

def upsert_daily_df(ticker: str, name: str, df: pd.DataFrame, scrape_id: str, source: str = "yahoo_finance") -> tuple:
    """
    Transforma DF diário em linhas com timestamp truncado para 00:00 UTC e faz upsert em lotes.
    Retorna (rows_processed, errors)
    """
    if df is None or df.empty:
        return 0, 0
    rows = []
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    for ts, row in df.iterrows():
        try:
            ts_day = truncate_to_day(ts)
            price = None if pd.isna(row.get("Close")) else float(row.get("Close"))
            volume = 0 if pd.isna(row.get("Volume")) else int(row.get("Volume"))
            change_24h = None  # calculado downstream
            q = {}  # por-row (vazio)
            rows.append((ticker, name, price, change_24h, volume, ts_day, source, scrape_id, True, json.dumps(q), now))
        except Exception as e:
            logger.exception("Row prepare error %s %s: %s", ticker, ts, e)
            continue

    if not rows:
        return 0, 0

    conn = POOL.get_connection()
    cur = conn.cursor()
    inserted = 0
    errors = 0
    try:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i+BATCH_SIZE]
            cur.executemany(UPSERT_SQL, batch)
            conn.commit()
            inserted += cur.rowcount
    except Error as e:
        logger.exception("DB upsert error: %s", e)
        conn.rollback()
        errors = 1
    finally:
        cur.close()
        conn.close()
    return inserted, errors

def run_backfill(tickers: list, days: int = 360, end_date: date = None):
    """
    Run-once backfill for tickers covering `days` up to `end_date` (inclusive).
    """
    if end_date is None:
        end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days)
    scrape_id = make_scrape_id()
    logger.info("Backfill id=%s tickers=%s start=%s end=%s (inclusive)", scrape_id, tickers, start_date, end_date)
    stats = {"success":0, "empty":0, "errors":0, "rows":0}
    for t in tickers:
        logger.info("Processing ticker %s", t)
        df = fetch_daily(t, start_date, end_date)
        qflags = compute_quality(df)
        if df.empty:
            logger.warning("Ticker %s: empty df flags=%s", t, qflags)
            stats["empty"] += 1
            continue
        inserted, errs = upsert_daily_df(t, t, df, scrape_id)
        stats["rows"] += inserted
        if errs == 0:
            stats["success"] += 1
            logger.info("Ticker %s upserted rows=%d flags=%s", t, inserted, qflags)
        else:
            stats["errors"] += 1
            logger.warning("Ticker %s upsert errors, flags=%s", t, qflags)
    logger.info("Backfill finished id=%s stats=%s", scrape_id, stats)
    return scrape_id, stats

# ------------------ CLI ------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run-once backfill daily data via yfinance")
    parser.add_argument("--tickers", required=True, help="Comma-separated tickers, ex: BTC-USD,ETH-USD")
    parser.add_argument("--days", type=int, default=360, help="Número de dias de backfill (default 360)")
    parser.add_argument("--end", type=str, default=None, help="Data final inclusive YYYY-MM-DD (default hoje UTC)")
    args = parser.parse_args()

    tickers = [s.strip() for s in args.tickers.split(",") if s.strip()]
    if args.end:
        try:
            end_dt = datetime.strptime(args.end, "%Y-%m-%d").date()
        except Exception as e:
            logger.error("Formato de --end inválido: %s", e)
            sys.exit(1)
    else:
        end_dt = None

    # run
    run_backfill(tickers, days=args.days, end_date=end_dt)
