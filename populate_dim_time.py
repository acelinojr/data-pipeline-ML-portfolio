# populate_dim_time.py
from datetime import datetime, timedelta
import mysql.connector

cfg = dict(host="localhost", user="Acelino", password="senha123", database="projeto_crypto", port=3306)
conn = mysql.connector.connect(**cfg)
cur = conn.cursor()

start = datetime.utcnow() - timedelta(days=365)  # 1 ano atrás
end = datetime.utcnow() + timedelta(days=180)    # +6 meses
ts = start
rows = []
while ts <= end:
    hour_id = int(ts.strftime("%Y%m%d%H"))
    rows.append((
        hour_id, ts.date(), ts.year, (ts.month-1)//3+1, ts.month, ts.day,
        ts.isoweekday(), ts.hour, ts.isoweekday() in (6,7), ts.day == (ts.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1) and ts.day == ts.day, False
    ))
    ts += timedelta(hours=1)

sql = """
INSERT IGNORE INTO dim_time_hourly
(hour_id, full_date, year, quarter, month, day, day_of_week, hour, is_weekend, is_month_end, is_quarter_end)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
cur.executemany(sql, rows)
conn.commit()
cur.close()
conn.close()
print("dim_time_hourly populated:", len(rows))