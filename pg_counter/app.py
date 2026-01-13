from flask import Flask
import os, threading
import psycopg
from psycopg_pool import ConnectionPool

try:
    import fcntl
except ImportError:
    fcntl = None

app = Flask(__name__)

STORAGE = os.getenv("STORAGE", "pg")   # pg
PG_DSN = os.getenv("PG_DSN")
pool = ConnectionPool(conninfo=PG_DSN, min_size=1, max_size=20) if PG_DSN else None

lock = threading.Lock()
counter = 0

@app.get("/inc")
def inc():
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1 RETURNING counter")
            val = cur.fetchone()[0]
            conn.commit()
    return str(val)

@app.get("/count")
def count():
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT counter FROM user_counter WHERE user_id = 1")
            val = cur.fetchone()[0]
            conn.commit()
    return str(val)

if __name__ == "__main__":
    from waitress import serve
    serve(app, host="127.0.0.1", port=8080, threads=20)
