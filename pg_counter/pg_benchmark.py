import os, time, threading
import psycopg
import sys

THREADS = int(os.getenv("THREADS", "10"))
ITERS = int(os.getenv("ITERS", "10000"))
DSN = os.getenv("PG_DSN", "postgresql://app:app@127.0.0.1:5432/counterdb")

USER_ID = 1

def reset_row():
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE user_counter SET counter=0, version=0 WHERE user_id=%s", (USER_ID,))
            conn.commit()

def read_counter():
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT counter, version FROM user_counter WHERE user_id=%s", (USER_ID,))
            return cur.fetchone()

def worker(mode: str, barrier: threading.Barrier):
    conn = psycopg.connect(DSN)
    cur = conn.cursor()
    barrier.wait()

    if mode == "lost":
        for _ in range(ITERS):
            cur.execute("BEGIN")
            cur.execute("SELECT counter FROM user_counter WHERE user_id=%s", (USER_ID,))
            c = cur.fetchone()[0] + 1
            cur.execute("UPDATE user_counter SET counter=%s WHERE user_id=%s", (c, USER_ID))
            conn.commit()

    elif mode == "serializable":
        for _ in range(ITERS):
            while True:
                try:
                    cur.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
                    cur.execute("SELECT counter FROM user_counter WHERE user_id=%s", (USER_ID,))
                    c = cur.fetchone()[0] + 1
                    cur.execute("UPDATE user_counter SET counter=%s WHERE user_id=%s", (c, USER_ID))
                    conn.commit()
                    break
                except psycopg.Error as e:
                    conn.rollback()
                    if getattr(e, "sqlstate", None) == "40001":
                        continue
                    raise

    elif mode == "inplace":
        for _ in range(ITERS):
            cur.execute("BEGIN")
            cur.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id=%s", (USER_ID,))
            conn.commit()

    elif mode == "for_update":
        for _ in range(ITERS):
            cur.execute("BEGIN")
            cur.execute("SELECT counter FROM user_counter WHERE user_id=%s FOR UPDATE", (USER_ID,))
            c = cur.fetchone()[0] + 1
            cur.execute("UPDATE user_counter SET counter=%s WHERE user_id=%s", (c, USER_ID))
            conn.commit()

    elif mode == "optimistic":
        for _ in range(ITERS):
            while True:
                cur.execute("BEGIN")
                cur.execute("SELECT counter, version FROM user_counter WHERE user_id=%s", (USER_ID,))
                c, v = cur.fetchone()
                c2, v2 = c + 1, v + 1
                cur.execute(
                    "UPDATE user_counter SET counter=%s, version=%s WHERE user_id=%s AND version=%s",
                    (c2, v2, USER_ID, v),
                )
                if cur.rowcount == 1:
                    conn.commit()
                    break
                conn.rollback()

    else:
        raise ValueError("mode must be one of: lost, serializable, inplace, for_update, optimistic")

    cur.close()
    conn.close()

def run(mode: str):
    reset_row()
    barrier = threading.Barrier(THREADS)
    threads = [threading.Thread(target=worker, args=(mode, barrier), daemon=True) for _ in range(THREADS)]

    t0 = time.perf_counter()
    for t in threads: t.start()
    for t in threads: t.join()
    dt = time.perf_counter() - t0

    c, v = read_counter()
    total = THREADS * ITERS
    ok = (c == total) if mode != "lost" else (c != total)
    rps = total / dt if dt > 0 else float("inf")

    print(f"mode={mode} threads={THREADS} iters={ITERS} total_ops={total}")
    print(f"time_sec={dt:.6f} rps={rps:.2f} final_counter={c} final_version={v} ok={ok}")
    print("-" * 60)


if __name__ == "__main__":
    modes = ["lost", "serializable", "inplace", "for_update", "optimistic"]
    if len(sys.argv) > 1:
        run(sys.argv[1])
    else:
        for m in modes:
            run(m)