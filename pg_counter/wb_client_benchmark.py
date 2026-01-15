import time, sys
from concurrent.futures import ThreadPoolExecutor
import requests
from requests.adapters import HTTPAdapter

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8080"
CLIENTS = int(sys.argv[2]) if len(sys.argv) > 2 else 10
N = int(sys.argv[3]) if len(sys.argv) > 3 else 10_000

def make_session():
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=CLIENTS, pool_maxsize=CLIENTS, max_retries=0)
    s.mount("http://", adapter)
    s.headers.update({"Connection": "keep-alive"})
    return s

def worker(n: int):
    s = make_session()
    for _ in range(n):
        r = s.get(f"{BASE}/inc", timeout=10)
        r.raise_for_status()

def get_count():
    return int(requests.get(f"{BASE}/count", timeout=10).text.strip())

def main():
    before = get_count()
    t0 = time.perf_counter()

    with ThreadPoolExecutor(max_workers=CLIENTS) as ex:
        futures = [ex.submit(worker, N) for _ in range(CLIENTS)]
        for f in futures:
            f.result()

    dt = time.perf_counter() - t0
    after = get_count()

    total = CLIENTS * N
    expected = before + total
    print(f"clients={CLIENTS} calls_per_client={N} total_calls={total}")
    print(f"time_sec={dt:.6f} rps={total/dt:.2f}")
    print(f"count_before={before} count_after={after} expected={expected} ok={after==expected}")

if __name__ == "__main__":
    main()
