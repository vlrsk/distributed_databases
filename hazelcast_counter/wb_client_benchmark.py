import time
import requests
from concurrent.futures import ThreadPoolExecutor

URL = "http://127.0.0.1:8080"

THREADS = 10
ITERS = 10_000


def worker():
    s = requests.Session()
    for _ in range(ITERS):
        s.post(URL + "/inc")


def main():
    requests.post(URL + "/reset")

    total = THREADS * ITERS
    t0 = time.perf_counter()

    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        list(ex.map(lambda _: worker(), range(THREADS)))

    dt = time.perf_counter() - t0
    final_val = requests.get(URL + "/count").json()["value"]

    print(f"threads={THREADS} iters={ITERS} total_ops={total}")
    print(f"time_sec={dt:.6f} rps={total/dt:.2f} final_counter={final_val}")


if __name__ == "__main__":
    main()