import argparse
import time
import threading
from concurrent.futures import ThreadPoolExecutor

import hazelcast


def bench(mode: str, threads: int, iters: int,
          cluster_name: str, members: list[str],
          redo: bool, progress_every: int,
          connect_timeout: float) -> None:

    expected = threads * iters
    print(f"[start] mode={mode} threads={threads} iters={iters} expected={expected}", flush=True)
    print(f"[start] connecting cluster_name={cluster_name} members={members} timeout={connect_timeout}s", flush=True)

    client = hazelcast.HazelcastClient(
        cluster_name=cluster_name,
        cluster_members=members,
        redo_operation=redo,
        cluster_connect_timeout=connect_timeout,
    )

    print("[ok] connected", flush=True)

    done = 0
    done_lock = threading.Lock()

    def tick():
        nonlocal done
        if progress_every <= 0:
            return
        to_print = None
        with done_lock:
            done += 1
            if done % progress_every == 0 or done == expected:
                to_print = done
        if to_print is not None:
            print(f"[progress] done={to_print}/{expected}", flush=True)

    key = "k"

    try:
        if mode.startswith("map"):
            m = client.get_map("counter-map").blocking()
            m.put(key, 0)

            def inc():
                for _ in range(iters):
                    if mode == "map-nolock":
                        v = m.get(key)
                        m.put(key, (v or 0) + 1)

                    elif mode == "map-pess":
                        m.lock(key)
                        try:
                            v = m.get(key)
                            m.put(key, (v or 0) + 1)
                        finally:
                            m.unlock(key)

                    elif mode == "map-opt":
                        while True:
                            old = m.get(key) or 0
                            if m.replace_if_same(key, old, old + 1):
                                break
                    else:
                        raise ValueError("Unknown mode")

                    tick()

            t0 = time.perf_counter()
            with ThreadPoolExecutor(max_workers=threads) as ex:
                list(ex.map(lambda _: inc(), range(threads)))
            dt = time.perf_counter() - t0

            final_val = m.get(key)
            print(f"[done] time_sec={dt:.6f} rps={expected/dt:.2f} final_counter={final_val}", flush=True)

        elif mode == "atomic":
            a = client.cp_subsystem.get_atomic_long("counter")
            a.set(0).result()

            def inc():
                for _ in range(iters):
                    a.increment_and_get().result()
                    tick()

            t0 = time.perf_counter()
            with ThreadPoolExecutor(max_workers=threads) as ex:
                list(ex.map(lambda _: inc(), range(threads)))
            dt = time.perf_counter() - t0

            final_val = a.get().result()
            print(f"[done] time_sec={dt:.6f} rps={expected/dt:.2f} final_counter={final_val}", flush=True)

        else:
            raise ValueError("Unknown mode")

    finally:
        client.shutdown()
        print("[end] client shutdown", flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", required=True, choices=["map-nolock", "map-pess", "map-opt", "atomic"])
    p.add_argument("--threads", type=int, default=10)
    p.add_argument("--iters", type=int, default=10_000)
    p.add_argument("--progress-every", type=int, default=5000)
    p.add_argument("--cluster-name", default="dev")
    p.add_argument("--members", default="127.0.0.1:5701,127.0.0.1:5702,127.0.0.1:5703")
    p.add_argument("--redo", action="store_true")
    p.add_argument("--connect-timeout", type=float, default=15.0)
    args = p.parse_args()

    bench(
        mode=args.mode,
        threads=args.threads,
        iters=args.iters,
        cluster_name=args.cluster_name,
        members=[m.strip() for m in args.members.split(",")],
        redo=args.redo,
        progress_every=args.progress_every,
        connect_timeout=args.connect_timeout,
    )


if __name__ == "__main__":
    main()