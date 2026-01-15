from flask import Flask
import os, threading

try:
    import fcntl
except ImportError:
    fcntl = None

app = Flask(__name__)

STORAGE = os.getenv("STORAGE", "mem")   # mem | file
PATH = os.getenv("COUNTER_FILE", "counter.txt")

lock = threading.Lock()
counter = 0

def _init_file():
    if not os.path.exists(PATH):
        with open(PATH, "w") as f:
            f.write("0")

def _read_file():
    with open(PATH, "r") as f:
        if fcntl: fcntl.flock(f, fcntl.LOCK_SH)
        s = f.read().strip() or "0"
        if fcntl: fcntl.flock(f, fcntl.LOCK_UN)
    return int(s)

def _inc_file():
    _init_file()
    with open(PATH, "r+") as f:
        if fcntl: fcntl.flock(f, fcntl.LOCK_EX)
        v = int((f.read().strip() or "0")) + 1
        f.seek(0); f.truncate()
        f.write(str(v)); f.flush(); os.fsync(f.fileno())
        if fcntl: fcntl.flock(f, fcntl.LOCK_UN)
    return v

@app.get("/inc")
def inc():
    global counter
    with lock:
        if STORAGE == "file":
            return str(_inc_file())
        counter += 1
        return str(counter)

@app.get("/count")
def count():
    with lock:
        if STORAGE == "file":
            return str(_read_file())
        return str(counter)

if __name__ == "__main__":
    from waitress import serve
    serve(app, host="127.0.0.1", port=8080, threads=20)
