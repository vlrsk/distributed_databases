from flask import Flask, jsonify
import hazelcast

app = Flask(__name__)

client = hazelcast.HazelcastClient(
    cluster_name="dev",
    cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"],
)
counter = client.cp_subsystem.get_atomic_long("web-counter")


@app.post("/inc")
def like():
    v = counter.increment_and_get().result()
    return jsonify(value=v)


@app.get("/count")
def count():
    v = counter.get().result()
    return jsonify(value=v)


@app.post("/reset")
def reset():
    counter.set(0).result()
    return jsonify(ok=True)


if __name__ == "__main__":
    from waitress import serve
    serve(app, host="127.0.0.1", port=8080, threads=20)