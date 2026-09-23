import json
import threading
from flask import Flask, render_template
from flask_socketio import SocketIO
from kafka import KafkaConsumer

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

KAFKA_BOOTSTRAP = "kafka:9092"

TOPIC_INPUT = "input"
TOPIC_OUTPUT = "output_json"

last_input_graph = None
last_output_graph = None


def input_to_graph(input_json):
    nodes = []
    edges = []
    seen_nodes = set()
    seen_edges = set()

    for device in input_json:
        node_id = device["container"]

        if node_id not in seen_nodes:
            seen_nodes.add(node_id)
            nodes.append({
                "data": {
                    "id": node_id,
                    "label": node_id,
                    "vendor": device.get("vendor", "unknown")
                }
            })

        for itf in device.get("interfaces", []):
            remote = itf.get("adjacent-node")
            remote_if = itf.get("adjacent-interface")
            local_if = itf.get("name")
            status = itf.get("status", "unknown")

            if not remote or not local_if:
                continue

            if remote not in seen_nodes:
                seen_nodes.add(remote)
                nodes.append({
                    "data": {
                        "id": remote,
                        "label": remote,
                        "vendor": "unknown"
                    }
                })

            edge_key = tuple(sorted([node_id, remote]))

            if edge_key in seen_edges:
                continue

            seen_edges.add(edge_key)

            edge_id = f"{edge_key[0]}-{edge_key[1]}"

            edges.append({
                "data": {
                    "id": edge_id,
                    "source": node_id,
                    "target": remote,
                    "label": f"{local_if} - {remote_if}",
                    "status": status
                }
            })

    return {"nodes": nodes, "edges": edges}


def yang_to_graph(yang_json):
    network = yang_json["ietf-network:networks"]["network"][0]

    nodes = []
    edges = []
    seen_nodes = set()

    for node in network.get("node", []):
        node_id = node["node-id"]
        seen_nodes.add(node_id)
        nodes.append({
            "data": {
                "id": node_id,
                "label": node_id
            }
        })

    for link in network.get("ietf-network-topology:link", []):
        src = link["source"]["source-node"]
        dst = link["destination"]["dest-node"]

        if src not in seen_nodes:
            seen_nodes.add(src)
            nodes.append({"data": {"id": src, "label": src}})

        if dst not in seen_nodes:
            seen_nodes.add(dst)
            nodes.append({"data": {"id": dst, "label": dst}})

        edges.append({
            "data": {
                "id": link["link-id"],
                "source": src,
                "target": dst,
                "label": f'{link["source"]["source-tp"]} - {link["destination"]["dest-tp"]}',
                "status": "unknown"
            }
        })

    return {"nodes": nodes, "edges": edges}


def kafka_listener(topic, graph_type):
    global last_input_graph, last_output_graph

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    print(f"[Kafka] Escuchando topic {topic}")

    for msg in consumer:
        try:
            if graph_type == "input":
                graph = input_to_graph(msg.value)
                last_input_graph = graph
                socketio.emit("topology_update_input", graph)

            elif graph_type == "output":
                graph = yang_to_graph(msg.value)
                last_output_graph = graph
                socketio.emit("topology_update_output", graph)

            print(f"[WebSocket] Topología enviada al navegador desde {topic}")

        except Exception as e:
            print(f"[ERROR] No se pudo procesar mensaje Kafka desde {topic}: {e}")


@app.route("/")
def index():
    return render_template("index.html")


@socketio.on("connect")
def handle_connect():
    print("[WebSocket] Navegador conectado")

    if last_input_graph is not None:
        socketio.emit("topology_update_input", last_input_graph)

    if last_output_graph is not None:
        socketio.emit("topology_update_output", last_output_graph)


if __name__ == "__main__":
    thread_input = threading.Thread(
        target=kafka_listener,
        args=(TOPIC_INPUT, "input"),
        daemon=True
    )

    thread_output = threading.Thread(
        target=kafka_listener,
        args=(TOPIC_OUTPUT, "output"),
        daemon=True
    )

    thread_input.start()
    thread_output.start()

    socketio.run(app, host="0.0.0.0", port=5000, debug=True, allow_unsafe_werkzeug=True)