"""
app.py — Dashboard Flask para el sistema de detección de discurso discriminatorio
Consume datos de Redis y los expone via API JSON + HTML
"""

import json
import os
import time
import random
import socket
import subprocess
import urllib.request
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template, request, Response
import redis

app = Flask(__name__)

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
# URLs de los gestores del cluster (en el master corren en localhost). Configurables.
FLINK_REST = os.environ.get("FLINK_REST", "http://localhost:8081")
SPARK_UI = os.environ.get("SPARK_UI", "http://localhost:8080")
# IP del master (la publica arrancar-cluster); por defecto se autodetecta.
MASTER_IP = os.environ.get("MASTER_IP", "")
# Carpeta donde los jobs batch de Spark escriben sus reportes JSON (data/reports/).
REPORTS_PATH = os.environ.get(
    "REPORTS_PATH",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "reports")),
)
# Script que ejecuta UNA pasada de los 5 jobs batch (para el botón "Generar ahora").
BATCH_SCRIPT = os.environ.get(
    "BATCH_SCRIPT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "aws", "lanzar-batch.sh")),
)


def _http_json(url, timeout=2.5):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        return None


def _local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80)); ip = s.getsockname()[0]; s.close()
        return ip
    except Exception:
        return "localhost"

def get_redis():
    return redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)


def safe_get(r, key):
    try:
        val = r.get(key)
        return json.loads(val) if val else None
    except Exception:
        return None


def safe_lrange(r, key, start=0, end=9):
    try:
        items = r.lrange(key, start, end)
        return [json.loads(i) for i in items if i]
    except Exception:
        return []


# ---- Fallback con datos demo cuando Redis no está disponible ----
def demo_metrics():
    t = time.time()
    return {
        "total_processed": int(t % 10000) + 5000,
        "hate_count": int(t % 1800) + 800,
        "terruco_count": int(t % 900) + 400,
        "throughput": round(random.uniform(2.5, 8.5), 2),
        "avg_latency_ms": round(random.uniform(80, 350), 1),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/metrics")
def api_metrics():
    try:
        r = get_redis()
        total = int(r.get("metrics:total_processed") or 0)
        hate = int(r.get("metrics:hate_count") or 0)
        terruco = int(r.get("metrics:terruco_count") or 0)
        discrimination = int(r.get("metrics:discrimination_count") or 0)
        latency_data = safe_get(r, "metrics:latency") or {}
        window_data = safe_get(r, "metrics:window:trends") or {}

        return jsonify({
            "total_processed": total,
            "hate_count": hate,
            "terruco_count": terruco,
            "discrimination_count": discrimination,
            "hate_rate": round(hate / max(total, 1) * 100, 2),
            "terruco_rate": round(terruco / max(total, 1) * 100, 2),
            "avg_latency_ms": latency_data.get("avg_latency_ms", 0),
            "p95_latency_ms": latency_data.get("p95_latency_ms", 0),
            "sla_violations": latency_data.get("sla_violations", 0),
            "window_trends": window_data,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
    except Exception:
        return jsonify(demo_metrics())


@app.route("/api/live-feed")
def api_live_feed():
    try:
        r = get_redis()
        items = safe_lrange(r, "hate:live", 0, 19)
        return jsonify({"feed": items, "count": len(items)})
    except Exception:
        demo_feed = [
            {"text": f"Post de prueba #{i} con contenido tóxico",
             "toxicity": round(random.uniform(0.4, 1.0), 2),
             "types": random.sample(["racial", "terruco", "genero"], k=random.randint(1,2)),
             "is_terruco": random.random() < 0.3,
             "processed_at": datetime.now(timezone.utc).isoformat()}
            for i in range(10)
        ]
        return jsonify({"feed": demo_feed, "count": 10})


@app.route("/api/topics")
def api_topics():
    try:
        r = get_redis()
        topic_names = ["politica", "economia", "seguridad", "corrupcion", "derechos_humanos",
                       "medioambiente", "educacion", "otros"]
        counts = {}
        for t in topic_names:
            counts[t] = int(r.get(f"topics:counts:{t}") or 0)
        return jsonify({"topics": counts})
    except Exception:
        return jsonify({"topics": {
            "economia": 1240, "seguridad": 980, "corrupcion": 1560,
            "derechos_humanos": 720, "medioambiente": 430,
            "educacion": 580, "otros": 890
        }})


@app.route("/api/timeline")
def api_timeline():
    """
    Serie temporal REAL por segundo y POR TIPO de detección (medida en el servidor por el
    Job Flink #1). Permite ver la evolución: si en los últimos seg/min se incrementó la
    detección de odio, terruqueo, discriminación o contenido político.

    Query: ?window=N segundos (default 60, máx 300).
    Devuelve: series[{t,total,hate,terruco,discrim,political}], current (msg/s) y
    'trend' (variación % de la 2ª mitad vs 1ª mitad de la ventana, por tipo).
    """
    keys = ["total", "hate", "terruco", "discrim", "political"]
    try:
        window = max(10, min(int(request.args.get("window", 60)), 300))
        r = get_redis()
        now = int(time.time())
        # Agregación adaptativa: ~40 puntos en la ventana. Cada punto es la TASA por
        # segundo (suma del bucket / tamaño del bucket), lo que suaviza el dentado que
        # produce el procesamiento por bundles de Flink y deja una línea legible.
        bsize = max(1, round(window / 40))
        start = now - (window // bsize) * bsize - 1     # alineado a buckets, omite seg en curso
        secs = list(range(start, now - 1))
        pipe = r.pipeline()
        for s in secs:
            pipe.hgetall(f"metrics:rate:{s}")
        raw = pipe.execute()

        nbuckets = len(secs) // bsize
        sums = [{k: 0 for k in keys} for _ in range(nbuckets)]
        for i, (s, h) in enumerate(zip(secs, raw)):
            bi = i // bsize
            if bi >= nbuckets:
                continue
            h = h or {}
            for k in keys:
                sums[bi][k] += int(h.get(k, 0))

        series = []
        for bi in range(nbuckets):
            t = start + (bi + 1) * bsize
            series.append({"t": t, **{k: round(sums[bi][k] / bsize, 1) for k in keys}})

        recent = [p["total"] for p in series[-3:]] or [0]
        current = round(sum(recent) / max(len(recent), 1))

        # tendencia: 2ª mitad vs 1ª mitad de la ventana (variación %)
        half = nbuckets // 2
        trend = {}
        for k in keys:
            first = sum(sums[bi][k] for bi in range(half)) or 0
            second = sum(sums[bi][k] for bi in range(half, nbuckets)) or 0
            trend[k] = (100.0 if second > 0 else 0.0) if first == 0 \
                else round((second - first) / first * 100, 1)
        return jsonify({"series": series, "current": current,
                        "trend": trend, "window": window, "bucket": bsize})
    except Exception:
        return jsonify({"series": [], "current": 0, "trend": {}, "window": 60, "bucket": 1})


@app.route("/api/sources")
def api_sources():
    """Volumen por procedencia REAL (source y dataset de origen) para el panel de fuentes."""
    try:
        r = get_redis()
        sources, origins = {}, {}
        for k in r.scan_iter("metrics:source:*", count=100):
            sources[k.split(":", 2)[2]] = int(r.get(k) or 0)
        for k in r.scan_iter("metrics:origin:*", count=100):
            origins[k.split(":", 2)[2]] = int(r.get(k) or 0)
        return jsonify({"sources": sources, "origins": origins})
    except Exception:
        return jsonify({
            "sources": {"twitter": 0, "reddit": 0, "news": 0},
            "origins": {}
        })


@app.route("/api/alerts")
def api_alerts():
    try:
        r = get_redis()
        bursts = safe_lrange(r, "alerts:bursts", 0, 9)
        sla = safe_lrange(r, "alerts:sla", 0, 9)
        return jsonify({"burst_alerts": bursts, "sla_alerts": sla})
    except Exception:
        return jsonify({"burst_alerts": [], "sla_alerts": []})


@app.route("/api/stream")
def api_stream():
    """Server-Sent Events para actualización en tiempo real del dashboard."""
    def event_generator():
        while True:
            try:
                r = get_redis()
                total = int(r.get("metrics:total_processed") or 0)
                hate = int(r.get("metrics:hate_count") or 0)
                data = json.dumps({"total": total, "hate": hate,
                                   "ts": datetime.now(timezone.utc).isoformat()})
            except Exception:
                data = json.dumps({"total": random.randint(4000, 9000),
                                   "hate": random.randint(1000, 3000),
                                   "ts": datetime.now(timezone.utc).isoformat()})
            yield f"data: {data}\n\n"
            time.sleep(3)

    return Response(event_generator(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _read_report(name):
    """Lee un reporte JSON de data/reports/. Devuelve dict o None."""
    try:
        with open(os.path.join(REPORTS_PATH, name), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


@app.route("/api/batch")
def api_batch():
    """
    Resultados de la capa BATCH (Spark). Lee los reportes JSON que generan los 5 jobs
    Spark (escritos en data/reports/) y el estado del planificador (_batch_status.json,
    que escribe aws/lanzar-batch.sh en cada corrida). Permite ver en el dashboard que la
    capa batch SE ESTA EJECUTANDO periodicamente y QUE produce, no solo la de streaming.
    """
    status = _read_report("_batch_status.json") or {}

    historical = _read_report("historical_hate_report.json")
    tfidf = _read_report("tfidf_keywords.json")
    graph = _read_report("cooccurrence_graph.json")
    sentiment = _read_report("sentiment_by_party.json")
    users = _read_report("user_profiles.json")

    # Resumen compacto por reporte (lo que el panel del dashboard necesita).
    reports = {
        "historical": None if not historical else {
            "data_source": historical.get("data_source"),
            "total_records": historical.get("total_records", 0),
            "hate_total": historical.get("hate_speech_total", 0),
            "terruco_total": historical.get("terruco_total", 0),
            "avg_toxicity": round(historical.get("avg_toxicity_global", 0) or 0, 3),
            "by_source": historical.get("by_source", []),
        },
        "tfidf": None if not tfidf else {
            "data_source": tfidf.get("data_source"),
            "hate_docs": tfidf.get("hate_docs", 0),
            "keywords": (tfidf.get("top_hate_keywords", []) or [])[:18],
        },
        "graph": None if not graph else {
            "data_source": graph.get("data_source"),
            "n_nodes": len(graph.get("nodes", []) or []),
            "n_edges": len(graph.get("edges", []) or []),
            "top_edges": sorted(
                (graph.get("edges", []) or []),
                key=lambda e: e.get("count", 0), reverse=True)[:8],
        },
        "sentiment": None if not sentiment else {
            "data_source": sentiment.get("data_source"),
            "total_mentions": sentiment.get("total_mentions", 0),
            "parties": sentiment.get("parties", []),
        },
        "users": None if not users else {
            "data_source": users.get("data_source"),
            "total_users": users.get("total_users_analyzed", 0),
            "top_risky": (users.get("high_risk_users", []) or [])[:10],
        },
    }
    ran = sum(1 for v in reports.values() if v is not None)
    return jsonify({
        "status": status,
        "reports": reports,
        "reports_ran": ran,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/api/batch/run", methods=["POST"])
def api_batch_run():
    """
    Dispara UNA pasada inmediata de los 5 jobs batch (boton 'Generar ahora' del dashboard).
    Lanza aws/lanzar-batch.sh en segundo plano (desacoplado) y vuelve al instante. Si ya hay
    una corrida en curso, no lanza otra (evita que se pisen los reportes).
    """
    status = _read_report("_batch_status.json") or {}
    if status.get("running"):
        return jsonify({"started": False, "reason": "running"})
    if not os.path.exists(BATCH_SCRIPT):
        return jsonify({"started": False, "reason": "script-not-found", "path": BATCH_SCRIPT})
    try:
        env = {**os.environ, "BATCH_INTERVAL_MIN": str(status.get("interval_min") or 10)}
        with open("/tmp/batch_manual.log", "w") as log:
            subprocess.Popen(
                ["bash", BATCH_SCRIPT],
                env=env, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        return jsonify({"started": True})
    except Exception as exc:
        return jsonify({"started": False, "reason": str(exc)})


@app.route("/api/cluster")
def api_cluster():
    """Estado del cluster DISTRIBUIDO en vivo: nodos (master + workers), su rol, los
    servicios que corren y su carga real (slots Flink ocupados, cores Spark). Lo arma
    consultando el JobManager de Flink (/taskmanagers) y el Spark Master (/json/)."""
    master_ip = MASTER_IP or _local_ip()
    nodes = {}

    def node(ip):
        if ip in ("localhost", "127.0.0.1", ""):
            ip = master_ip
        if ip not in nodes:
            is_master = (ip == master_ip)
            nodes[ip] = {
                "ip": ip,
                "role": "master" if is_master else "worker",
                "services": (["Kafka", "Redis", "Flink JobManager", "Spark Master",
                              "Dashboard", "Productor"] if is_master
                             else ["Flink TaskManager", "Spark Worker"]),
                "flink": None, "spark": None, "online": True,
            }
        return nodes[ip]

    ov = _http_json(f"{FLINK_REST}/overview") or {}
    tms = (_http_json(f"{FLINK_REST}/taskmanagers") or {}).get("taskmanagers", [])
    for tm in tms:
        host = (tm.get("id", "") or "").split(":")[0]
        slots = tm.get("slotsNumber", 0)
        free = tm.get("freeSlots", 0)
        node(host)["flink"] = {"slots": slots, "used": max(0, slots - free), "free": free}

    sp = _http_json(f"{SPARK_UI}/json/") or {}
    for w in sp.get("workers", []):
        if w.get("state") != "ALIVE":
            continue
        node(w.get("host") or "")["spark"] = {
            "cores": w.get("cores", 0), "used": w.get("coresused", 0)}

    node(master_ip)   # el master siempre aparece, aunque no compute

    node_list = sorted(nodes.values(), key=lambda x: (x["role"] != "master", x["ip"]))
    total_slots = sum((n["flink"]["slots"] if n["flink"] else 0) for n in node_list)
    used_slots = sum((n["flink"]["used"] if n["flink"] else 0) for n in node_list)
    return jsonify({
        "nodes": node_list,
        "summary": {
            "nodes": len(node_list),
            "taskmanagers": len(tms),
            "slots": total_slots,
            "slots_used": used_slots,
            "spark_workers": sum(1 for n in node_list if n["spark"]),
            "jobs_running": ov.get("jobs-running", 0),
            "flink_up": bool(tms or ov),
            "spark_up": bool(sp.get("workers")),
        },
        "master_ip": master_ip,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
