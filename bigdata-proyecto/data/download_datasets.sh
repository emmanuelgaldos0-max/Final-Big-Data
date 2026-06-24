#!/bin/bash
# =============================================================================
# download_datasets.sh — Descarga los datasets reales para el proyecto
# =============================================================================

DATA_DIR="$(dirname "$0")"
echo "=== Descarga de Datasets para Detección de Discurso Discriminatorio ==="

# -----------------------------------------------------------------------
# DATASET 1: HatEval SemEval-2019 (tweets en español con hate speech)
# URL: https://github.com/cicl2018/hateeval
# -----------------------------------------------------------------------
echo ""
echo "[1/3] HatEval SemEval-2019 — Tweets en español etiquetados"
echo "  → Clonar repositorio HatEval:"
echo "    git clone https://github.com/cicl2018/hateeval.git ${DATA_DIR}/hateeval"
echo "  → Los archivos están en: hateeval/data/ES/"
echo "  → Formato: TSV con columnas: id | tweet | HS | TR | AG"
echo "     HS=hate speech, TR=target range, AG=aggressiveness"
echo ""
echo "  Conversión a JSONL:"
cat << 'PYEOF'
import csv, json, sys

with open('hateeval/data/ES/train_es.tsv') as f, \
     open('sample_data.jsonl', 'a') as out:
    reader = csv.DictReader(f, delimiter='\t',
                             fieldnames=['id','text','HS','TR','AG'])
    for row in reader:
        obj = {
            "id": row['id'],
            "text": row['text'],
            "source": "twitter",
            "is_hate_speech": row['HS'] == '1',
            "is_aggressive": row['AG'] == '1',
            "author": f"user_{row['id']}",
        }
        out.write(json.dumps(obj, ensure_ascii=False) + '\n')
print("Conversión completada.")
PYEOF

# -----------------------------------------------------------------------
# DATASET 2: Reddit Peru via Pushshift / Pullpush
# URL: https://pullpush.io/
# -----------------------------------------------------------------------
echo ""
echo "[2/3] Reddit Peru — via API Pullpush (no requiere autenticación)"
echo "  → Ejecutar script de descarga:"
cat << 'PYEOF'
import requests, json, time

SUBREDDITS = ["peru", "PeruPolitica"]
QUERIES = ["elecciones", "presidente", "corrupto", "terruco", "gobierno"]
OUTPUT = "reddit_comments.jsonl"

with open(OUTPUT, 'w') as f:
    for sub in SUBREDDITS:
        for q in QUERIES:
            url = f"https://api.pullpush.io/reddit/comment/search"
            params = {"subreddit": sub, "q": q, "size": 500}
            try:
                r = requests.get(url, params=params, timeout=30)
                if r.status_code == 200:
                    for comment in r.json().get("data", []):
                        obj = {
                            "id": comment["id"],
                            "text": comment.get("body", ""),
                            "source": "reddit",
                            "author": comment.get("author", ""),
                            "subreddit": sub,
                            "score": comment.get("score", 0),
                        }
                        if obj["text"] and obj["text"] != "[deleted]":
                            f.write(json.dumps(obj, ensure_ascii=False) + '\n')
                print(f"  r/{sub} + '{q}': OK")
                time.sleep(1)  # Rate limit
            except Exception as e:
                print(f"  ERROR: {e}")

print(f"Descarga completada → {OUTPUT}")
PYEOF

# -----------------------------------------------------------------------
# DATASET 3: Zenodo — Comentarios políticos latinoamérica
# URL: https://zenodo.org/records/6524613
# -----------------------------------------------------------------------
echo ""
echo "[3/3] Zenodo — Comentarios políticos latinoamérica (Twitter)"
echo "  → URL directa: https://zenodo.org/records/6524613/files/dataset.zip"
echo "  → Descargar manualmente y descomprimir en data/"
echo "    wget 'https://zenodo.org/records/6524613/files/dataset.zip'"
echo "    unzip dataset.zip -d ${DATA_DIR}/zenodo/"
echo ""
echo "=== Instrucciones adicionales ==="
echo "Una vez descargados, convertir todos los archivos a JSONL con:"
echo "  python3 convert_datasets.py"
echo ""
echo "Los archivos finales deben estar en: data/sample_data.jsonl"
echo "El productor Kafka los leerá automáticamente."
