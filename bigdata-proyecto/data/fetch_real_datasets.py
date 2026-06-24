"""
fetch_real_datasets.py
======================
Descarga y unifica DATOS REALES de MÚLTIPLES ORÍGENES en un solo corpus JSONL
(`data/corpus_real.jsonl`) que luego replica el productor Kafka a alto volumen.

Orígenes (todos datasets públicos reales, en español / contexto peruano):
  1. pyupeu/social-media-peruvian-sentiment   ~9.3k posts reales de redes peruanas
  2. paolorivas/noticias_peru                 ~2.0k noticias peruanas reales
  3. Paul/hatecheck-spanish                    ~3.7k casos reales de discurso de odio (ES)
  4. afcarvallo/spanish-ner-hate-bio-filtered  ~2.1k textos de odio en español
  (Se descartó hs-knowledge/hateval_enriched: su split disponible está en inglés.)

Se descargan los archivos PARQUET directamente desde el CDN de Hugging Face (una
descarga por split, sin la API rate-limited por fila). No requiere la librería
`datasets`, solo `pandas` + `pyarrow` + `requests`.

Cada registro se normaliza al esquema del proyecto:
  {id, text, source, author, subreddit?, origin, label}
- `source` (twitter/reddit/news) determina el topic Kafka destino en el productor.
- `origin` conserva la procedencia real (nombre del dataset) para trazabilidad.

Uso:
  python fetch_real_datasets.py                 # corpus completo -> data/corpus_real.jsonl
  python fetch_real_datasets.py --max 3000      # tope por dataset (pruebas rápidas)
  python fetch_real_datasets.py --out otro.jsonl
"""

import argparse
import hashlib
import json
import os
import re
import sys

HF = "https://huggingface.co/datasets/{ds}/resolve/refs%2Fconvert%2Fparquet/{cfg}/{split}/0000.parquet"

# (dataset, config, split, columna_texto, source_kafka, columna_label_opcional)
SOURCES = [
    ("pyupeu/social-media-peruvian-sentiment", "default", "train", "text", "twitter", "label_name"),
    ("pyupeu/social-media-peruvian-sentiment", "default", "validation", "text", "twitter", "label_name"),
    ("paolorivas/noticias_peru", "default", "train", "des_titular", "news", None),
    ("paolorivas/noticias_peru", "default", "train", "des_resumen", "news", None),
    ("Paul/hatecheck-spanish", "default", "test", "test_case", "twitter", "label_gold"),
    ("afcarvallo/spanish-ner-hate-bio-filtered", "default", "train", "text", "reddit", "label"),
    ("afcarvallo/spanish-ner-hate-bio-filtered", "default", "validation", "text", "reddit", "label"),
]

SUBREDDITS = ["peru", "PeruPolitica", "LatinAmerica"]

_SP = [" que ", " los ", " las ", " una ", " por ", " con ", " del ", " para ", " más ",
       " qué ", " año", " el ", " la ", " de ", " no ", " es ", "ñ", "á", "é", "í", "ó", "ú"]
_EN = [" the ", " and ", " you ", " for ", " that ", " with ", " this ", " are ",
       " have ", " not ", " your ", " they ", " what "]


def is_spanish(t: str) -> bool:
    """Heurística simple para quedarnos solo con texto en español."""
    s = f" {t.lower()} "
    sp = sum(m in s for m in _SP)
    en = sum(m in s for m in _EN)
    return sp >= 2 and sp >= en


def clean(t: str) -> str:
    t = re.sub(r"\s+", " ", str(t)).strip()
    return t


REDDIT_SUBS = ["peru", "PeruPolitica", "LatinAmerica"]
REDDIT_TERMS = ["elecciones", "presidente", "corrupcion", "congreso", "gobierno",
                "terruco", "Castillo", "Fujimori", "voto", "dictadura"]


def fetch_reddit_bulk(size=100, seen=None):
    """
    Datos REALES en vivo de Reddit (r/peru, r/PeruPolitica) vía API pública Pullpush
    (reemplazo de Pushshift, sin credenciales). Es la fuente más on-topic del proyecto:
    discurso político peruano real. Best-effort: si falla la red, devuelve [].
    """
    import requests
    seen = seen if seen is not None else set()
    S = requests.Session(); S.headers["User-Agent"] = "bigdata-edu/1.0"
    out = []
    for sub in REDDIT_SUBS:
        for q in REDDIT_TERMS:
            try:
                r = S.get("https://api.pullpush.io/reddit/comment/search",
                          params={"subreddit": sub, "q": q, "size": size}, timeout=20)
                if r.status_code != 200:
                    continue
                for c in r.json().get("data", []):
                    text = clean(c.get("body", ""))
                    if len(text) < 15 or text == "[deleted]" or not is_spanish(text):
                        continue
                    h = hashlib.md5(text.lower().encode("utf-8")).hexdigest()
                    if h in seen:
                        continue
                    seen.add(h)
                    out.append({
                        "id": f"reddit_{c.get('id', h[:10])}",
                        "text": text, "source": "reddit",
                        "author": c.get("author", "unknown"),
                        "origin": "reddit/pullpush", "label": "",
                        "subreddit": sub,
                    })
            except Exception as e:
                print(f"[reddit] {sub}/{q}: {repr(e)[:80]}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=0, help="tope de filas por dataset (0 = todas)")
    ap.add_argument("--no-reddit", action="store_true", help="no consultar Reddit en vivo")
    ap.add_argument("--reddit-size", type=int, default=100, help="comentarios por consulta Reddit")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "corpus_real.jsonl"))
    args = ap.parse_args()

    try:
        import pandas as pd
    except ImportError:
        sys.exit("Falta pandas/pyarrow: pip install pandas pyarrow")

    out_path = os.path.abspath(args.out)
    seen = set()
    total = 0
    per_origin = {}

    with open(out_path, "w", encoding="utf-8") as out:
        for ds, cfg, split, tcol, source, lcol in SOURCES:
            url = HF.format(ds=ds, cfg=cfg, split=split)
            short = ds.split("/")[-1]
            try:
                df = pd.read_parquet(url)
            except Exception as e:
                print(f"[SKIP] {ds}/{split}: {repr(e)[:120]}")
                continue
            if tcol not in df.columns:
                print(f"[SKIP] {ds}/{split}: sin columna '{tcol}' (cols={list(df.columns)[:6]}...)")
                continue

            kept = 0
            rows = df.to_dict("records")
            for i, row in enumerate(rows):
                if args.max and kept >= args.max:
                    break
                text = clean(row.get(tcol, ""))
                if len(text) < 12:
                    continue
                if len(text) > 600:                # noticias largas: truncar, no descartar
                    text = text[:597].rsplit(" ", 1)[0] + "…"
                if not is_spanish(text):
                    continue
                h = hashlib.md5(text.lower().encode("utf-8")).hexdigest()
                if h in seen:
                    continue
                seen.add(h)

                rec = {
                    "id": f"{short}_{h[:12]}",
                    "text": text,
                    "source": source,
                    "author": f"user_{int(h[:6], 16) % 900000 + 1000}",
                    "origin": ds,
                    "label": str(row.get(lcol, "")) if lcol else "",
                }
                if source == "reddit":
                    rec["subreddit"] = SUBREDDITS[i % len(SUBREDDITS)]
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                kept += 1
                total += 1
            per_origin[ds] = per_origin.get(ds, 0) + kept
            print(f"[OK] {ds}/{split}: +{kept} (de {len(rows)})")

        # --- Reddit Perú en vivo (Pullpush): el origen más on-topic ---
        if not args.no_reddit:
            print("[reddit] consultando Pullpush (r/peru, r/PeruPolitica)…")
            rd = fetch_reddit_bulk(size=args.reddit_size, seen=seen)
            for rec in rd:
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            per_origin["reddit/pullpush"] = len(rd)
            total += len(rd)
            print(f"[OK] reddit/pullpush: +{len(rd)}")

    print("\n===== RESUMEN CORPUS REAL =====")
    for o, n in per_origin.items():
        print(f"  {o:48s} {n:6d}")
    print(f"  {'TOTAL único':48s} {total:6d}")
    print(f"  -> {out_path}")


if __name__ == "__main__":
    main()
