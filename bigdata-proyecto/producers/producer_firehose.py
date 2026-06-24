"""
producer_firehose.py
====================
Productor Kafka de ALTO VOLUMEN (firehose) — fuente principal de datos del proyecto.

Genera de forma combinatoria un universo prácticamente ilimitado de publicaciones
realistas sobre el contexto electoral peruano (discurso de odio, terruqueo, racismo,
clasismo, misoginia, polarización política y discurso cívico neutro), cada una con un
`post_id` ÚNICO para que el pipeline NO la deduplique.

Por qué existe (problema que resuelve):
  El productor de dataset cicla un archivo de ~29 líneas con IDs fijos; el Job Flink #1
  deduplica por post_id → el corpus se queda en ~29 únicos y el throughput real es ~5 msg/s.
  Este firehose produce miles de mensajes únicos por minuto, con balance de clases realista
  y cobertura de TODAS las técnicas NLP y de TODOS los temas que clasifica el Job #5.

Topics de destino: raw-tweets, raw-comments (el campo `source` distingue el origen).

Configuración por variables de entorno:
  KAFKA_BOOTSTRAP   brokers Kafka                         (default: localhost:9092)
  FIREHOSE_RATE     mensajes/segundo objetivo; 0 = máximo (default: 200)
  FIREHOSE_HATE     proporción [0..1] de mensajes tóxicos (default: 0.45)
  FIREHOSE_SEED     semilla para reproducibilidad         (default: aleatoria)

Modos de uso:
  python producer_firehose.py                 # firehose continuo a FIREHOSE_RATE msg/s
  python producer_firehose.py --rate 500      # 500 msg/s
  python producer_firehose.py --burst 50000   # envía 50k mensajes y termina (stress test)
  python producer_firehose.py --gen-corpus 20000 [archivo.jsonl]  # solo genera dataset
"""

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

# =============================================================================
# CORPUS COMBINATORIO
# -----------------------------------------------------------------------------
# Cada plantilla se arma con piezas intercambiables. La combinatoria de sujetos ×
# predicados × temas × cierres genera millones de variantes únicas y gramaticalmente
# coherentes. Las piezas incluyen, a propósito, el léxico que detectan el NLP y el
# clasificador de temas (Job #5) para que el dashboard muestre variedad real.
# =============================================================================

# --- Actores políticos / grupos (sujetos) -----------------------------------
SUJETOS_POLITICOS = [
    "el gobierno", "el congreso", "ese candidato", "esa candidata", "el presidente",
    "la oposición", "ese partido", "la izquierda", "la derecha", "el fujimorismo",
    "esa bancada", "el premier", "la fiscalía", "ese ministro", "esa congresista",
    "el alcalde", "la gestión actual", "esos políticos", "la clase política",
]

# --- Temas (cada frase contiene keywords del Job #5 TOPIC_KEYWORDS) ----------
# economia / seguridad / corrupcion / derechos_humanos / medioambiente / educacion
TEMAS = {
    "economia": [
        "subió otra vez la inflación y los precios",
        "no hace nada por el empleo ni los sueldos",
        "habla de inversión pero solo trae más impuestos",
        "con esta economía la pobreza no baja",
        "el dólar disparado y nadie responde por el trabajo de la gente",
    ],
    "seguridad": [
        "la delincuencia y la extorsión están fuera de control",
        "el narcotráfico avanza y la policía no llega",
        "otro homicidio más, pura inseguridad en las calles",
        "los sicarios y las pandillas hacen lo que quieren",
        "promete combatir el crimen pero el robo aumenta cada día",
    ],
    "corrupcion": [
        "otro caso de corrupción y coimas destapado",
        "está investigado por lavado de activos y soborno",
        "la fiscalía debería mandarlo a prisión por corrupto",
        "ese candidato huele a coima por todos lados",
        "puro soborno, así se reparten el país",
    ],
    "derechos_humanos": [
        "reprime la protesta y pisotea la democracia",
        "esto es una dictadura disfrazada, pura represión",
        "detención arbitraria en plena manifestación pacífica",
        "habla de libertad mientras ordena tortura y represión",
        "los derechos y la democracia les estorban",
    ],
    "medioambiente": [
        "la minería ilegal contamina los ríos de la amazonía",
        "deforestación y petróleo derramado en territorio indígena",
        "el cambio de clima y la contaminación no les importa",
        "venden la amazonía a la minería sin consultar a los indígenas",
        "más deforestación y cero política ambiental",
    ],
    "educacion": [
        "la educación pública y las escuelas siguen abandonadas",
        "ni becas ni presupuesto para universidades ni maestros",
        "los estudiantes y los maestros exigen mejor educación",
        "promete reforma del currículum y no cumple",
        "las universidades en crisis y nadie hace nada",
    ],
}

# --- Conectores de odio / terruqueo (insultos políticos) ---------------------
# Disparan detect_hate_speech y detect_terruco del nlp_pipeline.
INSULTOS_TERRUQUEO = [
    "ese terruco comunista", "puro caviar senderista", "esos chavistas",
    "el marxista de turno", "ese narcoterrorista disfrazado", "los terrucos del gobierno",
    "ese castrista", "los caviares de siempre", "esa comunista",
]
INSULTOS_DISCRIMINACION = [  # racial / clase / regional → discrimination_types
    "ese cholo igualado", "el serrano ese", "ese indio resentido", "pura chusma",
    "ese huachafo", "ese provinciano", "el selvático ese", "ese pituco de mierda",
    "ese zambo", "ese asiático de mierda",
]
INSULTOS_GENERO = [  # misoginia → discrimination_types: genero
    "esa feminazi", "la hembrista esa", "esa bruja", "esa feminazi resentida",
]

# --- Acciones negativas (predicados de odio) ---------------------------------
ACCIONES_ODIO = [
    "quiere expropiar todo y arruinar el país",
    "nos va a llevar a la ruina como en Venezuela",
    "no sabe ni gobernar, debería irse",
    "solo busca robar y repartirse el poder",
    "quiere destruir la familia y las tradiciones",
    "no merece ni un voto, es una vergüenza",
    "está vendiendo el país al mejor postor",
]

# --- Discurso cívico NEUTRO / POSITIVO (no tóxico) ---------------------------
CIVICO = [
    "Apoyamos la democracia y el estado de derecho en el Perú.",
    "Respetemos el resultado de las elecciones, es la voluntad del pueblo.",
    "Necesitamos unidad y diálogo, no más polarización política.",
    "La inversión privada y la educación pública pueden ir de la mano.",
    "Trabajemos juntos por un Perú más justo e igualitario para todos.",
    "La corrupción daña a todos los partidos por igual; exijamos transparencia.",
    "Más presupuesto para escuelas, universidades y maestros: esa es la prioridad.",
    "Hay que combatir el crimen y la extorsión con inteligencia, no con discursos.",
    "Cuidemos la amazonía: frenar la minería ilegal protege a las comunidades indígenas.",
    "Una economía sana baja la pobreza y crea empleo digno para la gente.",
    "Gracias a quienes salieron a votar de forma pacífica e informada.",
    "El debate de ideas fortalece la democracia; respetemos al que piensa distinto.",
]

# --- Polarización política PURA (izquierda/derecha, sin odio) ----------------
POLARIZACION = [
    "El libre mercado y la inversión privada son la única salida para el desarrollo.",
    "Solo el socialismo y la organización del obrero garantizan justicia social.",
    "Hay que privatizar y dar orden al país, basta de estatismo.",
    "Defendamos los sindicatos y el derecho a huelga de los trabajadores.",
    "Fuerza Popular y la derecha representan el orden que el Perú necesita.",
    "Perú Libre y la izquierda defienden al pueblo frente a las élites.",
    "Menos Estado y más empresa: así crece la economía.",
    "Nacionalizar los recursos es defender la soberanía del país.",
]

# --- Plantillas de armado ----------------------------------------------------
APERTURAS = ["", "", "Increíble que ", "Otra vez ", "Como siempre ", "La verdad ",
             "No puede ser que ", "Indignante: ", "Cada día peor: "]
CIERRES = ["", "", "", " #Elecciones2026", " #Perú", " #SegundaVuelta",
           " que verguenza", " hasta cuando", " basta ya", " el pueblo no olvida"]

FUENTES = ["twitter", "twitter", "twitter", "reddit", "reddit", "mastodon", "facebook"]
SUBREDDITS = ["peru", "PeruPolitica", "LatinAmerica", "vzla", ""]


def _cap(s: str) -> str:
    return s[0].upper() + s[1:] if s else s


def _toxic_text() -> str:
    """Genera un texto TÓXICO (odio/terruqueo/discriminación/misoginia) + tema."""
    tema_key = random.choice(list(TEMAS.keys()))
    tema_frase = random.choice(TEMAS[tema_key])
    bucket = random.random()
    if bucket < 0.45:                       # terruqueo / odio político
        sujeto = random.choice(INSULTOS_TERRUQUEO)
    elif bucket < 0.80:                     # racismo / clasismo / regional
        sujeto = random.choice(INSULTOS_DISCRIMINACION)
    else:                                   # misoginia
        sujeto = random.choice(INSULTOS_GENERO)

    estilo = random.random()
    if estilo < 0.5:
        core = f"{sujeto} {random.choice(ACCIONES_ODIO)}, además {tema_frase}"
    else:
        core = f"{tema_frase} y encima {sujeto} {random.choice(ACCIONES_ODIO)}"
    return _cap(random.choice(APERTURAS) + core) + random.choice(CIERRES)


def _political_text() -> str:
    """Polarización política sin odio, a veces con tema."""
    base = random.choice(POLARIZACION)
    if random.random() < 0.5:
        tema_frase = random.choice(random.choice(list(TEMAS.values())))
        base = f"{base} {_cap(tema_frase)}."
    return base


def _civic_text() -> str:
    return random.choice(CIVICO)


def generate_record(hate_ratio: float = 0.45) -> dict:
    """
    Devuelve un registro 'crudo' (mismo esquema que data/sample_data.jsonl) con
    post_id único. Distribución: hate_ratio tóxico, resto político/cívico.
    """
    roll = random.random()
    if roll < hate_ratio:
        text = _toxic_text()
    elif roll < hate_ratio + (1 - hate_ratio) * 0.45:
        text = _political_text()
    else:
        text = _civic_text()

    source = random.choice(FUENTES)
    rec = {
        "id": f"fh_{uuid.uuid4().hex[:16]}",
        "text": text,
        "source": source,
        "author": f"user_{random.randint(1000, 999999)}",
    }
    if source == "reddit":
        rec["subreddit"] = random.choice(SUBREDDITS)
    return rec


def build_message(raw: dict) -> dict:
    """Enriquecimiento que viaja a Kafka (mismo contrato que producer_dataset)."""
    return {
        "post_id": raw["id"],
        "text": raw["text"],
        "source": raw.get("source", "unknown"),
        "author": raw.get("author", f"user_{random.randint(1000, 9999)}"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lang": "es",
        "metadata": {
            "likes": random.randint(0, 5000),
            "retweets": random.randint(0, 1500),
            "subreddit": raw.get("subreddit", ""),
        },
    }


def create_producer() -> KafkaProducer:
    """Productor optimizado para throughput (batching + linger + compresión)."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1,                 # throughput > durabilidad total (demo)
        retries=3,
        linger_ms=50,           # agrupa mensajes ~50ms antes de enviar
        batch_size=64 * 1024,   # lotes de 64KB
        compression_type="gzip",
        buffer_memory=64 * 1024 * 1024,
    )


# =============================================================================
# MODOS DE EJECUCIÓN
# =============================================================================
def run_firehose(rate: float, hate_ratio: float, limit: int = 0):
    """Bucle principal de alto volumen. rate=0 → sin límite (máximo posible)."""
    producer = create_producer()
    print(f"[FIREHOSE] Kafka: {BOOTSTRAP_SERVERS}")
    print(f"[FIREHOSE] Objetivo: {rate or 'MÁXIMO'} msg/s · hate_ratio={hate_ratio}"
          + (f" · límite={limit}" if limit else " · continuo"))
    print("[FIREHOSE] Ctrl+C para detener\n")

    sent = 0
    start = time.time()
    last_report = start
    # Para control de tasa por lotes (más eficiente que dormir por mensaje).
    batch = max(1, int(rate / 20)) if rate else 2000   # ~20 lotes/seg

    try:
        while True:
            t_batch = time.time()
            for _ in range(batch):
                raw = generate_record(hate_ratio)
                msg = build_message(raw)
                topic = "raw-tweets" if msg["source"] in ("twitter", "mastodon") else "raw-comments"
                producer.send(topic, key=msg["post_id"], value=msg)
                sent += 1
                if limit and sent >= limit:
                    break

            # Control de tasa: ajusta el sueño para acercarse a `rate`
            if rate:
                target_dt = batch / rate
                elapsed = time.time() - t_batch
                if elapsed < target_dt:
                    time.sleep(target_dt - elapsed)

            now = time.time()
            if now - last_report >= 2.0:
                thr = sent / (now - start)
                print(f"[{datetime.now():%H:%M:%S}] enviados={sent:,} · "
                      f"throughput={thr:,.0f} msg/s")
                last_report = now

            if limit and sent >= limit:
                break

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        elapsed = max(time.time() - start, 1e-6)
        print(f"\n[FIREHOSE] Detenido. Total={sent:,} en {elapsed:,.1f}s "
              f"(media {sent/elapsed:,.0f} msg/s)")


def run_gen_corpus(n: int, path: str, hate_ratio: float):
    """Genera un dataset JSONL grande y único (no toca Kafka)."""
    print(f"[CORPUS] Generando {n:,} registros únicos → {path}")
    seen = set()
    with open(path, "w", encoding="utf-8") as f:
        written = 0
        while written < n:
            rec = generate_record(hate_ratio)
            if rec["text"] in seen:
                continue
            seen.add(rec["text"])
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            written += 1
    print(f"[CORPUS] Listo: {written:,} registros únicos escritos.")


def main():
    ap = argparse.ArgumentParser(description="Firehose Kafka de alto volumen")
    ap.add_argument("--rate", type=float,
                    default=float(os.environ.get("FIREHOSE_RATE", "200")),
                    help="mensajes/segundo (0 = máximo)")
    ap.add_argument("--hate", type=float,
                    default=float(os.environ.get("FIREHOSE_HATE", "0.45")),
                    help="proporción de mensajes tóxicos [0..1]")
    ap.add_argument("--burst", type=int, default=0,
                    help="envía N mensajes y termina (stress test)")
    ap.add_argument("--gen-corpus", type=int, default=0,
                    help="genera N registros a un JSONL y termina (no usa Kafka)")
    ap.add_argument("outfile", nargs="?",
                    default=os.path.join(os.path.dirname(__file__), "..", "data", "sample_data.jsonl"),
                    help="archivo de salida para --gen-corpus")
    args = ap.parse_args()

    seed = os.environ.get("FIREHOSE_SEED")
    if seed:
        random.seed(int(seed))

    if args.gen_corpus:
        run_gen_corpus(args.gen_corpus, os.path.abspath(args.outfile), args.hate)
    else:
        run_firehose(args.rate, args.hate, limit=args.burst)


if __name__ == "__main__":
    main()
