"""
producer_twitch.py
==================
Productor Kafka que lee el CHAT EN VIVO de Twitch (comentarios de streaming) de
forma ANÓNIMA por IRC — no requiere credenciales ni token (usuario justinfan).

Fuente:  irc.chat.twitch.tv:6667  (IRC anónimo)
Topic de destino: raw-comments  (el chat es comentario tipo conversación)
Solo fluyen mensajes de canales que estén EN VIVO.
"""

import json
import os
import re
import socket
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "raw-comments"
HOST, PORT = "irc.chat.twitch.tv", 6667

# Canales (streamers grandes en español; al menos uno suele estar en vivo).
# Configurable con TWITCH_CHANNELS="canal1,canal2,..."
CHANNELS = os.environ.get(
    "TWITCH_CHANNELS",
    "auronplay,ibai,elxokas,westcol,rivers_gg,elmariana,spreen,illojuan,perxitaa,zeling",
).split(",")

_MSG_RE = re.compile(r":(\w+)!\w+@[\w.]+ PRIVMSG #(\w+) :(.*)")


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def build_message(user: str, channel: str, text: str) -> dict:
    return {
        "post_id": "twitch_" + uuid.uuid4().hex,   # único por mensaje
        "text": text,
        "source": "twitch",
        "author": user,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lang": "es",
        "metadata": {"channel": channel},
    }


def connect():
    s = socket.socket()
    s.settimeout(20)
    s.connect((HOST, PORT))
    s.send(f"NICK justinfan{uuid.uuid4().int % 90000 + 10000}\r\n".encode())
    for c in CHANNELS:
        s.send(f"JOIN #{c.strip()}\r\n".encode())
    return s


def main():
    producer = create_producer()
    print(f"[TWITCH PRODUCER] Kafka: {BOOTSTRAP_SERVERS}")
    print(f"[TWITCH PRODUCER] Canales: {CHANNELS}")

    sent = 0
    while True:  # reconexión automática si se cae
        try:
            s = connect()
            buf = ""
            print("[TWITCH PRODUCER] Conectado al chat IRC (anónimo).")
            while True:
                try:
                    data = s.recv(8192).decode("utf-8", "ignore")
                except socket.timeout:
                    s.send(b"PING :tmi.twitch.tv\r\n")  # keep-alive
                    continue
                if not data:
                    raise ConnectionError("conexión cerrada")
                buf += data
                while "\r\n" in buf:
                    line, buf = buf.split("\r\n", 1)
                    if line.startswith("PING"):
                        s.send(b"PONG :tmi.twitch.tv\r\n")
                        continue
                    m = _MSG_RE.match(line)
                    if not m:
                        continue
                    user, channel, text = m.group(1), m.group(2), m.group(3).strip()
                    if len(text) < 2:
                        continue
                    producer.send(TOPIC, key=None, value=build_message(user, channel, text))
                    sent += 1
                    if sent % 25 == 0:
                        print(f"[TWITCH PRODUCER] enviados: {sent}  (último: [{channel}] {user}: {text[:50]})")
        except Exception as e:
            print(f"[TWITCH PRODUCER] desconectado ({e}); reconectando en 5s...")
            try:
                producer.flush()
            except Exception:
                pass
            time.sleep(5)


if __name__ == "__main__":
    main()
