# Final-Big-Data — Versión AWS EC2

Esta es la **versión para la nube** del trabajo final de Big Data (detección de discurso
discriminatorio y polarización política en el contexto electoral peruano). Es **el mismo
pipeline** que la versión de máquinas físicas —

```
Kafka → Flink (5 jobs streaming) → Spark (5 jobs batch) → Redis → Dashboard
```
rec
— pero desplegado sobre **3 instancias EC2** (1 master + 2 workers) en lugar de PCs físicas.
El enunciado permite **AWS o varias computadoras como cluster**; esta carpeta cubre la opción AWS.

## Estructura

```
Final-Big-Data-AWS/
├── bigdata-proyecto/        ← el código del proyecto (idéntico a la versión física)
│   ├── producers/           ← productor del corpus REAL (16.6k textos multi-origen)
│   ├── flink-jobs/          ← 5 jobs streaming (PyFlink DataStream API)
│   ├── spark-jobs/          ← 5 jobs batch (PySpark)
│   ├── nlp/                 ← pipeline NLP léxico/regex (odio, terruqueo, discriminación)
│   ├── dashboard/           ← dashboard Flask + Chart.js (interactivo, tiempo real)
│   ├── data/corpus_real.jsonl ← datos REALES (Twitter/Reddit/noticias Perú)
│   └── docker-compose.cluster.yml ← Kafka (KRaft) + Redis para el master
└── aws/                     ← capa de despliegue en EC2 (lo nuevo de esta versión)
    ├── GUIA-AWS-ACADEMY.md  ←  EMPIEZA AQUÍ: guía clic a clic para AWS Academy
    ├── setup.sh             ← instala todo en cada instancia (Java 11/Python 3.10/Flink/Spark, nativo)
    ├── arrancar-master.sh   ← arranca el nodo master
    ├── arrancar-worker.sh   ← arranca un nodo worker (se une al master)
    ├── lanzar-jobs.sh       ← somete los 5 jobs Flink + productor (en el master)
    ├── verificar.sh         ← chequeo de salud del cluster
    ├── parar.sh             ← detiene servicios de un nodo
    └── requirements-cluster.txt ← dependencias Python probadas
```

## Cómo empezar

Lee **[`aws/GUIA-AWS-ACADEMY.md`](aws/GUIA-AWS-ACADEMY.md)** — está pensada para alguien sin
experiencia previa en AWS: usa solo la consola web y comandos para copiar y pegar.

Resumen ultra-corto (detalle en la guía):

```bash
# en las 3 instancias EC2 (Ubuntu 24.04, t3.large):
bash ~/Final-Big-Data-AWS/aws/setup.sh
# master:
bash ~/Final-Big-Data-AWS/aws/arrancar-master.sh         # imprime su IP privada
# cada worker:
bash ~/Final-Big-Data-AWS/aws/arrancar-worker.sh <IP_PRIVADA_MASTER>
# master:
bash ~/Final-Big-Data-AWS/aws/lanzar-jobs.sh
# dashboard:  http://<IP_PUBLICA_MASTER>:5000
```

## Subir esta carpeta a GitHub (para clonarla en EC2)

La forma más simple de llevar el código a las instancias es `git clone`. Para eso, sube esta
carpeta a un repositorio (una vez, desde tu laptop):

```bash
cd "Final-Big-Data-AWS"
git init -b main
git add .
git commit -m "Versión AWS EC2 del proyecto Big Data"
# crea un repo vacío en github.com y luego:
git remote add origin git@github.com:<tu-usuario>/Final-Big-Data-AWS.git
git push -u origin main
```

Después, en cada instancia EC2:
```bash
git clone git@github.com:<tu-usuario>/Final-Big-Data-AWS.git ~/Final-Big-Data-AWS
```
(O usa `scp` sin GitHub — ver la guía, §7 Opción B.)

## Diferencias con la versión de máquinas físicas

| | Físico (PCs) | AWS EC2 (esta carpeta) |
|---|---|---|
| Nodos | PC Ubuntu + Mac M4 (+ 3.º) | 3× EC2 `t3.large` Ubuntu 24.04 |
| IPs | LAN, autodetectadas | IP privada de la VPC |
| Python entre nodos | riesgo de versiones distintas | **idéntico** (Python 3.10 vía deadsnakes en las 3) |
| Kafka advertised | IP LAN | IP privada del master |
| Acceso dashboard | LAN | IP pública del master (puerto 5000) |
| Master hace cómputo | sí (también TM) | no: solo coordina; los 2 workers computan |

El código del pipeline (jobs, NLP, dashboard, productor) es **exactamente el mismo**.
