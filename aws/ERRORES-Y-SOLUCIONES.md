# Errores que tuvimos + soluciones + re-despliegue RÁPIDO

Registro real del primer despliegue en AWS Academy (us-east-1). Si vuelves a montar
desde cero, lee la sección **"Re-despliegue rápido"** al final: evita todos estos errores
y es mucho más veloz.

---

## Errores encontrados (y cómo se resolvieron)

### 1. La AMI de Ubuntu 22.04 "limpia" no existe en Inicio rápido
- **Síntoma:** en el lanzador, la única Ubuntu 22.04 trae **SQL Server** (cuesta extra). Las
  limpias son 24.04 / 26.04.
- **Solución:** usar **Ubuntu Server 24.04 LTS** (limpia, free tier). El proyecto se adaptó a 24.04.

### 2. Ubuntu 24.04 trae Python 3.12 y PyFlink 1.19 NO lo soporta
- **Síntoma:** PyFlink necesita Python 3.10/3.11; 24.04 trae 3.12.
- **Solución:** `setup.sh` instala Python 3.10 vía **PPA deadsnakes** (`add-apt-repository ppa:deadsnakes/ppa`).
- **Sub-error:** `python3.10-dev` NO está en deadsnakes para 24.04 → **no lo instales** (no hace falta;
  numpy/pyarrow/etc. tienen wheels). Ya quitado de `setup.sh`.

### 3. `archive.apache.org` LENTÍSIMO (~0.5 MB/s)
- **Síntoma:** bajar Flink (450 MB) + Spark (400 MB) + Kafka (114 MB) por nodo tarda 20-30 min.
  `dlcdn.apache.org` NO tiene las versiones 1.19.1 / 3.5.1 (están archivadas, da 404).
- **Solución (la que más tiempo ahorró):** descargar en **UN** nodo y copiar
  Flink/Spark/venv a los otros **EC2-a-EC2** (rsync intra-AWS, ~100 MB/s). Ver runbook.

### 4. El SSH se cae (Wi-Fi inestable) y mata `setup.sh`
- **Síntoma:** correr `ssh ... 'bash setup.sh'` ata el script a la sesión SSH; si la conexión
  se corta, el script muere con SIGHUP (se quedó sin venv a medias).
- **Solución:** correr **DESACOPLADO** en el remoto:
  `ssh ... 'nohup bash setup.sh > ~/setup.log 2>&1 &'`. Igual para arrancar-master y lanzar-jobs.

### 5. ⭐ Security Group bloqueaba el tráfico entre nodos (EL MÁS IMPORTANTE)
- **Síntoma:** los workers alcanzaban el master en `:22` (SSH) pero **`:6123, :9092, :7077`
  daban TIMEOUT**. Resultado: 0 TaskManagers, 0 Spark workers.
- **Causa:** la regla "All traffic desde el propio SG" quedó apuntando a **otro** grupo
  (el `sg-...` mostrado al crear el SG difería del SG real de las instancias).
- **Solución:** agregar una regla **NUEVA y separada**: Tipo **Todo el tráfico**, Origen
  **`172.31.0.0/16`** (el CIDR de la VPC). ⚠️ AWS NO deja mezclar CIDR + grupo en la misma
  regla ("No puede especificar un CIDR para una regla de ID de grupo") → tiene que ser regla aparte.

### 6. TaskManagers "gated" tras abrir el SG
- **Síntoma:** después de arreglar el SG, los Spark workers se conectaron solos pero los
  Flink TaskManagers seguían en 0 (estado de reintento bloqueado).
- **Solución:** reiniciarlos en cada worker:
  `source ~/.bdenv && taskmanager.sh stop && taskmanager.sh start`.

---

## Configuración correcta del Security Group (desde el inicio)

| Tipo | Puerto | Origen | Para |
|---|---|---|---|
| SSH | 22 | 0.0.0.0/0 (o Mi IP) | entrar por SSH |
| Custom TCP | 5000 | 0.0.0.0/0 | dashboard |
| Custom TCP | 8081 | 0.0.0.0/0 | Flink UI |
| Custom TCP | 8080 | 0.0.0.0/0 | Spark UI |
| **Todo el tráfico** | — | **`172.31.0.0/16`** | **entre los 3 nodos (CLAVE)** |

> Usa el **CIDR `172.31.0.0/16`** (no el "self-SG"). Es lo que evita el error #5.

---

## Re-despliegue RÁPIDO (desde cero, optimizado)

### A. Consola (tú) — ~8 min
1. Learner Lab → **Start Lab** → esperar 🟢 → **AWS** (consola, región us-east-1).
2. **AWS Details → Download PEM** → guarda `labsuser.pem`.
3. EC2 → Security Groups → crear `bigdata-sg` con las 5 reglas de arriba
   (¡incluida la de `172.31.0.0/16`!).
4. EC2 → Lanzar instancias: **3** × **Ubuntu 24.04** × **t3.large** × clave **vockey** ×
   SG **bigdata-sg** × **20 GiB** × IP pública **habilitada**.
5. Pásame: las **3 IPs públicas** + el **`labsuser.pem`**.

### B. Yo (por SSH, optimizado con copia EC2-a-EC2)
```bash
KEY=~/Descargas/labsuser.pem
# subir el proyecto a las 3
for ip in IP1 IP2 IP3; do
  rsync -az -e "ssh -i $KEY" --exclude='.git' --exclude='.venv' \
    Final-Big-Data-AWS/ ubuntu@$ip:~/Final-Big-Data-AWS/
done
# setup SOLO en 1 nodo (instala Java/Python3.10/Flink/Spark/venv) — DESACOPLADO
ssh -i $KEY ubuntu@IP1 'nohup bash ~/Final-Big-Data-AWS/aws/setup.sh > ~/setup.log 2>&1 &'
# ...esperar a que IP1 termine (~10 min con la descarga lenta)...
# copiar la llave a IP1 para que alcance a los otros, y desde IP2/IP3 jalar de IP1:
scp -i $KEY $KEY ubuntu@IP1:~/.ssh/ec2key.pem   # y chmod 600
# en IP2 e IP3: instalar java/python (rápido) y COPIAR flink/spark/venv de IP1 (rápido)
#   ssh IP2 'sudo apt install -y openjdk-11-jdk; add deadsnakes; apt install python3.10 python3.10-venv'
#   ssh IP2 'rsync de IP1: ~/flink ~/spark ~/Final-Big-Data-AWS/bigdata-proyecto/.venv ; escribir ~/.bdenv'
# arrancar (master = IP1):
ssh -i $KEY ubuntu@IP1 'nohup bash ~/Final-Big-Data-AWS/aws/arrancar-master.sh > ~/master.log 2>&1 &'
MPRIV=$(ssh -i $KEY ubuntu@IP1 'hostname -I | awk "{print \$1}"')   # IP privada master
ssh -i $KEY ubuntu@IP2 "bash ~/Final-Big-Data-AWS/aws/arrancar-worker.sh $MPRIV"
ssh -i $KEY ubuntu@IP3 "bash ~/Final-Big-Data-AWS/aws/arrancar-worker.sh $MPRIV"
# si 0 TaskManagers: reiniciarlos (gated) -> taskmanager.sh stop/start en IP2 e IP3
ssh -i $KEY ubuntu@IP1 'bash ~/Final-Big-Data-AWS/aws/lanzar-jobs.sh'
# dashboard: http://IP1_PUBLICA:5000
```

> **Idea para acelerar aún más la próxima:** subir Flink/Spark/Kafka + el venv a un bucket
> **S3** del Learner Lab UNA vez, y que el setup los baje de S3 (rápido intra-AWS) en lugar
> de `archive.apache.org`. (No implementado; opción si el re-despliegue se hará seguido.)

### C. Al terminar (SIEMPRE)
- EC2 → Instancias → seleccionar las 3 → **Detener** (Stop) o **Terminar** (Terminate).
  Cobran ~0.25 USD/h las 3.

---

## Datos del despliegue que funcionó (referencia)
- Región: us-east-1 · 3× t3.large Ubuntu 24.04 · 20 GiB · clave vockey.
- Master = JobManager + Spark Master + Kafka + Redis (NATIVOS, sin Docker) + dashboard + productor.
- 2 workers = TaskManager (4 slots c/u) + Spark Worker → 8 slots, trabajo repartido.
- Dashboard: `http://<IP_PUBLICA_MASTER>:5000` (panel muestra los 3 nodos).

---

## ⭐ ATAJO: las instancias PERSISTEN entre sesiones del Lab

Si **NO terminas** las instancias (solo se detienen al cerrar el Lab), al volver siguen ahí
con TODO instalado (Flink/Spark/venv/código en el disco EBS). Re-desplegar es **~2 min**:

1. Las **IPs públicas CAMBIAN** (stop/start las reasigna); las **privadas se MANTIENEN**
   (172.31.39.239 master, etc.) → la config sigue válida, no se reconfigura nada.
2. El **Security Group también persiste** (la regla `172.31.0.0/16` sigue puesta).
3. Solo re-arrancar los servicios (NO setup):
   ```bash
   KEY="~/Descargas/labsuser.pem"
   ssh -i "$KEY" ubuntu@<MASTER_PUB> 'nohup bash ~/Final-Big-Data-AWS/aws/arrancar-master.sh > ~/master.log 2>&1 &'
   # esperar ~50s (sin descargas: Kafka/Spark ya están)
   ssh -i "$KEY" ubuntu@<WORKER1_PUB> 'bash ~/Final-Big-Data-AWS/aws/arrancar-worker.sh 172.31.39.239'
   ssh -i "$KEY" ubuntu@<WORKER2_PUB> 'bash ~/Final-Big-Data-AWS/aws/arrancar-worker.sh 172.31.39.239'
   ssh -i "$KEY" ubuntu@<MASTER_PUB> 'bash ~/Final-Big-Data-AWS/aws/lanzar-jobs.sh'
   # dashboard: http://<MASTER_PUB>:5000
   ```

> **Recomendación:** para re-usar rápido, **DETÉN** (Stop) las instancias al terminar — NO las
> termines. Detenidas casi no cuestan (solo el disco). Solo **Terminate** si ya no las usarás más.
