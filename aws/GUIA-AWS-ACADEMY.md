# Guía paso a paso — Cluster Big Data en AWS EC2 (AWS Academy / Learner Lab)

Esta guía monta **el mismo proyecto** que la versión de máquinas físicas, pero sobre
**3 instancias EC2** (1 master + 2 workers), usando solo la **consola web** de AWS y
unos scripts que copias y pegas. No necesitas saber Terraform ni la línea de comandos
de AWS. Está pensada para el **Learner Lab de AWS Academy**.

> ⏱️ Tiempo aprox.: 35–50 min la primera vez (la instalación descarga Flink/Spark).
> En arranques siguientes, ~5 min.

---

## 0. Topología que vas a montar

```
                 ┌─────────────────────────────────────────────┐
                 │  VPC por defecto · Security Group "bigdata-sg"│
                 │                                               │
   tú (laptop) ──┼──► MASTER  (EC2 #1, t3.large)                 │
   navegador/SSH │      Kafka + Redis (nativos)                   │
                 │      Flink JobManager · Spark Master          │
                 │      Dashboard (5000) · Productor             │
                 │            ▲              ▲                    │
                 │            │ red privada  │                    │
                 │      ┌─────┘              └─────┐              │
                 │   WORKER #1 (EC2 #2)      WORKER #2 (EC2 #3)   │
                 │   Flink TaskManager       Flink TaskManager    │
                 │   Spark Worker            Spark Worker         │
                 └─────────────────────────────────────────────┘
```

**Por qué 3 nodos (lo pide el profe):** el master coordina y sirve (no hace cómputo
pesado); los 2 workers aportan los **8 slots de Flink** (4 cada uno) y los executors de
Spark. Así "1 master + 2 workers" queda justificado.

---

## 1. Antes de empezar

- **Costo:** el Free Tier (t2.micro, 1 GB) **no alcanza** para Flink+Spark+Kafka. Usamos
  **`t3.large`** (2 vCPU, 8 GB). Tres de esas ≈ **0.25 USD/hora en total**. El Learner Lab
  te da créditos (normalmente 50–100 USD); una demo de 1–2 h gasta centavos.
  **⚠️ Al terminar, haz STOP o TERMINATE de las 3 instancias** (ver §10) o seguirán gastando.
- **Región:** el Learner Lab casi siempre te obliga a **`us-east-1`** (N. Virginia). Déjalo así.
- **Sistema operativo:** usamos **Ubuntu Server 24.04 LTS** (el setup.sh instala Python 3.10 solo, que es el
  que necesita el proyecto — esto elimina el problema de versiones de Python entre nodos).

---

## 2. Iniciar el Learner Lab y abrir la consola de AWS

1. Entra a tu curso en **AWS Academy** → módulo **Learner Lab** → **Start Lab**.
2. Espera a que el círculo junto a "AWS" se ponga **verde** 🟢.
3. Clic en **AWS** (el texto con el punto verde) → se abre la **consola de AWS** en una pestaña.
4. Arriba a la derecha confirma que la región diga **N. Virginia (us-east-1)**.

> El Learner Lab ya trae una **VPC por defecto** y un **key pair llamado `vockey`**. Los usaremos.

---

## 3. Descargar la llave SSH (`vockey`)

Para entrar por SSH a las instancias necesitas el archivo de la llave:

1. En la página del Learner Lab (la de "Start Lab"), clic en **AWS Details**.
2. En **SSH key**, clic en **Download PEM** → se baja `labsuser.pem`.
3. Guárdalo en una carpeta tuya y dale permisos (en tu laptop):
   ```bash
   chmod 400 ~/Descargas/labsuser.pem
   ```
   (En Windows usa PowerShell o PuTTY; en Mac/Linux el comando de arriba.)

---

## 4. Crear el Security Group (las reglas de red)

Esto define quién puede hablar con quién. Lo creamos **una vez**.

1. Consola AWS → busca **EC2** → menú izquierdo **Security Groups** → **Create security group**.
2. **Name:** `bigdata-sg`   ·   **Description:** `cluster bigdata`   ·   **VPC:** deja la *default*.
3. **Inbound rules** → agrega estas 3 reglas con **Add rule**:

   | Type            | Port range | Source                | Para qué |
   |-----------------|------------|-----------------------|----------|
   | SSH             | 22         | **My IP**             | entrar tú por SSH |
   | Custom TCP      | 5000       | **My IP**             | ver el dashboard en tu navegador |
   | All traffic     | (todo)     | **Custom `172.31.0.0/16`** | que las 3 EC2 se hablen entre sí |

   - Para la 4.ª regla: en **Type** elige `All traffic`; en **Source** elige `Custom` y escribe
     **`172.31.0.0/16`** (el CIDR de la VPC). Esto permite todo el tráfico *interno* del cluster
     (Kafka 9092, Flink 6123/8081, Spark 7077/8080, Redis 6379…) entre las 3 EC2.
     > ⚠️ **NO uses "el propio SG" como origen** — al crear el SG ese auto-referencia suele quedar
     > apuntando a otro grupo y el tráfico entre nodos queda BLOQUEADO (solo SSH abierto). Usa el CIDR.
     > Y OJO: AWS no deja mezclar un CIDR con un grupo en la misma regla; debe ser una regla aparte.
   - (Opcional) si quieres ver las UIs de Flink/Spark desde tu navegador, agrega también
     `Custom TCP 8081` y `Custom TCP 8080` con Source **My IP**.
4. **Create security group**.

---

## 5. Lanzar las 3 instancias EC2

Las 3 son iguales. Repite estos pasos **3 veces** (o lanza 1 y usa "Launch more like this").

1. EC2 → **Instances** → **Launch instances**.
2. **Name:** `bigdata-master` (luego `bigdata-worker-1`, `bigdata-worker-2`).
3. **Application and OS Images:** elige **Ubuntu** → **Ubuntu Server 24.04 LTS** (64-bit x86, free tier, sin SQL Server).
4. **Instance type:** **`t3.large`**.
5. **Key pair:** elige **`vockey`** (el que ya existe).
6. **Network settings** → **Edit** → en **Firewall (security groups)** elige
   **Select existing security group** → marca **`bigdata-sg`**.
7. **Configure storage:** sube a **20 GiB** (Flink+Spark+corpus ocupan espacio).
8. **Launch instance**.

Repite para `bigdata-worker-1` y `bigdata-worker-2`.

Cuando estén **Running**, anota de la lista de instancias (columna o pestaña *Details*):

| Instancia        | IP pública (Public IPv4) | IP privada (Private IPv4) |
|------------------|--------------------------|---------------------------|
| bigdata-master   | `____.____.____.____`    | `172.31.__.__`            |
| bigdata-worker-1 | `____.____.____.____`    | `172.31.__.__`            |
| bigdata-worker-2 | `____.____.____.____`    | `172.31.__.__`            |

> La **IP pública** sirve para conectarte tú (SSH y dashboard). La **IP privada** es la que
> usan las máquinas para hablarse entre ellas (es la que pide `arrancar-worker.sh`).

---

## 6. Conectarte por SSH a cada instancia

Desde tu laptop, una terminal por instancia (usa la **IP pública**):

```bash
ssh -i ~/Descargas/labsuser.pem ubuntu@<IP-PUBLICA-DE-LA-INSTANCIA>
```

La primera vez pregunta "Are you sure…?" → escribe `yes`. El usuario es **`ubuntu`**.

---

## 7. Traer el código del proyecto a cada instancia

**No tienes que hacer nada aquí.** Una vez que me pases las **IP públicas** de las 3
instancias y la **llave** (`labsuser.pem`), el equipo sube la carpeta `Final-Big-Data-AWS`
a las 3 instancias **por SSH (rsync)** desde la laptop, y corre la configuración por ti.

Queda en `~/Final-Big-Data-AWS` en cada instancia, con los scripts en `~/Final-Big-Data-AWS/aws/`.

> Alternativas manuales (por si prefieres hacerlo tú):
> ```bash
> # desde tu laptop, una vez por instancia:
> rsync -avz -e "ssh -i ~/Descargas/labsuser.pem" "Final-Big-Data-AWS" ubuntu@<IP-PUBLICA>:~/
> # o, si la subes a GitHub:  git clone <URL>.git ~/Final-Big-Data-AWS
> ```

---

## 8. Instalar el stack en las 3 instancias

En CADA instancia (master y los 2 workers), corre lo mismo:

```bash
bash ~/Final-Big-Data-AWS/aws/setup.sh
```

Esto instala Java 11, Python 3.10 (deadsnakes), descarga Flink 1.19.1 y Spark 3.5.1, el conector
Kafka y crea el entorno Python. Tarda varios minutos (descarga ~600 MB). Es **idempotente**:
si algo falla, vuelve a correrlo.

Al terminar, verás `LISTO. Instalación completa` (sin Docker, todo nativo).

> Puedes correr `setup.sh` en las 3 instancias **al mismo tiempo** (una terminal cada una).

---

## 9. Arrancar el cluster (orden importa)

**9.1 — En el MASTER:**
```bash
bash ~/Final-Big-Data-AWS/aws/arrancar-master.sh
```
Al final imprime su **IP privada** y las URLs. **Copia esa IP privada.**

**9.2 — En CADA worker** (las 2 terminales de los workers), usando esa IP privada:
```bash
bash ~/Final-Big-Data-AWS/aws/arrancar-worker.sh <IP-PRIVADA-DEL-MASTER>
```
Cada worker debería decir que se unió. Cuando los 2 estén arriba hay **8 slots**.

**9.3 — De vuelta en el MASTER**, lanza los jobs y el productor de datos reales:
```bash
bash ~/Final-Big-Data-AWS/aws/lanzar-jobs.sh
```

**9.4 — Verificación rápida (en el master):**
```bash
bash ~/Final-Big-Data-AWS/aws/verificar.sh
```
Deberías ver: 2 TaskManagers, 8 slots, 5 jobs RUNNING, 2 Spark workers ALIVE,
`metrics:total_processed` creciendo, dashboard HTTP 200.

---

## 10. Abrir el dashboard

En tu navegador:
```
http://<IP-PUBLICA-DEL-MASTER>:5000
```
Verás el dashboard en vivo con datos **100% reales** (corpus de Twitter/Reddit/noticias del
Perú). Las UIs de Flink y Spark (si abriste esos puertos): `:8081` y `:8080`.

---

## 11. ⚠️ Apagar para NO gastar créditos

Detener los servicios **no** deja de cobrar — lo que cobra es la **instancia encendida**.

- **Pausa corta (seguir mañana):** EC2 → selecciona las 3 → **Instance state → Stop**.
  No cobra cómputo; el disco sigue (centavos). Para volver: **Start** y repite §9.
  *(Ojo: al hacer Stop/Start cambia la IP pública; la privada se mantiene.)*
- **Terminar (ya no la necesitas):** **Instance state → Terminate**. Borra la instancia.
  Tendrás que relanzar desde §5 la próxima vez.
- El **Learner Lab también se apaga solo** al cerrar la sesión del lab, pero **no termina tus
  instancias**: revisa siempre que queden en *stopped* o *terminated*.

---

## 12. Problemas comunes

| Síntoma | Causa probable | Solución |
|---|---|---|
| SSH "Connection timed out" | falta la regla SSH/My IP, o tu IP cambió | en `bigdata-sg` revisa la regla SSH = **My IP** (tu IP pública cambia con la red) |
| El worker no aparece (slots < 8) | el master no estaba listo, o red bloqueada | confirma master arriba; revisa que la 3.ª regla SG = **All traffic / la propia SG** |
| Dashboard no carga en el navegador | falta regla 5000/My IP, o tu IP cambió | revisa la regla 5000 = **My IP**; usa la **IP pública** del master |
| Jobs Flink se caen al rato | poca memoria si pusiste un tipo chico | usa **t3.large**; no resometas jobs en bucle (presiona memoria) |
| "Public IPv4" vacío | la instancia no tiene IP pública | EC2 → Instance → Actions → Networking → asociar IP, o relanzar con auto-assign public IP **on** |

---

## 13. Resumen de comandos (chuleta)

```bash
# en las 3 instancias:
bash ~/Final-Big-Data-AWS/aws/setup.sh

# master:
bash ~/Final-Big-Data-AWS/aws/arrancar-master.sh      # imprime IP privada

# cada worker:
bash ~/Final-Big-Data-AWS/aws/arrancar-worker.sh <IP_PRIVADA_MASTER>

# master:
bash ~/Final-Big-Data-AWS/aws/lanzar-jobs.sh
bash ~/Final-Big-Data-AWS/aws/verificar.sh

# dashboard:  http://<IP_PUBLICA_MASTER>:5000

# detener servicios en un nodo (NO apaga la EC2):
bash ~/Final-Big-Data-AWS/aws/parar.sh
# y luego, en la consola AWS: Instance state -> Stop (o Terminate)
```
