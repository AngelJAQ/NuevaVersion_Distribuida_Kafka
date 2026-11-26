# Distribuida_Kafka

Chat por tópicos + calculadora distribuida sobre un cluster Kafka de 3 equipos (KRaft).
Todo corre con Docker Compose: no hace falta instalar Python ni Kafka, solo Docker.

## Qué levanta cada equipo

| Servicio  | Qué hace                                                     |
|-----------|--------------------------------------------------------------|
| `kafka`   | Broker del cluster (los 3 forman el cuórum)                  |
| `router`  | Router de la calculadora; prioridad de relevo = `NODE_ID`    |
| `nodo`    | Nodo de cálculo; atiende los roles de `NODO_ROLES`           |
| `cliente` | Consola de chat + calculadora (bajo demanda, no arranca solo)|

## Puesta en marcha (repetir en CADA equipo)

Los equipos se comunican por **Hamachi**, así que todas las IP son las `25.x.x.x` de Hamachi.
Cada equipo ya tiene su archivo listo: `equipo.pc1.env` (Led), `equipo.pc2.env` (Lizzard), `equipo.pc3.env` (Turip).

1. **Windows, una sola vez por equipo** (PowerShell como administrador):
   `Set-ExecutionPolicy -Scope Process Bypass; .\preparar_windows.ps1`
   Marca el adaptador de Hamachi como red *Privada* y abre los puertos 9092 y 9093.
2. Comprueba la red (con Hamachi encendido en los 3 equipos):

   ```bash
   docker compose --env-file equipo.pcN.env run --rm diagnostico
   ```

   Muestra qué puertos de qué equipo no se alcanzan (solo responden cuando el kafka de ese equipo está arriba).
3. Levanta el sistema (cambia `N` por 1, 2 o 3):

   ```bash
   docker compose --env-file equipo.pcN.env up -d --build
   docker compose --env-file equipo.pcN.env ps
   ```

   Hasta que haya al menos 2 brokers arriba, `router` y `nodo` se reinician solos
   mientras esperan al cuórum; es normal.
4. Si antes probaron otras IP, en los 3 equipos primero: `docker compose --env-file equipo.pcN.env down -v`
   (borra los datos viejos de Kafka, que guardan el cuórum anterior).

Reglas del `.env`: sin comentarios en la misma línea que un valor, y las 3 `IP_PCx` iguales en los 3 equipos.

## Si un broker no se une

```bash
docker logs kafka-N --tail 100
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
```

## Equipos que solo son clientes (no son uno de los 3 brokers)

Un cliente normal no levanta Kafka ni router: solo necesita **Hamachi** (dentro de la misma red que los 3 brokers),
Docker y el proyecto. No lleva `NODE_ID` ni `MI_IP`, y no necesita abrir puertos.

```bash
docker compose -f docker-compose.cliente.yml --env-file cliente.env run --rm --build cliente
```

Sin Docker (solo Python 3.10+): `pip install confluent-kafka prompt_toolkit`, y luego
`KAFKA_SERVER=25.21.176.255:9092,25.20.239.198:9092,25.33.81.76:9092 python cliente.py`
(en PowerShell: `$env:KAFKA_SERVER="..."; python cliente.py`).

## Cliente interactivo

```bash
docker compose --env-file equipo.pcN.env run --rm cliente                    # solo cliente
docker compose --env-file equipo.pcN.env run --rm cliente --rol suma resta   # cliente + roles
docker compose --env-file equipo.pcN.env run --rm cliente --rol todos
```

## Utilidades

```bash
docker compose --env-file equipo.pcN.env logs -f router     # ver qué router está ACTIVO
docker compose --env-file equipo.pcN.env logs -f nodo
docker compose --env-file equipo.pcN.env stop router        # simular caída del router de este equipo
docker compose --env-file equipo.pcN.env down               # apagar (conserva los datos de Kafka)
docker compose --env-file equipo.pcN.env down -v            # apagar y BORRAR los datos de Kafka
```

## Pruebas de relevo entre equipos

- Apaga el equipo 1 (o `stop router` / `stop kafka`): el router y el broker de prioridad 2 toman el mando.
- Al volver el equipo 1 recupera el liderazgo solo (~10 s para el broker).
- Para probar solo la parte de cálculo, usa distintos `NODO_ROLES` por equipo
  (p. ej. PC1 `suma resta`, PC2 `multiplicacion division`, PC3 `potencia modulo raiz`).

## Sin Docker (opcional)

`pip install -r requirements.txt` y `python router.py --prioridad N`, `python nodo_calculo.py todos`,
`python cliente.py`. Los brokers se leen de la variable `KAFKA_SERVER` o de `kafka_servidores.txt`.
