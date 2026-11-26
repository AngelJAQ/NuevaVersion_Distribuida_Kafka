"""Capa mínima sobre confluent-kafka. El resto del proyecto solo usa estas funciones.

    publicar(topico, dict)            -> manda un mensaje (JSON) a un tópico
    asegurar_topicos([...])           -> crea los tópicos que falten (replicados en varios brokers)
    listar_topicos("chat.")           -> IDENTIFICA los tópicos que existen en el cluster
    crear_consumer(grupo, ...)        -> consumidor listo para usar
    estado_cluster()                  -> brokers vivos/caídos y líder de cada tópico

RELEVO DE BROKERS: KAFKA_SERVER lleva la lista de los 3 brokers. Si uno cae, la librería
reconecta sola con otro y Kafka elige nuevos líderes para sus particiones.
"""
import json
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from . import config
from .utilidades import imprimir_seguro

_producer = None
_ultimo_aviso = {}


def _al_error(err):
    """Avisos de la conexión con el cluster (por ejemplo, 'un broker dejó de responder').
    Se muestran una vez cada 20 s para no llenar la pantalla."""
    codigo = err.code()
    detalle = err.str()
    clave = (codigo, detalle[:30])
    ahora = time.time()
    if ahora - _ultimo_aviso.get(clave, 0) < 20:
        return
    _ultimo_aviso[clave] = ahora
    if codigo == KafkaError._ALL_BROKERS_DOWN:
        imprimir_seguro("[CLUSTER] Ningún broker responde. Reintentando conexión...")
    elif codigo == KafkaError._TRANSPORT:
        imprimir_seguro(f"[CLUSTER] Un broker dejó de responder ({detalle[:90]}). "
                        "Se continúa con los demás brokers del cluster.")
    else:
        imprimir_seguro(f"[CLUSTER] Aviso de Kafka: {err}")


def _base():
    # log_level=2: se silencian los mensajes técnicos de librdkafka; los avisos útiles salen por _al_error
    return {"bootstrap.servers": config.KAFKA_SERVER, "error_cb": _al_error, "log_level": 2}


def verificar_kafka():
    try:
        md = AdminClient(_base()).list_topics(timeout=5)
        ids = sorted(md.brokers)
        imprimir_seguro(f"[KAFKA] Conectado. Brokers activos en el cluster: {len(md.brokers)} de "
                        f"{config.REPLICACION} (ids: {ids})")
        if len(ids) < config.REPLICACION:
            faltan = [i for i in range(1, config.REPLICACION + 1) if i not in ids]
            imprimir_seguro(f"[AVISO] Faltan los brokers con id {faltan}. Revisa en ese equipo: "
                            "'docker logs kafka-<id>' y 'docker compose ... run --rm diagnostico'.")
        return True
    except KafkaException as e:
        imprimir_seguro(f"[ERROR] No se pudo conectar a Kafka en '{config.KAFKA_SERVER}': {e}\n"
                        "        ¿Están corriendo los contenedores? (docker compose ps)\n"
                        "        Tras 'docker compose up -d' espera unos 30 segundos.")
        return False


# ------------------------------------------------------------------ publicar
def _al_entregar(err, msg):
    if err is not None:
        imprimir_seguro(f"[ERROR] Kafka no entregó un mensaje a '{msg.topic()}': {err}")


def _get_producer():
    global _producer
    if _producer is None:
        # acks=all + idempotencia: el mensaje se confirma cuando varios brokers lo tienen
        # y si hay un reintento por la caída de un broker no se duplica.
        _producer = Producer({**_base(), "linger.ms": 5, "acks": "all", "enable.idempotence": True})
    return _producer


def publicar(topico, datos, key=None):
    """datos=None publica un 'mensaje de borrado' (tombstone) para esa key en tópicos compactados."""
    p = _get_producer()
    valor = None if datos is None else json.dumps(datos).encode("utf-8")
    p.produce(topico, value=valor, key=key.encode("utf-8") if key else None, on_delivery=_al_entregar)
    p.poll(0)


def vaciar():
    """Espera a que todo lo publicado esté confirmado por el cluster."""
    _get_producer().flush(15)


# ------------------------------------------------------------------ consumir
def crear_consumer(group_id, offset="latest", client_id=None, extra=None):
    conf = {**_base(), "group.id": group_id, "auto.offset.reset": offset,
            "enable.auto.commit": False,
            # si un consumidor se cae, en ~10 s Kafka le da su trabajo a otro del grupo
            "session.timeout.ms": 10000, "heartbeat.interval.ms": 3000}
    if client_id:
        conf["client.id"] = client_id
    if extra:
        conf.update(extra)
    return Consumer(conf)


def particiones_de(consumer, topico):
    meta = consumer.list_topics(topico, timeout=5).topics.get(topico)
    return sorted(meta.partitions) if meta else []


# ------------------------------------------------------------------ tópicos
def listar_topicos(prefijo=""):
    """IDENTIFICAR TÓPICOS: pregunta al cluster qué tópicos existen."""
    topicos = AdminClient(_base()).list_topics(timeout=5).topics
    return sorted(t for t in topicos if t.startswith(prefijo))


def asegurar_topicos(nombres, particiones=1, config_topico=None):
    """Crea los tópicos que falten.

    COLA DE SUCESIÓN DE BROKERS: las réplicas de cada partición se listan en orden de prioridad
    (broker 1, luego 2, luego 3). Kafka nombra líder a la primera réplica viva de esa lista:
    si cae el broker #1 pasa al #2, si cae también el #2 pasa al #3, y cuando el prioritario
    vuelve recupera el liderazgo."""
    admin = AdminClient(_base())
    md = admin.list_topics(timeout=5)
    faltan = [n for n in nombres if n not in set(md.topics)]
    if not faltan:
        return
    # Esperar a que lleguen los 3 brokers: un tópico creado con 2 réplicas se queda con 2 para siempre.
    limite_brokers = time.time() + config.ESPERA_BROKERS_S
    while len(md.brokers) < config.REPLICACION and time.time() < limite_brokers:
        time.sleep(1)
        md = admin.list_topics(timeout=5)
    if len(md.brokers) < config.REPLICACION:
        imprimir_seguro(f"[AVISO] Solo hay {len(md.brokers)} de {config.REPLICACION} brokers (ids {sorted(md.brokers)}). "
                        "Los tópicos se crean con las réplicas disponibles.")
    vivos = sorted(md.brokers)[:max(1, config.REPLICACION)]     # ids de broker, de mayor a menor prioridad
    cfg = config_topico or {}

    def crear(con_orden):
        if con_orden:
            nuevos = [NewTopic(n, num_partitions=-1, replication_factor=-1,
                               replica_assignment=[list(vivos) for _ in range(particiones)], config=cfg)
                      for n in faltan]
        else:   # respaldo: que Kafka reparta las réplicas
            nuevos = [NewTopic(n, num_partitions=particiones, replication_factor=len(vivos), config=cfg)
                      for n in faltan]
        for n, f in admin.create_topics(nuevos).items():
            try:
                f.result()
            except KafkaException as e:
                if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise

    try:
        crear(True)
    except (TypeError, ValueError, KafkaException) as e:
        imprimir_seguro(f"[AVISO] No se pudo fijar el orden de prioridad de las réplicas ({e}); "
                        "se crean los tópicos con el reparto automático de Kafka.")
        crear(False)

    limite = time.time() + 5      # esperar a que el cluster publique el nuevo tópico
    while time.time() < limite:
        if set(faltan) <= set(admin.list_topics(timeout=5).topics):
            return
        time.sleep(0.2)


def asegurar_calculadora():
    asegurar_topicos([config.TOPICO_SOLICITUDES, config.TOPICO_RESULTADOS, config.TOPICO_RESPUESTAS])
    asegurar_topicos([config.TOPICO_PLANES], config_topico={"cleanup.policy": "compact"})
    asegurar_topicos([config.TOPICO_LATIDOS], config_topico={"retention.ms": "600000"})   # 10 min
    asegurar_topicos([config.PREFIJO_CALC + r for r in config.ROLES_NODO],
                     particiones=config.PARTICIONES_CALC)


# ------------------------------------------------------------------ estado
def nodos_por_rol():
    """Devuelve {rol: [nicknames de los nodos conectados]} leyendo los consumer groups.
    Reemplaza a la tabla de roles que antes mantenía el broker."""
    admin = AdminClient(_base())
    resultado = {r: [] for r in config.ROLES_NODO}
    try:
        futuros = admin.describe_consumer_groups([f"nodos-{r}" for r in config.ROLES_NODO])
    except Exception:
        return None   # versión vieja de la librería: no disponible
    for grupo, fut in futuros.items():
        try:
            desc = fut.result()
            resultado[grupo[len("nodos-"):]] = sorted(m.client_id for m in desc.members)
        except Exception:
            pass
    return resultado


def estado_cluster():
    """Devuelve (brokers_vivos {id: 'host:puerto'}, ids_caidos [..], topicos {nombre: [particiones]}).
    Cada partición es {'lider': id, 'replicas': [ids], 'isr': [ids sincronizados]}."""
    md = AdminClient(_base()).list_topics(timeout=5)
    vivos = {bid: f"{b.host}:{b.port}" for bid, b in md.brokers.items()}
    conocidos = set(vivos)
    topicos = {}
    for nombre, t in md.topics.items():
        if not nombre.startswith((config.PREFIJO_CHAT, config.PREFIJO_CALC)):
            continue
        partes = []
        for pid in sorted(t.partitions):
            p = t.partitions[pid]
            conocidos.update(p.replicas)
            partes.append({"lider": p.leader, "replicas": list(p.replicas), "isr": list(p.isrs)})
        topicos[nombre] = partes
    return vivos, sorted(conocidos - set(vivos)), topicos
