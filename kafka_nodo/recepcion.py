"""CLIENTE: qué hacer con cada mensaje que llega de Kafka."""
import json

from . import estado
from .config import PREFIJO_CHAT, TOPICO_LATIDOS, TOPICO_RESPUESTAS
from .lotes import mostrar_paso, registrar_respuesta
from .utilidades import imprimir_seguro, latencia_str


def manejar_mensaje(msg):
    topico = msg.topic()
    d = json.loads(msg.value().decode("utf-8"))

    if topico == TOPICO_LATIDOS:
        estado.tabla_routers.actualizar(d)

    elif topico == TOPICO_RESPUESTAS:
        if d["destino"] != estado.cid:      # la respuesta es para otro cliente
            return
        if d["tipo"] == "paso":
            mostrar_paso(d["id"], d["ts"], d["contenido"])
        else:
            registrar_respuesta(d["id"], d["ts"], d["contenido"])

    elif topico.startswith(PREFIJO_CHAT):
        if d["id"] == estado.cid:           # mi propio mensaje: no se muestra de nuevo
            return
        imprimir_seguro(f"{latencia_str(d['ts'])} @{topico[len(PREFIJO_CHAT):]} [{d['nick']}]: {d['texto']}")
