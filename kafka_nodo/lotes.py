"""CLIENTE: seguimiento de los lotes de operaciones que este cliente ha pedido."""
import threading
import time

from .config import TIEMPO_MAX_LOTE_S, TOPICO_CALCULADORA
from .utilidades import ahora_ms, imprimir_seguro, latencia_str

lotes = {}
lock_lotes = threading.Lock()


def crear_lote(lote_id, total):
    with lock_lotes:
        lotes[lote_id] = {"total": total, "recibidos": 0, "errores": 0, "inicio": ahora_ms()}


def mostrar_paso(id_solicitud, ts, contenido):
    _, _, numero = id_solicitud.partition(".")
    imprimir_seguro(f"{latencia_str(ts)} @{TOPICO_CALCULADORA} #{numero} {contenido}")


def registrar_respuesta(id_solicitud, ts, contenido):
    lote_id, _, numero = id_solicitud.partition(".")
    progreso, resumen = "", None
    with lock_lotes:
        lote = lotes.get(lote_id)
        if lote:
            lote["recibidos"] += 1
            if "SISTEMA_ERROR" in contenido or "]: Error" in contenido:
                lote["errores"] += 1
            if lote["total"] > 1:
                progreso = f"[{numero}/{lote['total']}] "
            if lote["recibidos"] >= lote["total"]:
                del lotes[lote_id]
                if lote["total"] > 1:
                    resumen = (f"[LOTE {lote_id}] Completo: {lote['total']} resultados en "
                               f"{ahora_ms() - lote['inicio']} ms ({lote['errores']} con error)")
    imprimir_seguro(f"{latencia_str(ts)} @{TOPICO_CALCULADORA} {progreso}{contenido}")
    if resumen:
        imprimir_seguro(resumen)


def vigilar_lotes():
    """Si nadie contesta (p. ej. no hay router o nodo), no se queda esperando para siempre."""
    while True:
        time.sleep(2)
        limite = ahora_ms() - TIEMPO_MAX_LOTE_S * 1000
        with lock_lotes:
            vencidos = {k: v for k, v in lotes.items() if v["inicio"] < limite}
            for k in vencidos:
                del lotes[k]
        for lote_id, l in vencidos.items():
            imprimir_seguro(f"[LOTE {lote_id}] Sin respuesta tras {TIEMPO_MAX_LOTE_S} s: llegaron "
                            f"{l['recibidos']}/{l['total']}. ¿Está corriendo el router y hay un nodo con ese rol?")
