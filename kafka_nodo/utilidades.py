"""Funciones auxiliares: tiempo, impresión segura y limpieza de nombres."""
import re
import threading
import time

print_lock = threading.Lock()


def ahora_ms():
    return int(time.time() * 1000)


def latencia_str(ts_origen_ms):
    latencia = max(0, ahora_ms() - ts_origen_ms)
    hora = time.strftime("%H:%M:%S", time.localtime(ts_origen_ms / 1000.0))
    return f"[{hora} | Latencia: {latencia} ms]"


def imprimir_seguro(texto):
    with print_lock:
        print(texto)


def limpiar_nombre(texto):
    """Nombre válido de tópico: minúsculas, solo letras, números, '_' y '-'."""
    return re.sub(r"[^a-z0-9_-]+", "_", texto.strip().lower()).strip("_")
