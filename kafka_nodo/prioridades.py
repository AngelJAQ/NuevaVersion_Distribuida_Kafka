"""TABLA DE PRIORIDADES de los routers (equivalente a la 'cola de sucesión' del programa original).

Cada router publica un latido cada segundo en 'sistema.routers':
    {"id", "prioridad", "host", "activo"}
Quien escucha ese tópico arma esta tabla. Reglas:
  - Solo UN router está ACTIVO (el que procesa las solicitudes); los demás esperan.
  - Si el activo deja de dar latidos (equipo apagado, red caída), toma el mando el router
    VIVO con el número de prioridad más bajo (#1 antes que #2, etc.).
  - Si el equipo prioritario vuelve mientras otro está activo, no le quita el puesto (no hay
    vaivén): queda en espera como primero de la fila.
"""
import threading
import time

from . import config


class TablaRouters:
    def __init__(self):
        self.lock = threading.Lock()
        self.datos = {}          # id -> latido + 'visto' (hora local en que llegó)

    def actualizar(self, latido):
        with self.lock:
            if latido.get("bye"):                    # el router avisó que se despide
                self.datos.pop(latido["id"], None)
            else:
                self.datos[latido["id"]] = {**latido, "visto": time.time()}

    def vivos(self):
        """Routers con latido reciente."""
        limite = time.time() - config.TIMEOUT_ROUTER_S
        with self.lock:
            return [dict(r) for r in self.datos.values() if r["visto"] >= limite]

    def filas(self):
        """Para mostrar: lista de (prioridad, id, host, estado) ordenada por prioridad."""
        ahora = time.time()
        filas = []
        with self.lock:
            for rid in list(self.datos):
                r = self.datos[rid]
                edad = ahora - r["visto"]
                if edad > 120:                       # los caídos viejos se olvidan
                    del self.datos[rid]
                    continue
                if edad > config.TIMEOUT_ROUTER_S:
                    est = f"CAÍDO (sin latido hace {int(edad)} s)"
                else:
                    est = "ACTIVO" if r.get("activo") else "EN ESPERA"
                filas.append((r["prioridad"], r["id"], r.get("host", "?"), est))
        return sorted(filas)


def decidir(tabla, yo_id, yo_prioridad, yo_activo):
    """Devuelve 'activar', 'ceder' o None, según las reglas de arriba."""
    otros = [r for r in tabla.vivos() if r["id"] != yo_id]
    yo = (yo_prioridad, yo_id)
    otros_activos = [r for r in otros if r.get("activo")]

    if yo_activo:
        # Si hay otro activo con mejor prioridad (caso raro de dos activos a la vez), cedo.
        if any((r["prioridad"], r["id"]) < yo for r in otros_activos):
            return "ceder"
        return None

    if otros_activos:                                # ya hay quien trabaje: espero
        return None
    candidatos = sorted([(r["prioridad"], r["id"]) for r in otros] + [yo])
    return "activar" if candidatos[0] == yo else None
