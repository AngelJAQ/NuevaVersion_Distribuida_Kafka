"""ROUTER: reemplaza al despachador del broker. REDIRIGE cada solicitud al tópico de su rol.

    calc.solicitudes  --(detecta el rol)-->  calc.<rol>
    calc.resultados   --(avanza el plan o responde)-->  calc.respuestas

RELEVO ENTRE EQUIPOS (tabla de prioridades):
  Se ejecuta un router por equipo, cada uno con su número de prioridad (--prioridad N).
  Solo el router ACTIVO procesa mensajes; los demás envían latidos y esperan. Si el activo
  cae, toma el mando el router vivo de mejor prioridad (ver prioridades.py). Para no perder
  las expresiones compuestas a medias, cada plan se guarda en 'calc.planes' ANTES de mandar
  cada paso, y el router que toma el mando lo recupera de ahí y retoma desde el último
  mensaje confirmado.
"""
import json
import socket
import threading
import time
import uuid

from confluent_kafka import OFFSET_BEGINNING, TopicPartition

from . import config
from .calculadora import (EJEMPLOS_CALC, ErrorExpresion, detectar_rol_operacion, expresion_del_paso,
                          formatear_numero, planificar_expresion)
from .escucha import Escucha
from .kafka_util import (asegurar_calculadora, crear_consumer, particiones_de, publicar, vaciar,
                         verificar_kafka)
from .prioridades import TablaRouters, decidir
from .utilidades import ahora_ms, imprimir_seguro

planes = {}   # id_solicitud -> estado de una expresión compuesta (copia en memoria)


# ------------------------------------------------------------------ respaldo de planes
def guardar_plan(sol_id):
    publicar(config.TOPICO_PLANES, planes[sol_id], key=sol_id)
    vaciar()      # hasta que Kafka lo confirme no se manda el paso


def borrar_plan(sol_id):
    planes.pop(sol_id, None)
    publicar(config.TOPICO_PLANES, None, key=sol_id)    # mensaje de borrado


def cargar_planes():
    """Lee 'calc.planes' completo y devuelve los planes que siguen pendientes."""
    consumer = crear_consumer(f"router-restaurar-{uuid.uuid4().hex[:6]}", "earliest")
    try:
        tps = [TopicPartition(config.TOPICO_PLANES, p, OFFSET_BEGINNING)
               for p in particiones_de(consumer, config.TOPICO_PLANES)]
        consumer.assign(tps)
        fin = {}
        for tp in tps:
            bajo, alto = consumer.get_watermark_offsets(tp, timeout=5)
            if alto > bajo:
                fin[tp.partition] = alto
        pendientes = {}
        silencios, limite = 0, time.time() + 20
        while fin and silencios < 3 and time.time() < limite:
            msg = consumer.poll(1.0)
            if msg is None:
                silencios += 1
                continue
            silencios = 0
            if msg.error():
                continue
            clave = msg.key().decode("utf-8")
            if msg.value() is None:
                pendientes.pop(clave, None)
            else:
                pendientes[clave] = json.loads(msg.value().decode("utf-8"))
            if msg.offset() + 1 >= fin.get(msg.partition(), 0):
                fin.pop(msg.partition(), None)
        return pendientes
    finally:
        consumer.close()


# ------------------------------------------------------------------ lógica de redirección
def redirigir(sol_id, destino, rol, expr, paso=0, compuesta=False):
    """REDIRIGIR: manda la operación al tópico del rol (calc.suma, calc.resta...)."""
    topico = config.PREFIJO_CALC + rol
    publicar(topico, {"id": sol_id, "destino": destino, "rol": rol, "expr": expr,
                      "paso": paso, "compuesta": compuesta, "ts_cola": ahora_ms()}, key=sol_id)
    imprimir_seguro(f"[REDIRIGIR] #{sol_id} '{expr}' -> {topico}")


def responder(destino, sol_id, etiqueta, texto, tipo="res"):
    publicar(config.TOPICO_RESPUESTAS, {"destino": destino, "id": sol_id, "tipo": tipo,
                                        "ts": ahora_ms(), "contenido": f"[{etiqueta}]: {texto}"},
             key=destino)


def manejar_solicitud(d):
    sol_id, destino, expr = d["id"], d["destino"], d["expr"]
    imprimir_seguro(f"[SOLICITUD] #{sol_id} de '{d['nick']}': {expr}")

    rol = detectar_rol_operacion(expr)
    if rol:                                   # operación simple
        redirigir(sol_id, destino, rol, expr)
        return

    plan = planes.get(sol_id)
    if plan is None:                          # expresión compuesta nueva: se divide en pasos
        try:
            pasos = planificar_expresion(expr)
            if not pasos:
                raise ErrorExpresion("no contiene ninguna operación")
        except ErrorExpresion as e:
            responder(destino, sol_id, "SISTEMA_ERROR", f"Expresión no válida: {e}. Ejemplos: {EJEMPLOS_CALC}")
            return
        planes[sol_id] = {"destino": destino, "original": expr, "pasos": pasos, "valores": [], "traza": []}
        guardar_plan(sol_id)
        imprimir_seguro(f"[PLAN] #{sol_id} '{expr}' se resolverá en {len(pasos)} pasos, uno por uno.")
        plan = planes[sol_id]

    i = len(plan["valores"])                  # (si ya existía, se repite el paso actual)
    if i < len(plan["pasos"]):
        rol, expr_paso = expresion_del_paso(plan["pasos"][i], plan["valores"])
        redirigir(sol_id, destino, rol, expr_paso, paso=i, compuesta=True)


def manejar_resultado(d):
    sol_id, destino = d["id"], d["destino"]
    etiqueta = "SISTEMA_ERROR" if d.get("sistema") else f"NODO {d['rol'].upper()} ({d['nick_nodo']})"

    if not d.get("compuesta"):                # operación simple: se entrega tal cual
        responder(destino, sol_id, etiqueta, d["texto"])
        return

    plan = planes.get(sol_id)
    if plan is None:
        # Puede ser un resultado repetido de un plan ya terminado: se ignora.
        return
    i, total = len(plan["valores"]), len(plan["pasos"])
    if d["paso"] != i:                        # resultado repetido (Kafka entrega "al menos una vez")
        return

    if d["valor"] is None:                    # error en un paso: se cancela el resto
        responder(destino, sol_id, etiqueta, f"{d['texto']}  (paso {i + 1}/{total} de '{plan['original']}')")
        borrar_plan(sol_id)
        return

    plan["valores"].append(d["valor"])
    plan["traza"].append(d["texto"])
    if i + 1 == total:
        responder(destino, sol_id, "RESULTADO FINAL",
                  f"{plan['original']} = {formatear_numero(d['valor'])}  [pasos: {' → '.join(plan['traza'])}]")
        borrar_plan(sol_id)
        return

    guardar_plan(sol_id)                      # respaldo ANTES de mandar el siguiente paso
    responder(destino, sol_id, etiqueta, f"Paso {i + 1}/{total}: {d['texto']}  [{etiqueta}]", tipo="paso")
    rol, expr_paso = expresion_del_paso(plan["pasos"][i + 1], plan["valores"])
    redirigir(sol_id, destino, rol, expr_paso, paso=i + 1, compuesta=True)


# ------------------------------------------------------------------ router con prioridad
class RouterPrioridad:
    def __init__(self, prioridad):
        self.prioridad = prioridad
        self.id = f"router-{uuid.uuid4().hex[:4]}"
        self.host = socket.gethostname()
        self.activo = False
        self.tabla = TablaRouters()
        self.consumer = None
        self._parar = threading.Event()
        self._bye = True

    def detener(self, bye=True):
        """Pide parar. bye=False simula una caída: no avisa a los demás, que se enteran por falta de latidos."""
        self._bye = bye
        self._parar.set()

    # ---- latidos y decisión
    def _latido(self):
        return {"id": self.id, "prioridad": self.prioridad, "host": self.host, "activo": self.activo}

    def _ciclo_prioridad(self):
        ahora = time.time()
        if ahora >= self._proximo_latido:
            self._proximo_latido = ahora + config.LATIDO_S
            publicar(config.TOPICO_LATIDOS, self._latido(), key=self.id)
        if ahora - self._inicio < config.GRACIA_ARRANQUE_S:     # primero escucho quién más hay
            return
        accion = decidir(self.tabla, self.id, self.prioridad, self.activo)
        if accion == "activar":
            self._activar()
        elif accion == "ceder":
            self._desactivar()

    def _activar(self):
        imprimir_seguro(f"[ROUTER] Prioridad #{self.prioridad} ({self.host}): ahora soy el router ACTIVO. "
                        "Recuperando planes pendientes de 'calc.planes'...")
        planes.clear()
        planes.update(cargar_planes())
        # Retomo desde el último mensaje confirmado por el router anterior (o desde el inicio)
        tps = [TopicPartition(t, p) for t in (config.TOPICO_SOLICITUDES, config.TOPICO_RESULTADOS)
               for p in particiones_de(self.consumer, t)]
        self.consumer.assign(self._offsets_a_retomar(tps))
        self.activo = True
        self._proximo_latido = 0          # aviso inmediato: "ya soy el activo"
        imprimir_seguro(f"[ROUTER] {len(planes)} expresión(es) compuesta(s) recuperada(s). Continúo trabajando.")

    def _offsets_a_retomar(self, tps):
        resultado = []
        for tp in self.consumer.committed(tps, timeout=10):
            resultado.append(TopicPartition(tp.topic, tp.partition,
                                            tp.offset if tp.offset >= 0 else OFFSET_BEGINNING))
        return resultado

    def _desactivar(self):
        imprimir_seguro("[ROUTER] Hay un router de mayor prioridad activo: paso a EN ESPERA.")
        self.consumer.unassign()
        planes.clear()
        self.activo = False
        self._proximo_latido = 0

    # ---- bucle principal
    def correr(self):
        if not verificar_kafka():
            return
        asegurar_calculadora()
        self._escucha = Escucha(f"router-latidos-{self.id}", self._al_latido)
        self._escucha.fijar({config.TOPICO_LATIDOS})
        # El grupo solo sirve para guardar hasta dónde se leyó (los offsets); no reparte trabajo
        self.consumer = crear_consumer("router-activo", "earliest", client_id=self.id)
        self._inicio, self._proximo_latido = time.time(), 0
        imprimir_seguro(f"[ROUTER] '{self.id}' en '{self.host}' | PRIORIDAD #{self.prioridad}. "
                        f"Escuchando a los demás routers {config.GRACIA_ARRANQUE_S:.0f} s antes de decidir "
                        "(Ctrl+C para salir)...")
        try:
            while not self._parar.is_set():
                self._ciclo_prioridad()
                if not self.activo:
                    self._parar.wait(0.2)
                    continue
                msg = self.consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    imprimir_seguro(f"[ERROR] Kafka: {msg.error()}")
                    continue
                try:
                    d = json.loads(msg.value().decode("utf-8"))
                    if msg.topic() == config.TOPICO_SOLICITUDES:
                        manejar_solicitud(d)
                    else:
                        manejar_resultado(d)
                    vaciar()
                except Exception as e:
                    imprimir_seguro(f"[ERROR] No se pudo procesar el mensaje de '{msg.topic()}': {e}")
                self.consumer.commit(message=msg, asynchronous=False)
        finally:
            self._cerrar()

    def _al_latido(self, msg):
        if msg.topic() == config.TOPICO_LATIDOS:
            self.tabla.actualizar(json.loads(msg.value().decode("utf-8")))

    def _cerrar(self):
        try:
            if self._bye:                 # despedida: los demás lo saben al instante, sin esperar el timeout
                publicar(config.TOPICO_LATIDOS, {"id": self.id, "bye": True}, key=self.id)
                vaciar()
        finally:
            self.activo = False
            if self.consumer:
                self.consumer.close()


def ejecutar_router(prioridad=1):
    try:
        RouterPrioridad(prioridad).correr()
    except KeyboardInterrupt:
        pass
