"""NODO DE CÁLCULO: consume el tópico de su rol, resuelve y publica el resultado.

Cada rol usa su propio consumer group (nodos-suma, nodos-resta...). Así:
  - dos nodos con el mismo rol se reparten el trabajo,
  - si uno se cae, Kafka le da sus particiones al otro (reemplaza la 'herencia de roles'),
  - si nadie atiende el rol, las solicitudes esperan en el tópico hasta que aparezca un nodo.
"""
import json
import threading

from . import config
from .calculadora import calcular_operacion
from .kafka_util import asegurar_calculadora, crear_consumer, publicar, vaciar
from .utilidades import ahora_ms, imprimir_seguro


def iniciar_trabajador(roles, nick):
    asegurar_calculadora()
    for rol in roles:
        threading.Thread(target=_bucle_rol, args=(rol, nick), name=f"nodo_{rol}", daemon=True).start()


def _bucle_rol(rol, nick):
    consumer = crear_consumer(f"nodos-{rol}", "earliest", client_id=nick)
    consumer.subscribe([config.PREFIJO_CALC + rol])
    imprimir_seguro(f"[NODO] '{nick}' atiende el rol {rol.upper()} "
                    f"(tópico {config.PREFIJO_CALC}{rol}, grupo nodos-{rol})")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            imprimir_seguro(f"[ERROR] Kafka ({rol}): {msg.error()}")
            continue
        try:
            resolver(rol, nick, json.loads(msg.value().decode("utf-8")))
        except Exception as e:
            imprimir_seguro(f"[ERROR] Falló el trabajo de '{rol}': {e}")
        # El commit va DESPUÉS de responder: si el nodo se cae a medias, otro reintenta el mensaje
        consumer.commit(message=msg, asynchronous=False)


def resolver(rol, nick, d):
    sistema = False
    if ahora_ms() - d["ts_cola"] > config.TIEMPO_MAX_ESPERA_S * 1000:
        texto, valor, sistema = (f"Ningún nodo tomó la operación '{rol.upper()}' en "
                                 f"{config.TIEMPO_MAX_ESPERA_S} s."), None, True
    else:
        texto, valor = calcular_operacion(d["expr"], rol_esperado=rol)

    imprimir_seguro(f"[TRABAJO | {rol.upper()}] {d['expr']}  ->  {texto}")
    publicar(config.TOPICO_RESULTADOS, {
        "id": d["id"], "destino": d["destino"], "paso": d["paso"], "compuesta": d["compuesta"],
        "rol": rol, "nick_nodo": nick, "valor": valor, "texto": texto, "sistema": sistema,
    }, key=d["id"])
    vaciar()
