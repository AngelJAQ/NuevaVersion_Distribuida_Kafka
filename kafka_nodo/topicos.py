"""CLIENTE: suscripción a tópicos de chat y el comando /topico."""
from . import estado
from .config import PREFIJO_CHAT, TOPICO_LATIDOS, TOPICO_POR_DEFECTO, TOPICO_RESPUESTAS
from .kafka_util import asegurar_topicos
from .utilidades import imprimir_seguro, limpiar_nombre


def kafka_de(nombre):
    return PREFIJO_CHAT + nombre


def sincronizar_suscripcion():
    """Crea en Kafka los tópicos que falten y le dice al consumidor cuáles escuchar."""
    asegurar_topicos([kafka_de(t) for t in estado.mis_topicos])
    estado.escucha.fijar({kafka_de(t) for t in estado.mis_topicos} | {TOPICO_RESPUESTAS, TOPICO_LATIDOS})


def cambiar_topico_exclusivo(topico):
    estado.mis_topicos = {topico}
    estado.topico_publicacion = topico
    sincronizar_suscripcion()


def agregar_topico(topico):
    estado.mis_topicos.add(topico)
    estado.topico_publicacion = topico
    sincronizar_suscripcion()


def quitar_topico(topico):
    if topico not in estado.mis_topicos:
        imprimir_seguro(f"[INFO] No estás suscrito a '{topico}'.")
        return
    estado.mis_topicos.discard(topico)
    if not estado.mis_topicos:
        estado.mis_topicos.add(TOPICO_POR_DEFECTO)
    if estado.topico_publicacion == topico:
        estado.topico_publicacion = next(iter(estado.mis_topicos))
    sincronizar_suscripcion()


def manejar_comando_topico(argumento):
    if not argumento:
        imprimir_seguro("[ERROR] Uso: /topico <nombre>   (cambia exclusivo)\n"
                        "        /topico +<nombre>  (se agrega sin dejar los demás)\n"
                        "        /topico -<nombre>  (se sale de ese tópico)")
        return
    modo = argumento[0] if argumento[0] in "+-" else ""
    nombre = limpiar_nombre(argumento[len(modo):])
    if not nombre:
        imprimir_seguro(f"[ERROR] Debes indicar un nombre después de '{modo}', ej: /topico {modo}deportes")
        return
    if modo == "+":
        agregar_topico(nombre)
    elif modo == "-":
        quitar_topico(nombre)
    else:
        cambiar_topico_exclusivo(nombre)
    mostrar_mis_topicos()


def mostrar_mis_topicos():
    imprimir_seguro(f"[INFO] Suscrito a: {', '.join(sorted(estado.mis_topicos))} "
                    f"| Publicando en: '{estado.topico_publicacion}'")
