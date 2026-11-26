"""Interpretación de lo que escribe el usuario: comandos, calculadora y mensajes de chat."""
import os
import re
import uuid

from . import estado
from .calculadora import EJEMPLOS_CALC, USO_CALCULADORA, es_expresion_valida
from .config import MAX_SOLICITUDES_LOTE, PREFIJO_CHAT, TOPICO_SOLICITUDES
from .kafka_util import publicar, vaciar
from .lotes import crear_lote
from .topicos import manejar_comando_topico, mostrar_mis_topicos
from .utilidades import ahora_ms, imprimir_seguro
from .vistas import mostrar_cluster, mostrar_menu_ayuda, mostrar_prioridades, mostrar_topicos


def publicar_mensaje(texto):
    topico = PREFIJO_CHAT + estado.topico_publicacion
    publicar(topico, {"id": estado.cid, "nick": estado.mi_nickname, "ts": ahora_ms(), "texto": texto})
    vaciar()


# ------------------------------------------------------------------ calculadora
def pedir_calculo(argumento):
    expresiones = interpretar_argumento_calculadora(argumento)
    if expresiones:
        enviar_lote(expresiones)


def interpretar_argumento_calculadora(argumento):
    if not argumento:
        imprimir_seguro(USO_CALCULADORA)
        return None
    if es_expresion_valida(argumento):
        return [argumento]

    m = re.match(r"^(\d+)(?:\s+(.+))?$", argumento)
    if not m:
        imprimir_seguro(f"[ERROR] Expresión no válida. Ejemplos: {EJEMPLOS_CALC}")
        return None
    cantidad = int(m.group(1))
    if not 1 <= cantidad <= MAX_SOLICITUDES_LOTE:
        imprimir_seguro(f"[ERROR] La cantidad debe estar entre 1 y {MAX_SOLICITUDES_LOTE}.")
        return None
    if m.group(2):
        expr = m.group(2).strip()
        if not es_expresion_valida(expr):
            imprimir_seguro(f"[ERROR] Expresión no válida: '{expr}'. Ejemplos: {EJEMPLOS_CALC}")
            return None
        return [expr] * cantidad
    return pedir_expresiones_interactivas(cantidad)


def pedir_expresiones_interactivas(cantidad):
    imprimir_seguro(f"[CALCULADORA] Escribe las {cantidad} operaciones. "
                    "Se enviarán todas juntas al final (ENTER vacío para cancelar).")
    expresiones = []
    while len(expresiones) < cantidad:
        try:
            expr = estado.session.prompt(f"  Operación {len(expresiones) + 1}/{cantidad}: ").strip()
        except (KeyboardInterrupt, EOFError):
            expr = ""
        if not expr:
            imprimir_seguro("[CALCULADORA] Envío cancelado.")
            return None
        if not es_expresion_valida(expr):
            imprimir_seguro(f"  [ERROR] Expresión no válida, escríbela de nuevo. Ejemplos: {EJEMPLOS_CALC}")
            continue
        expresiones.append(expr)
    return expresiones


def enviar_lote(expresiones):
    total = len(expresiones)
    lote_id = uuid.uuid4().hex[:6]
    crear_lote(lote_id, total)
    if total > 1:
        imprimir_seguro(f"[LOTE {lote_id}] Enviando {total} solicitudes simultáneas...")
    ts = ahora_ms()
    for i, expr in enumerate(expresiones, start=1):
        publicar(TOPICO_SOLICITUDES, {"id": f"{lote_id}.{i}", "destino": estado.cid,
                                      "nick": estado.mi_nickname, "ts": ts, "expr": expr},
                 key=estado.cid)
    vaciar()


# ------------------------------------------------------------------ despacho de comandos
def ejecutar_comando(texto):
    t = texto.lower()
    if t in ("/help", "/ayuda"):
        mostrar_menu_ayuda()
    elif t == "salir":
        os._exit(0)
    elif t == "topicos":
        mostrar_topicos()
    elif t == "/cluster":
        mostrar_cluster()
    elif t in ("/prioridades", "tabla"):
        mostrar_prioridades()
    elif t == "/topicos":
        mostrar_mis_topicos()
    else:
        comando, _, argumento = texto.partition(" ")
        comando, argumento = comando.lower(), argumento.strip()
        if comando == "/topico":
            manejar_comando_topico(argumento)
        elif comando == "/calculadora":
            pedir_calculo(argumento)
        else:
            publicar_mensaje(texto)
