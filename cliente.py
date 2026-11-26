"""Nodo con consola: chat por tópicos + calculadora. Opcionalmente también atiende roles.

Uso:
  python cliente.py                      # solo cliente
  python cliente.py --rol suma resta     # cliente que además resuelve sumas y restas
  python cliente.py --rol todos          # cliente que atiende los 7 roles
"""
import argparse
import os
import random
import sys
import threading

try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.patch_stdout import patch_stdout
except ImportError:
    print("[ERROR] Falta 'prompt_toolkit'. Instálalo con: pip install prompt_toolkit")
    sys.exit(1)

from kafka_nodo import estado
from kafka_nodo.comandos import ejecutar_comando
from kafka_nodo.config import KAFKA_SERVER, ROLES_NODO
from kafka_nodo.escucha import Escucha
from kafka_nodo.kafka_util import asegurar_calculadora, verificar_kafka
from kafka_nodo.lotes import vigilar_lotes
from kafka_nodo.recepcion import manejar_mensaje
from kafka_nodo.topicos import mostrar_mis_topicos, sincronizar_suscripcion
from kafka_nodo.trabajador import iniciar_trabajador
from kafka_nodo.utilidades import imprimir_seguro


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rol", nargs="*", default=[], choices=ROLES_NODO + ["todos"],
                    help="roles de calculadora que este nodo atiende")
    args = ap.parse_args()
    roles = ROLES_NODO if "todos" in args.rol else args.rol

    print(f"=== NODO DISTRIBUIDO CON KAFKA ({KAFKA_SERVER}) ===")
    if not verificar_kafka():
        sys.exit(1)

    estado.session = PromptSession()
    estado.mi_nickname = input("Ingresa tu Nickname / Apodo: ").strip() or f"Nodo_{random.randint(100, 999)}"
    print(f"[NICKNAME] Tu apodo configurado es: '{estado.mi_nickname}'")

    asegurar_calculadora()
    estado.escucha = Escucha(f"cliente-{estado.cid}", manejar_mensaje)
    sincronizar_suscripcion()
    threading.Thread(target=vigilar_lotes, daemon=True).start()
    if roles:
        iniciar_trabajador(roles, estado.mi_nickname)

    mostrar_mis_topicos()
    print("[INFO] Escribe '/help' o '/ayuda' para ver la lista de comandos disponibles.")

    with patch_stdout():
        while True:
            try:
                entrada = estado.session.prompt(f"[{estado.mi_nickname}]: ").strip()
            except (KeyboardInterrupt, EOFError):
                os._exit(0)
            if not entrada:
                continue
            try:
                ejecutar_comando(entrada)
            except Exception as e:
                imprimir_seguro(f"[ERROR] {e}")


if __name__ == "__main__":
    main()
