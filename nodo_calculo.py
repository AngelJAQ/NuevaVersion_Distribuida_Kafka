"""Nodo de cálculo sin consola (solo resuelve operaciones).

  python nodo_calculo.py suma resta --nick Ana
  python nodo_calculo.py todos
"""
import argparse
import time

from kafka_nodo.config import ROLES_NODO
from kafka_nodo.kafka_util import verificar_kafka
from kafka_nodo.trabajador import iniciar_trabajador

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("roles", nargs="+", choices=ROLES_NODO + ["todos"])
    ap.add_argument("--nick", default="nodo")
    a = ap.parse_args()
    if verificar_kafka():
        iniciar_trabajador(ROLES_NODO if "todos" in a.roles else a.roles, a.nick)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
