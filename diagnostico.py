"""DIAGNÓSTICO DE RED: prueba, desde ESTE equipo, los puertos 9092 (clientes/brokers) y 9093 (cuórum)
de los 3 equipos. Ejecútalo en los 3 equipos y compara: así se ve quién NO alcanza a quién.

    docker compose --env-file equipo.pcN.env run --rm diagnostico
"""
import os
import socket
import time

PUERTOS = {9092: "clientes/brokers", 9093: "cuórum"}
EQUIPOS = {1: os.environ.get("IP_PC1", ""), 2: os.environ.get("IP_PC2", ""), 3: os.environ.get("IP_PC3", "")}
MI_IP = os.environ.get("MI_IP", "")
NODE_ID = os.environ.get("NODE_ID", "?")


def probar(ip, puerto):
    t = time.time()
    try:
        with socket.create_connection((ip, puerto), timeout=4):
            return True, f"{(time.time() - t) * 1000:.0f} ms"
    except OSError as e:
        return False, (str(e) or e.__class__.__name__)[:60]


print(f"\n== Diagnóstico desde el equipo NODE_ID={NODE_ID}  MI_IP={MI_IP} ==")
problemas = []

# 1) revisar el archivo de configuración
valores = [v for v in EQUIPOS.values()]
if any(not v for v in valores):
    problemas.append("Falta alguna IP_PCx en el archivo .env")
if any((" " in v or "#" in v) for v in valores) or (" " in MI_IP or "#" in MI_IP):
    problemas.append("Alguna IP trae espacios o comentarios pegados: en el .env pon los comentarios en línea APARTE")
if MI_IP != EQUIPOS.get(int(NODE_ID) if NODE_ID.isdigit() else 0):
    problemas.append(f"MI_IP ({MI_IP}) no coincide con IP_PC{NODE_ID} ({EQUIPOS.get(int(NODE_ID) if NODE_ID.isdigit() else 0)}): "
                     "revisa NODE_ID y MI_IP")
if len(set(valores)) < 3:
    problemas.append("Hay IPs repetidas entre PC1, PC2 y PC3")
if not all(v.startswith("25.") for v in valores if v):
    print("  [nota] No todas las IP empiezan por 25.x: ¿estás usando IP de Hamachi?")

# 2) probar conectividad
print()
for nid, ip in EQUIPOS.items():
    if not ip:
        continue
    for puerto, desc in PUERTOS.items():
        ok, det = probar(ip, puerto)
        marca = "OK   " if ok else "FALLA"
        yo = "  (este equipo)" if str(nid) == NODE_ID else ""
        print(f"  [{marca}] PC{nid} {ip}:{puerto} ({desc}) -> {det}{yo}")
        if not ok and str(nid) != NODE_ID:
            problemas.append(f"No se alcanza PC{nid} {ip}:{puerto}. Si el contenedor kafka de ese equipo está arriba, "
                             "es firewall o Hamachi (ejecuta preparar_windows.ps1 en ese equipo).")

print()
if problemas:
    print("PROBLEMAS DETECTADOS:")
    for p in dict.fromkeys(problemas):
        print("  -", p)
else:
    print("Red OK: este equipo alcanza a los 3 en 9092 y 9093.")
print("(Los puertos solo responden si el contenedor kafka de ese equipo está levantado.)\n")
