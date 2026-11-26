"""Vistas en consola: tópicos del cluster y menú de ayuda."""
from . import estado
from .config import PREFIJO_CALC, PREFIJO_CHAT, ROLES_NODO
from .kafka_util import estado_cluster, listar_topicos, nodos_por_rol
from .utilidades import imprimir_seguro


def mostrar_topicos():
    """IDENTIFICAR TÓPICOS: muestra lo que realmente existe en Kafka."""
    lineas = ["\n================ TÓPICOS EN KAFKA ================",
              f"  [Yo] '{estado.mi_nickname}'",
              "  --- Tópicos de chat ---"]
    chats = listar_topicos(PREFIJO_CHAT)
    for t in chats:
        corto = t[len(PREFIJO_CHAT):]
        marca = "  (suscrito)" if corto in estado.mis_topicos else ""
        lineas.append(f"    {corto}{marca}")
    if not chats:
        lineas.append("    (no hay tópicos de chat todavía)")

    lineas.append("  --- Roles de la calculadora (un tópico por rol) ---")
    existentes = set(listar_topicos(PREFIJO_CALC))
    nodos = nodos_por_rol()
    for rol in ROLES_NODO:
        t = PREFIJO_CALC + rol
        if t not in existentes:
            detalle = "(tópico aún no creado)"
        elif nodos is None:
            detalle = "(nodos activos: no disponible con esta versión de la librería)"
        elif nodos[rol]:
            detalle = "nodos activos: " + ", ".join(nodos[rol])
        else:
            detalle = "SIN NODO (las solicitudes esperarán)"
        lineas.append(f"    {rol:<15} -> {t:<20} {detalle}")
    lineas.append("==================================================\n")
    imprimir_seguro("\n".join(lineas))


def mostrar_cluster():
    """Muestra qué brokers están vivos y quién es el líder de cada tópico (para ver el relevo)."""
    vivos, caidos, topicos = estado_cluster()
    total = len(vivos) + len(caidos)
    orden = next((t[0]["replicas"] for n, t in sorted(topicos.items()) if t and t[0]["replicas"]), [])
    lineas = ["\n================ CLUSTER KAFKA ================",
              f"  Brokers activos ({len(vivos)}/{total}): " +
              ", ".join(f"{i} ({d})" for i, d in sorted(vivos.items())),
              "  Brokers caídos: " + (", ".join(map(str, caidos)) if caidos else "ninguno"),
              "  Orden de relevo (prioridad): " + " > ".join(f"broker {b}" for b in orden),
              "  --- Líder de cada partición y réplicas sincronizadas ---"]
    for nombre in sorted(topicos):
        partes = topicos[nombre]
        lideres = ", ".join(str(p["lider"]) if p["lider"] >= 0 else "SIN LÍDER" for p in partes)
        minimo = min(len(p["isr"]) for p in partes)
        copias = max(len(p["replicas"]) for p in partes)
        lineas.append(f"    {nombre:<22} líder(es): {lideres:<10} | sincronizadas: {minimo}/{copias}")
    lideres = {p["lider"] for partes in topicos.values() for p in partes}
    if orden and lideres == {orden[0]}:
        lineas.append(f"  >> Todos los tópicos los lidera el broker {orden[0]} (prioridad #1).")
    elif orden and lideres:
        ids = ", ".join(str(i) for i in sorted(lideres))
        if orden[0] in vivos:
            lineas.append(f"  >> El broker prioritario ({orden[0]}) ya volvió y está recuperando el liderazgo "
                          f"(ahora lidera(n): {ids}).")
        else:
            lineas.append(f"  >> RELEVO ACTIVO: el broker prioritario ({orden[0]}) está caído; "
                          f"lidera(n) el/los broker(s) {ids}.")
    lineas.append("===============================================\n")
    imprimir_seguro("\n".join(lineas))


def mostrar_prioridades():
    """Tabla de prioridades de los routers (cola de sucesión), como en el programa original."""
    filas = estado.tabla_routers.filas()
    lineas = ["\n--- TABLA DE PRIORIDAD DE ROUTERS (ORDEN DE RELEVO) ---"]
    if not filas:
        lineas.append("  (no se ven routers; ¿hay alguno corriendo? 'python router.py --prioridad 1')")
    for prio, rid, host, est in filas:
        lineas.append(f"  [Prioridad #{prio}] -> Equipo: {host} ({rid})  {est}")
    lineas.append("---------------------------------------------------------\n")
    imprimir_seguro("\n".join(lineas))


def mostrar_menu_ayuda():
    imprimir_seguro("\n".join([
        "\n========================== MENÚ DE COMANDOS ==========================",
        "  /help o /ayuda          : Muestra este menú.",
        "  salir                   : Cierra la aplicación.",
        "",
        "--- TÓPICOS DE CHAT ---",
        "  /topicos                : Muestra los tópicos a los que estás suscrito.",
        "  /topico <nombre>        : Cambia la suscripción de forma exclusiva a ese tópico.",
        "  /topico +<nombre>       : Se suscribe a un tópico nuevo sin abandonar los actuales.",
        "  /topico -<nombre>       : Abandona un tópico.",
        "  Topicos                 : Lista los tópicos que existen en Kafka y los nodos por rol.",
        "  /cluster                : Muestra los brokers vivos/caídos, su orden de relevo y el líder.",
        "  /prioridades            : Tabla de prioridad de los routers (quién está activo y quién sigue).",
        "",
        "--- CALCULADORA DISTRIBUIDA ---",
        "  /calculadora <expresión>: Ej. /calculadora 5 + 3     /calculadora raiz 25",
        "  /calculadora 8*8+3/5-2  : Expresión compuesta (el router la divide en pasos).",
        "  /calculadora <cantidad> : Te pide esa cantidad de operaciones y las manda juntas.",
        "  /calculadora <cantidad> <expresión> : Manda la misma operación N veces. Ej. /calculadora 20 5 + 3",
        "=======================================================================\n",
    ]))
