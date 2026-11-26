"""Constantes de configuración (versión Kafka)."""
import os

def _leer_servidores():
    """Brokers del cluster. Orden de búsqueda: variable KAFKA_SERVER, archivo kafka_servidores.txt."""
    env = os.environ.get("KAFKA_SERVER")
    if env:
        return env
    ruta = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "kafka_servidores.txt")
    try:
        with open(ruta, encoding="utf-8") as f:
            partes = [x.strip() for linea in f if not linea.strip().startswith("#")
                      for x in linea.split(",") if x.strip()]
        if partes:
            return ",".join(partes)
    except OSError:
        pass
    return "localhost:19092,localhost:19093,localhost:19094"


# Lista de brokers (uno por equipo). El cliente se conecta al primero que responda y, si ese
# cae, usa automáticamente los otros: eso es el RELEVO de brokers entre equipos.
KAFKA_SERVER = _leer_servidores()
REPLICACION = 3               # cada tópico se copia en 3 brokers (si hay menos vivos, usa los que haya)
ESPERA_BROKERS_S = 20         # al crear un tópico, espera hasta este tiempo a que estén los 3 brokers
                              # (así no queda creado con solo 2 réplicas por llegar un equipo tarde)

# ---- Tópicos de CHAT: uno por cada tema que cree el usuario (chat.general, chat.deportes...)
PREFIJO_CHAT = "chat."
TOPICO_POR_DEFECTO = "general"

# ---- Tópicos de CALCULADORA
#   cliente --> calc.solicitudes --> ROUTER --> calc.<rol> --> NODO --> calc.resultados --> ROUTER --> calc.respuestas --> cliente
PREFIJO_CALC = "calc."
TOPICO_SOLICITUDES = "calc.solicitudes"
TOPICO_RESULTADOS = "calc.resultados"
TOPICO_RESPUESTAS = "calc.respuestas"
TOPICO_LATIDOS = "sistema.routers"    # latidos de los routers (tabla de prioridades)
TOPICO_PLANES = "calc.planes"          # respaldo de expresiones compuestas (para el relevo del router)
TOPICO_CALCULADORA = "calculadora"   # solo para mostrar en pantalla (@calculadora)

ROLES_NODO = ["suma", "resta", "multiplicacion", "division", "potencia", "modulo", "raiz"]
PARTICIONES_CALC = 3          # con 3 particiones, hasta 3 nodos del mismo rol se reparten el trabajo

MAX_SOLICITUDES_LOTE = 100    # máximo de operaciones de golpe con /calculadora
TIEMPO_MAX_ESPERA_S = 30      # una solicitud más vieja que esto se descarta con error
TIEMPO_MAX_LOTE_S = 45        # el cliente deja de esperar un lote tras este tiempo

# ---- Tabla de prioridades de los ROUTERS (relevo entre equipos)
LATIDO_S = 1.0               # cada router avisa que sigue vivo cada segundo
TIMEOUT_ROUTER_S = 5.0       # sin latido durante este tiempo = router caído
GRACIA_ARRANQUE_S = 3.0      # al arrancar, un router escucha antes de decidir si toma el mando
