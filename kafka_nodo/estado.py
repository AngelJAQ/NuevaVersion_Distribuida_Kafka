"""Estado del cliente (se accede como `estado.cid`, `estado.mis_topicos`, etc.)."""
import uuid

from .config import TOPICO_POR_DEFECTO
from .prioridades import TablaRouters

cid = uuid.uuid4().hex[:8]            # identificador único de este cliente
mi_nickname = "Anónimo"
mis_topicos = {TOPICO_POR_DEFECTO}    # nombres cortos (sin el prefijo 'chat.')
topico_publicacion = TOPICO_POR_DEFECTO
escucha = None                        # objeto Escucha (consumidor del cliente)
session = None                        # PromptSession
tabla_routers = TablaRouters()      # latidos de los routers (tabla de prioridades)
