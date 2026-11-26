"""CLIENTE: consumidor en un hilo. Se suscribe a varios tópicos y puede cambiarlos en caliente.

Usa assign() en vez de subscribe(): cada cliente lee TODOS los mensajes de sus tópicos
(difusión), sin esperas de rebalanceo, y empieza desde el final (solo mensajes nuevos).
"""
import threading
import time

from confluent_kafka import KafkaError, KafkaException, TopicPartition, OFFSET_END

from .kafka_util import crear_consumer, particiones_de
from .utilidades import imprimir_seguro


class Escucha:
    def __init__(self, group_id, manejador):
        self.consumer = crear_consumer(group_id, "latest")
        self.manejador = manejador
        self.topicos = set()
        self.cambio = False
        self.lock = threading.Lock()
        threading.Thread(target=self._bucle, daemon=True).start()

    def fijar(self, topicos):
        """Define la lista completa de tópicos a escuchar (el hilo la aplica enseguida)."""
        with self.lock:
            self.topicos = set(topicos)
            self.cambio = True

    def _reasignar(self):
        with self.lock:
            deseados = set(self.topicos)
            self.cambio = False
        actuales = {(tp.topic, tp.partition) for tp in self.consumer.assignment()}
        nuevos = []
        for t in sorted(deseados):
            for p in particiones_de(self.consumer, t):
                tp = TopicPartition(t, p)
                tp.offset = OFFSET_END
                if (t, p) in actuales:          # ya lo escuchaba: conservar la posición
                    pos = self.consumer.position([TopicPartition(t, p)])[0].offset
                    if pos >= 0:
                        tp.offset = pos
                nuevos.append(tp)
        self.consumer.assign(nuevos)

    def _bucle(self):
        while True:
            if self.cambio:
                try:
                    self._reasignar()
                except KafkaException as e:
                    imprimir_seguro(f"[ERROR] No se pudo cambiar la suscripción: {e}")
                    self.cambio = True
                    time.sleep(1)
            msg = self.consumer.poll(0.3)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() not in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    imprimir_seguro(f"[ERROR] Kafka: {msg.error()}")
                continue
            try:
                self.manejador(msg)
            except Exception as e:
                imprimir_seguro(f"[ERROR] Mensaje ilegible en '{msg.topic()}': {e}")
