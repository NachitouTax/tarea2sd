import json
from kafka import KafkaConsumer
import time

# Configurar el consumidor de Kafka
consumer = KafkaConsumer('topic_pedidos',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Definir las constantes de tiempo
TIEMPO_PROCESAMIENTO = 5  # Ajustar según sea necesario
TIEMPO_PREPARACION = 10  # Ajustar según sea necesario
TIEMPO_ENTREGA = 15  # Ajustar según sea necesario

# Función para enviar notificaciones
def enviar_notificacion(pedido):
    # Implementar la lógica de notificación aquí
    pass

# Función para procesar un pedido
def procesar_pedido(pedido):
    # Actualizar el estado del pedido
    pedido['estado'] = 'recibido'
    enviar_notificacion(pedido)
    time.sleep(TIEMPO_PROCESAMIENTO)
    
    pedido['estado'] = 'preparando'
    enviar_notificacion(pedido)
    time.sleep(TIEMPO_PREPARACION)
    
    pedido['estado'] = 'entregando'
    enviar_notificacion(pedido)
    time.sleep(TIEMPO_ENTREGA)
    
    pedido['estado'] = 'finalizado'
    enviar_notificacion(pedido)

# Bucle principal para consumir pedidos
for message in consumer:
    pedido = message.value
    procesar_pedido(pedido)