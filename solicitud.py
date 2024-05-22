from flask import Flask, request, jsonify 
from kafka import KafkaProducer
import json
import uuid

# Crear una instancia de Flask
app = Flask(__name__)

# Configurar el productor de Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Endpoint para recibir solicitudes de pedidos
@app.route('/solicitar_pedido', methods=['POST'])
def solicitar_pedido():
    # Obtener los datos del pedido desde la solicitud
    datos_pedido = request.json
    
    # Generar un ID único para el pedido
    datos_pedido['id'] = str(uuid.uuid4())
    
    # Enviar el pedido al topic de Kafka
    producer.send('topic_pedidos', datos_pedido)
    
    return jsonify({'mensaje': 'Pedido recibido'}), 200