from flask import Flask, jsonify
from kafka import KafkaConsumer
import smtplib
import json

# Crear una instancia de Flask
app = Flask(__name__)

# Configurar el consumidor de Kafka
consumer = KafkaConsumer('topic_notificaciones',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Función para enviar notificaciones por correo
def enviar_correo(pedido):
    # Configurar el servidor SMTP
    smtp_server = smtplib.SMTP('smtp.gmail.com', 587)
    smtp_server.starttls()
    smtp_server.login('tu_correo@gmail.com', 'tu_contrasena')
    
    # Enviar el correo
    mensaje = f"Estado del pedido {pedido['id']}: {pedido['estado']}"
    smtp_server.sendmail('tu_correo@gmail.com', pedido['correo'], mensaje)
    
    smtp_server.quit()

# Endpoint para consultar el estado de un pedido
@app.route('/estado_pedido/<id>', methods=['GET'])
def estado_pedido(id):
    # Buscar el pedido en el topic de notificaciones
    for message in consumer:
        pedido = message.value
        if pedido['id'] == id:
            return jsonify(pedido)
    
    return jsonify({'mensaje': 'Pedido no encontrado'}), 404

# Bucle principal para consumir notificaciones
for message in consumer:
    pedido = message.value
    enviar_correo(pedido)