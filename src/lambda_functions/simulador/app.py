import json
import time
import requests
import os
from datetime import datetime
import random

# Variables de entorno configurables en la Lambda
API_URL = os.environ.get("API_URL")
API_KEY = os.environ.get("API_KEY")
BUS_ID = os.environ.get("BUS_ID", "BUS_101")

# Ruta Portal Norte → Héroes
RUTA = [
    (4.7644, -74.0321),  # Portal Norte
    (4.7572, -74.0338),
    (4.7321, -74.0458),
    (4.7088, -74.0501),
    (4.6784, -74.0563),
    (4.6486, -74.0643)   # Héroes
]

def generar_estado_y_velocidad():
    """Simula un estado y velocidad de bus aleatorios."""
    estados = ["EN_RUTA", "EN_PARADA", "DETENIDO"]
    estado = random.choices(estados, weights=[0.7, 0.2, 0.1])[0]  # 70% en ruta
    if estado == "EN_RUTA":
        velocidad = round(random.uniform(20, 50), 1)
    elif estado == "EN_PARADA":
        velocidad = round(random.uniform(0, 5), 1)
    else:
        velocidad = 0.0
    return estado, velocidad

def enviar_coordenada(lat, lon):

    estado, velocidad = generar_estado_y_velocidad()

    payload = {
        "bus_id": BUS_ID,
        "lat": lat,
        "lon": lon,
        "speed": velocidad,
        "status": estado,
        "timestamp": datetime.utcnow().isoformat()
    }

    headers = {
        "Content-Type": "application/json",
        "x-api-key": API_KEY
    }

    try:
        response = requests.post(API_URL, headers=headers, json=payload)
        print(f" Enviando coordenada: {payload}")
        print(f" Respuesta: {response.status_code} - {response.text}")
    except Exception as e:
        print(f" Error al enviar coordenada: {e}")

def lambda_handler(event, context):
    print("=" * 70)
    print(" Simulador TransMilenio ejecutado")
    print(f"Bus: {BUS_ID} | Fecha: {datetime.utcnow().isoformat()}")
    print("=" * 70)

    # Envía una coordenada en cada ejecución (siguiente punto de la ruta)
    index = int(time.time()) % len(RUTA)
    lat, lon = RUTA[index]
    enviar_coordenada(lat, lon)

    return {
        "statusCode": 200,
        "body": json.dumps({"bus_id": BUS_ID, "lat": lat, "lon": lon})
    }
