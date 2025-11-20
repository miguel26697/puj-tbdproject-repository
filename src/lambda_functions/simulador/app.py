import json
import time
import requests
import os
from datetime import datetime
from psycopg2.extras import RealDictCursor
import random
import boto3
import psycopg2
import decimal
# Cache de estaciones
cached_estaciones = None
last_load = None

# Variables de entorno configurables en la Lambda
API_URL = os.environ.get("API_URL")
API_KEY = os.environ.get("API_KEY")

region_name = os.environ.get("REGION_NAME", "us-east-1")

def decimal_to_float(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

def get_secret(secret_name: str, region_name: str):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])

    return secret

def connect_aurora(secret):
    return psycopg2.connect(
        host=secret["host"],
        user=secret["username"],
        password=secret["password"],
        database=secret.get("dbname", "puj_tranmilenio_db"),
        port=int(secret.get("port", 3306)),
        connect_timeout=5
    )

def get_estaciones_aurora(ruta):
    global cached_estaciones, last_load

    # Si no hay cache o han pasado más de 3600 segundos (1 hora)
    if cached_estaciones is None or (datetime.utcnow() - last_load).seconds > 3600:
        print(" Cargando estaciones desde Aurora PostgreSQL...")
        # Obtener credenciales del secret manager
        secret_aurora = get_secret("puj-aurora-secrets", region_name)

        # Establecer conexión
        conn = connect_aurora(secret_aurora)

        # Cursor tipo diccionario para devolver filas como {columna: valor}
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                    SELECT
                    e.* 
                    FROM transmi.ruta r
                    JOIN transmi.estacion_por_ruta epr
                        ON epr.ruta_id = r.id
                    JOIN transmi.estacion e
                        ON e.id = epr.estacion_id
                    where r.codigo = '""" + ruta + """' and epr.parada is true
                    ORDER BY r.codigo, epr.orden;
            """)
            cached_estaciones = cursor.fetchall()

        conn.close()
        last_load = datetime.utcnow()
        print(f" Estaciones cargadas y cacheadas ({len(cached_estaciones)} registros).")
    else:
        print(" Devolviendo estaciones desde cache.")

    return cached_estaciones

# -------------------- INTERPOLACIÓN --------------------
def interpolar_coordenadas(lat1, lon1, lat2, lon2, fraccion):
    """Devuelve un punto intermedio entre dos coordenadas (lat/lon)."""

    lat = lat1 + (lat2 - lat1) * fraccion
    lon = lon1 + (lon2 - lon1) * fraccion
    return lat, lon

def definir_escenario(event):
    """
    Determina el escenario en función del evento recibido.
    Si el evento incluye 'escenario', lo usa directamente.
    Si no, aplica una lógica por defecto (aleatoria).
    """
    if event and "escenario" in event:
        escenario = event["escenario"].upper()
        print(f" Escenario recibido desde el evento: {escenario}")
        return escenario

    # Si no se envía escenario explícito, usar uno aleatorio
    escenarios = ["HORA_VALLE", "HORA_PICO", "BUS_VARADO"]
    escenario = random.choices(escenarios, weights=[0.6, 0.3, 0.1])[0]
    print(f" Escenario aleatorio elegido: {escenario}")
    return escenario

def generar_estado_y_velocidad(escenario, fraccion):
    if escenario == "BUS_VARADO":
        return "DETENIDO", 0.0

    # En estación (inicio o final del tramo)
    if fraccion <= 0.05 or fraccion >= 0.95:
        estado = "EN_PARADA"
        velocidad = round(random.uniform(0, 5), 1)
        return estado, velocidad

    estado = "EN_RUTA"
    if escenario == "HORA_PICO":
        velocidad = round(random.uniform(5, 25), 1)
    else:  # HORA_VALLE
        velocidad = round(random.uniform(25, 50), 1)

    return estado, velocidad
    
def enviar_coordenada(lat, lon, estado, velocidad, BUS_ID, RUTA,
    escenario, direction, est_a, est_b, fraccion,
    steps_per_segment, current_step,):

    payload = {
        "bus_id": BUS_ID,
        "ruta": RUTA,
        "timestamp": datetime.utcnow().isoformat(),

        # Formato GEOJSON para MongoDB
        "location": {
            "type": "Point",
            "coordinates": [lon, lat]   # ¡Mongo usa [lon, lat]!
        },

        "estado": estado,
        "velocidad_kmh": velocidad,
        "escenario": escenario,
        "direccion": direction,

        "tramo": {
            "inicio": est_a["nombre"],
            "fin": est_b["nombre"],
            "fraccion": round(fraccion, 4)
        },

        "metrics_runtime": {
            "steps_per_segment": steps_per_segment,
            "current_step": current_step,
            "steps_remaining": max(steps_per_segment - current_step, 0)
        }
    }
    
    headers = {"Content-Type": "application/json", "x-api-key": API_KEY}

    try:
        response = requests.post(API_URL, headers=headers, json=payload)
        print(f" Enviando: {payload}")
        print(f" Respuesta: {response.status_code} - {response.text}")
    except Exception as e:
        print(f" Error al enviar coordenada: {e}")

def lambda_handler(event, context):
    global current_segment, current_step, direction, last_fraccion,RUTA

    print("=" * 80)
    BUS_ID = event.get("bus_id", "BUS_101")
    RUTAS = event.get("rutas")
    DIRECCION = event.get("direccion", "IDA")
    print(f" Simulador TransMilenio ejecutado | Bus: {BUS_ID}")
    print("=" * 80)
    print(RUTAS[0])
    # Cargar estaciones (desde cache o Aurora)
    estaciones = get_estaciones_aurora(RUTAS[0])
    print(estaciones)

    if len(estaciones) < 2:
        return {"statusCode": 500, "body": json.dumps({"error": "Faltan estaciones"})}

    # ================================
    #  Determinar escenario y pasos
    # ================================
    escenario = definir_escenario(event)


    # Configurar pasos por tramo según escenario
    if escenario == "HORA_VALLE":
        steps_per_segment = 10     # rápido
    elif escenario == "HORA_PICO":
        steps_per_segment = 20    # lento
    else:
        steps_per_segment = 1     # varado

    total_segments = len(estaciones) - 1  # solo los tramos (n-1)

    # ================================
    # Inicializar variables globales
    # ================================
    if "current_segment" not in globals():
        current_segment = 0
        current_step = 0
        direction = DIRECCION
        RUTA = RUTAS[0]
        last_fraccion = 0.0
        print(" Inicializando simulador (primera ejecución).")

    # ================================
    # Lógica de movimiento paso a paso
    # ================================
    # Avanza solo si no está varado

    if escenario != "BUS_VARADO":
        current_step += 1
        # Si completó todos los pasos, cambia al siguiente tramo
        if current_step > steps_per_segment:
            current_step = 0
            if direction == "IDA":
                current_segment += 1
                # Si llegó al final, cambia de dirección
                if current_segment >= total_segments:
                    current_segment = total_segments - 1
                    direction = "VUELTA" 
                    RUTA = RUTAS[1]
                    
            else:  # VUELTA
                current_segment -= 1
                # Si regresó al inicio, cambia de dirección
                if current_segment < 0:
                    current_segment = 0
                    direction = "IDA"
                    RUTA = RUTAS[0]

    # ================================
    #  Cálculo de fracción dentro del tramo
    # ================================
    fraccion = current_step / float(steps_per_segment)
    if direction == "VUELTA":
        fraccion = 1 - fraccion
    last_fraccion = fraccion

    # Calcular estado y velocidad basados en escenario y progreso
    estado, velocidad = generar_estado_y_velocidad(escenario, fraccion)

    # ================================
    #  Interpolación de coordenadas
    # ================================
    est_a = estaciones[current_segment]
    est_b = estaciones[current_segment + 1]
    lat_a, lon_a = decimal_to_float(est_a["latitud"]), decimal_to_float(est_a["longitud"])
    lat_b, lon_b = decimal_to_float(est_b["latitud"]), decimal_to_float(est_b["longitud"])

    lat, lon = interpolar_coordenadas(lat_a, lon_a, lat_b, lon_b, fraccion)

    # ================================
    #  Enviar coordenada simulada
    # ================================

    enviar_coordenada(
    lat, lon, estado, velocidad,
    BUS_ID, RUTA,
    escenario, direction,
    est_a, est_b, fraccion,
    steps_per_segment, current_step
    )

    # ================================
    #  Logs y respuesta
    # ================================
    print(f"Bus: {BUS_ID}")
    print(f"Ruta: {RUTA}")
    print(f"Tramo: {est_a['nombre']} → {est_b['nombre']}")
    print(f"Dirección: {direction}")
    print(f"Progreso: {fraccion:.2f} ({current_step}/{steps_per_segment})")
    print(f"Estado: {estado} | Velocidad: {velocidad} km/h")
    print("=" * 80)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "bus_id": BUS_ID,
            "ruta": RUTA,
            "escenario": escenario,
            "direccion": direction,
            "tramo": f"{est_a['nombre']} → {est_b['nombre']}",
            "estado": estado,
            "velocidad": velocidad,
            "posicion_relativa": round(fraccion, 2),
            "paso": current_step,
            "tramo_index": current_segment
        }, default=decimal_to_float)
    }
