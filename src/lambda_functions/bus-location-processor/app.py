import pymongo
import json
import os
import boto3
from datetime import datetime
import socket
import pymongo
import sys
import traceback

secrets_client = boto3.client('secretsmanager')
secret_name = "puj-documentdb--cluster-secrets"
region_name = "us-east-1"
db_client = None
CA_CERT_PATH = "/opt/python/global-bundle.pem" 

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

def test_tcp_connection(host: str, port: int, timeout: int = 5):
    #Verifica la conectividad TCP al host:puerto

    try:
        socket.create_connection((host, port), timeout=timeout)
        print(f" Conexión TCP exitosa a {host}:{port}")
        return True
    except Exception as e:
        print(f" Error TCP al conectar con {host}:{port}: {e}, revise si el servidor esta encendido")
        return False

def connect_to_documentdb(secret: dict):
    """Establece conexión con DocumentDB usando pymongo"""

    try:
        username = secret["username"]
        password = secret["password"]
        host = secret["host"]
        port = secret.get("port", 27017)

        # URI recomendada por AWS para DocumentDB Serverless
        uri = (
            f"mongodb://{username}:{password}@{host}:{port}/"
            "?tls=true"
            f"&tlsCAFile={CA_CERT_PATH}"
            "&replicaSet=rs0"
            "&readPreference=secondaryPreferred"
            "&retryWrites=false"
        )

        print(f" Intentando conectar a {host}:{port} ...")

        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=10000)
        client.admin.command("ping")  # Prueba de conexión

        print("-- Conexión exitosa a DocumentDB Serverless con Pymongo")
        return client
    except Exception as e:
        print("-- Error crítico al conectar con DocumentDB por pymongo:")
        traceback.print_exc()
        raise

def is_within_bogota(lat: float, lon: float) -> bool:
    """Valida si las coordenadas están dentro de un rango aproximado de Bogotá"""
    # Bogotá: lat 4.45 a 4.85, lon -74.3 a -73.9
    return 4.45 <= lat <= 4.85 and -74.3 <= lon <= -73.9

# --- HANDLER PRINCIPAL DE LAMBDA ---
def lambda_handler(event, context):

    print("=" * 80)
    print("---------------  Lambda ejecutada - Ingesta de coordenadas TransMilenio ---------------------------- ")
    print("Evento recibido:", json.dumps(event))
    print("=" * 80)

    # Mostrar contenido del layer
    try:
        print("\n Archivos en /opt/python:", os.listdir("/opt/python"))
    except Exception:
        print("\n No se pudo listar /opt/python")

    try:
        # -------------------- 1️ Obtener cuerpo del POST (del API Gateway proxy)----------------------
        if "body" in event:
            body = json.loads(event["body"])
        else:
            body = event

        bus_id = body.get("bus_id")
        lat = body.get("lat")
        lon = body.get("lon")
        timestamp = body.get("timestamp", datetime.utcnow().isoformat())

        if not all([bus_id, lat, lon]):
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Campos requeridos: bus_id, lat, lon"})
            }

        if not is_within_bogota(float(lat), float(lon)):
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": "Coordenadas fuera del rango de Bogotá",
                    "lat": lat, "lon": lon
                })
            }

        #  ------------------------- 2 Obtener credenciales y conectar -------------------------------
        secret = get_secret(secret_name, region_name)
        host = secret["host"]
        port = secret.get("port", 27017)

        if not test_tcp_connection(host, port):
            return {
                "statusCode": 504,
                "body": json.dumps({"error": f"No hay conectividad TCP a {host}:{port}"}),
            }

        client = connect_to_documentdb(secret)

        databases = client.list_database_names()
        print(" Bases de datos disponibles:", databases)

        db = client["transmilenio"]
        collection = db["locations"]

        # Crear índice 2dsphere si no existe
        indexes = collection.index_information()
        if "location_2dsphere" not in indexes:
            collection.create_index([("location", "2dsphere")])
            print("-- Índice 2dsphere creado en 'location'")


        # 3 Insertar coordenada
        document = {
            "bus_id": bus_id,
            "location": {
                "type": "Point",
                "coordinates": [float(lon), float(lat)]
            },
            "timestamp": timestamp,
            "ingested_at": datetime.utcnow().isoformat()
        }


        result = collection.insert_one(document)
        print(f" Documento insertado con ID: {result.inserted_id}")

        # 4️ Respuesta exitosa
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Coordenada almacenada correctamente",
                "inserted_id": str(result.inserted_id)
            })
        }

    except Exception as e:
        print(" Error en lambda_handler:", e)
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "type": str(type(e).__name__)})
        }