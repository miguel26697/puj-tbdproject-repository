import pymongo
import json
import os
import boto3
from datetime import datetime
import socket
import pymongo
import sys
import traceback
from pymongo.server_api import ServerApi

secrets_client = boto3.client('secretsmanager')
region_name = os.environ.get("REGION_NAME")
CA_CERT_PATH = os.environ.get("CA_CERT_PATH")


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
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])

    return secret


def connect_to_atlas(secret):

    username = secret["username"]
    password = secret["password"]
    cluster = secret["cluster"]
    app_name = secret.get("appName", "myApp")

    uri = (
        f"mongodb+srv://{username}:{password}@{cluster}/"
        f"?retryWrites=true&w=majority&appName={app_name}"
    )
    # Create a new client and connect to the server
    client = pymongo.MongoClient(uri, 
            server_api=ServerApi('1'),
            serverSelectionTimeoutMS=10000
        )
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client

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
        lat = body.get("location", {}).get("coordinates", [None, None])[1] or body.get("lat")
        lon = body.get("location", {}).get("coordinates", [None, None])[0] or body.get("lon")

        timestamp = body.get("timestamp")
        velocidad = body.get("velocidad_kmh")
        estado = body.get("estado")
        escenario = body.get("escenario")
        ruta = body.get("ruta")
        direccion = body.get("direccion")

        # Subdocumento de tramo
        tramo = body.get("tramo", {})
        inicio_tramo = tramo.get("inicio")
        fin_tramo = tramo.get("fin")
        fraccion = tramo.get("fraccion")

        # Métricas internas
        metrics_runtime = body.get("metrics_runtime", {})
        steps_per_segment = metrics_runtime.get("steps_per_segment")
        current_step = metrics_runtime.get("current_step")
        steps_remaining = metrics_runtime.get("steps_remaining")



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

        #  ------------------------- 2 Conectar Atlas-------------------------------

        secret_mongo = get_secret("puj-mongoDB-secrets", region_name)
        client = connect_to_atlas(secret_mongo)

        databases = client.list_database_names()
        print(" Bases de datos disponibles:", databases)

        db = client["transmilenio"]
        collection = db["locations"]
        col_last = db["last_positions"]
        


        # Crear índice 2dsphere si no existe
        indexes = collection.index_information()

        if "location_2dsphere" not in indexes:
            collection.create_index([("location", "2dsphere")])
            print("-- Índice 2dsphere creado en 'location'")

        indexes_last = col_last.index_information()

        if "location_2dsphere" not in indexes_last:
            col_last.create_index([("location", "2dsphere")])
            print("-- Índice 2dsphere creado en 'last_positions'")

        try:
            timestamp_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except:
            timestamp_dt = datetime.utcnow()
    
        # Índice TTL (si no existe)
        if "timestamp_ttl" not in indexes:
            # 86400 segundos = 24 horas
            collection.create_index(
                [("timestamp", 1)],
                expireAfterSeconds=86400,
                name="timestamp_ttl"
            )
            print("Índice TTL creado: documentos expirarán después de 24h")

        # -------------------- 3️ Insertar coordenada con velocidad y estado ----------------------
        document = {
            "bus_id": bus_id,
            "ruta": ruta,
            "escenario": escenario,
            "estado": estado,
            "direccion": direccion,
            "timestamp": timestamp_dt,
            "ingested_at": datetime.utcnow(),

            "location": {
                "type": "Point",
                "coordinates": [float(lon), float(lat)]
            },

            "velocidad_kmh": float(velocidad) if velocidad is not None else None,
            "tramo": tramo,
            "metrics_runtime": metrics_runtime
        }

        # ------------------- Insert EN Coleccion Historica -------------------|

        result = collection.insert_one(document)
        print(f" Documento insertado con ID en historico: {result.inserted_id}")

        document_last = {
            "bus_id": bus_id,
            "ruta": ruta,
            "escenario": escenario,
            "estado": estado,
            "direccion": direccion,
            "timestamp": timestamp_dt,
            "ingested_at": datetime.utcnow(),

            "location": {
                "type": "Point",
                "coordinates": [float(lon), float(lat)]
            },

            "velocidad_kmh": float(velocidad) if velocidad is not None else None,
            "tramo": tramo,
            "metrics_runtime": metrics_runtime
        }


        # ------------------- UPSERT EN Coleccion last_positions -------------------|
        col_last.update_one(
            {"bus_id": bus_id},
            {"$set": document_last},    # guarda la última posición
            upsert=True
        )
        print(f"Última posición actualizada para {bus_id}")

        # -------------------- 4️ Respuesta final ----------------------
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Coordenada almacenada correctamente",
                "inserted_id": str(result.inserted_id),
                "bus_id": bus_id,
                "ruta": ruta,
                "timestamp": timestamp,  
                "ingested_at": datetime.utcnow().isoformat(),

                "location": document["location"],

                "estado": estado,
                "velocidad_kmh": velocidad,
                "escenario": escenario,
                "direccion": direccion,

                "tramo": document["tramo"],
                "metrics_runtime": document["metrics_runtime"]
            })
        }

        # -------------------- 5️ Cerrar conexión ----------------------
        client.close()
        
    except Exception as e:
        print(" Error en lambda_handler:", e)
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "type": str(type(e).__name__)})
        }