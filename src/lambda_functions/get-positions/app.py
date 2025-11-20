# Lambda: puj-lambda-getpositions
import pymongo, json, boto3, os,socket,psycopg2
from datetime import datetime
from psycopg2.extras import RealDictCursor
from pymongo.server_api import ServerApi
import decimal
from math import radians, sin, cos, sqrt, atan2
# Variables globales para cache
cached_estaciones = None
last_load = None

region_name = os.environ.get("REGION_NAME", "us-east-1")
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


def connect_aurora(secret):
    return psycopg2.connect(
        host=secret["host"],
        user=secret["username"],
        password=secret["password"],
        database=secret.get("dbname", "puj_tranmilenio_db"),
        port=int(secret.get("port", 3306)),
        connect_timeout=5
    )


def load_estaciones():
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
                SELECT id, nombre, latitud, longitud, tipo
                FROM transmi.estacion;
            """)
            cached_estaciones = cursor.fetchall()

        conn.close()
        last_load = datetime.utcnow()
        print(f" Estaciones cargadas y cacheadas ({len(cached_estaciones)} registros).")
    else:
        print(" Devolviendo estaciones desde cache.")

    return cached_estaciones

def json_serial(obj):
    """Soporte para datetime en json.dumps"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return str(obj)


def lambda_handler(event, context):
    try:
        print("Evento recibido:", json.dumps(event))
        params = event.get("queryStringParameters") or {}
        mode = params.get("mode")

        estaciones = load_estaciones()

        # PRIMERO: cargar estaciones si se solicitan
        if mode == "estaciones":
            return {
                "statusCode": 200,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"estaciones": estaciones},default = json_serial)
            }


        # Conexión a MongoDB Atlas
        # Mongo

        secret_mongo = get_secret("puj-mongoDB-secrets", region_name)
        client = connect_to_atlas(secret_mongo)
        db = client["transmilenio"]

        collection = db["locations"]         # histórico
        col_last = db["last_positions"]    # última posición

        # ============================
        # MODE: all
        # ============================
        if mode == "all":
            buses = list(col_last.find({}).sort("timestamp", -1))
            print("buses:", buses)
            return {
                "statusCode": 200,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"buses": buses, "estaciones": estaciones}, default=str)
            }

        # ============================
        # MODE: route
        # ============================
        if mode == "route":
            ruta = params["ruta"]
            buses = list(col_last.find({"ruta": ruta}).sort("timestamp", -1))
            return {
                "statusCode": 200,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"ruta": ruta, "buses": buses}, default=str)
            }


        # ============================
        # MODE: status
        # ============================
        if mode == "status":
            estado = params["estado"]
            buses = list(col_last.find({"estado": estado}).sort("timestamp", -1))
            return {
                "statusCode": 200,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"estado": estado, "buses": buses}, default=str)
            }

        # ============================
        # MODE: escenario
        # ============================
        if mode == "escenario":
            esc = params["escenario"]
            buses = list(col_last.find({"escenario": esc}).sort("timestamp", -1))
            return {
                "statusCode": 200,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"escenario": esc, "buses": buses}, default=str)
            }


        # ============================
        # MODE: near_buses → con distancia
        # ============================
        if mode == "near_buses":
            lat = float(params["lat"])
            lon = float(params["lon"])
            max_dist = float(params.get("maxDistance", 200))

            pipeline = [
                {
                    "$geoNear": {
                        "near": {"type": "Point", "coordinates": [lon, lat]},
                        "distanceField": "distancia_m",
                        "maxDistance": max_dist,
                        "spherical": True
                    }
                }
            ]

            buses = list(col_last.aggregate(pipeline))


            return {
                "statusCode": 200,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"buses": buses}, default=str)
            }


        # ============================
        # MODE: near_station
        # ============================
        if mode == "near_station":
            est_id = int(params["id"])
            estacion = next(e for e in estaciones if e["id"] == est_id)

            lon = float(estacion["longitud"])
            lat = float(estacion["latitud"])

            pipeline = [
                {
                    "$geoNear": {
                        "near": {"type": "Point", "coordinates": [lon, lat]},
                        "distanceField": "distancia_m",
                        "maxDistance": 20000,
                        "spherical": True
                    }
                }
            ]

            buses = list(col_last.aggregate(pipeline))

            return {
                "statusCode": 200,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"estacion": estacion, "buses": buses}, default=str)
            }

        # ============================
        # MODE: history
        # ============================
        if mode == "history":
            bus_id = params["bus_id"]
            limit = int(params.get("limit", 20))

            data = list(collection.find({"bus_id": bus_id}).sort("timestamp", -1).limit(limit))
            return {
                "statusCode": 200,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"bus_id": bus_id, "history": data}, default=str)
            }

        # --------------------------------------------------------------
        # MODE desconocido
        # --------------------------------------------------------------
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Modo no reconocido: {mode}"})
        }

    except Exception as e:
        print("ERROR:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }