import pymongo
import json
import os
import boto3
from datetime import datetime

# --- CONFIGURACIÓN (leída desde las variables de entorno de la Lambda) ---
DB_HOST = os.environ['DOCDB_HOST']
DB_SECRET_ARN = os.environ['DOCDB_SECRET_ARN']
DB_NAME = os.environ['DB_NAME']
COLLECTION_NAME = os.environ['COLLECTION_NAME']

# El certificado debe estar en la misma carpeta que este script
CA_CERT_PATH = 'global-bundle.pem' 

# --- CLIENTES (se inicializan fuera del handler para reutilizar la conexión) ---
secrets_client = boto3.client('secretsmanager')
db_client = None

def get_db_credentials():
    """Obtiene las credenciales de DocumentDB desde Secrets Manager."""
    try:
        response = secrets_client.get_secret_value(SecretId=DB_SECRET_ARN)
        secret = json.loads(response['SecretString'])
        return secret['username'], secret['password']
    except Exception as e:
        print(f"Error al obtener el secreto de Secrets Manager: {e}")
        raise e

def get_db_client():
    """
    Inicializa y retorna el cliente de DocumentDB. 
    Reutiliza la conexión si ya existe (práctica recomendada en Lambda).
    """
    global db_client
    if db_client:
        try:
            # Validar si la conexión sigue viva
            db_client.admin.command('ismaster')
            return db_client
        except Exception:
            print("Conexión perdida, reconectando...")
            db_client = None # Forzar reconexión

    try:
        db_user, db_pass = get_db_credentials()
        
        print(f"Intentando conectar a {DB_HOST}...")
        client = pymongo.MongoClient(
            f"mongodb://{db_user}:{db_pass}@{DB_HOST}:27017/?tls=true&tlsCAFile={CA_CERT_PATH}&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false",
            serverSelectionTimeoutMS=5000 # Timeout de 5 segundos
        )
        
        # Forzar una conexión para validar credenciales y conectividad
        client.admin.command('ismaster')
        print("Conexión a DocumentDB exitosa.")
        db_client = client
        return db_client
    
    except Exception as e:
        print(f"Error crítico al conectar a DocumentDB: {e}")
        raise e

# --- HANDLER PRINCIPAL DE LAMBDA ---
def lambda_handler(event, context):
    try:
        # 1. Obtener el cliente de la base de datos
        client = get_db_client()
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # 2. Parsear los datos de la API Gateway
        # Se asume que la API Gateway usa integración Proxy Lambda
        data = json.loads(event.get('body', '{}'))
        bus_id = data.get('bus_id')
        if not bus_id:
            return {'statusCode': 400, 'body': json.dumps({'error': 'bus_id es requerido'})}

        # 3. Preparar el documento para DocumentDB
        document_to_upsert = {
            'route_id': data.get('route_id'),
            'last_seen': datetime.now().isoformat(),
            'location': {
                'type': 'Point',
                'coordinates': [data.get('longitude'), data.get('latitude')] # Formato GeoJSON [lon, lat]
            },
            'speed_kph': data.get('speed_kph', 0),
            'status': data.get('status', 'en_ruta')
        }

        # 4. Ejecutar la operación UPSERT (Actualiza si existe, Inserta si es nuevo)
        collection.update_one(
            {'_id': bus_id},  # El filtro (usamos el bus_id como el _id único)
            {'$set': document_to_upsert}, # Los datos a actualizar/insertar
            upsert=True  # La magia del UPSERT
        )

        # 5. Responder a API Gateway
        return {
            'statusCode': 200,
            'body': json.dumps({'message': f'Datos del bus {bus_id} actualizados'})
        }

    except Exception as e:
        print(f"Error en el handler: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Error interno del servidor', 'details': str(e)})}