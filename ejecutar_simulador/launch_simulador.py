import boto3
import json
import time

lambda_client = boto3.client("lambda", region_name="us-east-1")

payload = {
    "escenario": "HORA_VALLE",
    "bus_id": "T110",
    "rutas": ["J10", "B10"]
}

while True:
    print("Ejecutando simulación…")

    lambda_client.invoke(
        FunctionName="puj-simulador-transmilenio",
        InvocationType="Event",  # async (no espera resultado)
        Payload=json.dumps(payload)
    )

    time.sleep(5)
