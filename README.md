# Plataforma de Datos TransMilenio  
**Proyecto de ingestión geoespacial + almacenamiento + analítica en AWS**

Este repositorio contiene la infraestructura y el código del proyecto **TransMilenio**, desplegado mediante **AWS SAM (Serverless Application Model)**.  
La plataforma permite:

- Recibir coordenadas de buses en tiempo real (API Gateway + Lambda POST)
- Consultar posiciones recientes (API Gateway + Lambda GET)
- Almacenar datos geoespaciales en DocumentDB / MongoDB Atlas
- Ejecutar un ETL (Glue Crawler + Glue Job) hacia S3 y Redshift
- Analizar datos históricos mediante Redshift Serverless

---

##  Estructura del repositorio

/
├── ejecutar_simulador/
│   └── launch_simulador.py         # Ejecuta simulación de buses
│
├── etl_script/
│   └── script.py                   # Contenido del Glue Job (PySpark)
│
├── layers/
│   ├── pymongo-layer/              # Layer para Lambdas que usan Mongo Atlas
│   ├── psycopg2/                   # Layer PostgreSQL
│   ├── request-layer/              # Layer para requests
│   └── pyscopg2.zip
│
├── src/lambda_functions/
│   ├── bus-location-processor/     # Lambda POST para insertar posiciones
│   │   └── app.py
│   ├── get-positions/              # Lambda GET para consultar posiciones
│   │   └── app.py
│   └── simulador/                  # Lambda para control del simulador
│       └── app.py
│
├── web_app/
│   └── … archivos web
│
├── rds-combined-ca-bundle.pem      # Certificado para conexiones SSL
│
├── template_vpc.yaml               # VPC + Subnets + NAT + SG
├── template_redshift.yaml          # Redshift Serverless
├── template_apigateway.yaml        # API Gateway (POST, GET, CORS)
├── template_lambdas.yaml           # Lambdas + Layers + Roles
├── template_glue_jobs.yaml         # Crawler + ETL Job PySpark
├── template_aurora_db.yaml         # Aurora (estaciones, rutas)
│
└── README.md


## 1️ Prerrequisitos

- AWS CLI configurado (`aws configure`)
- AWS SAM CLI instalado  
- Python 3.13
- Cuenta AWS en `us-east-1`
- Permisos para:
  - Lambda
  - API Gateway
  - CloudFormation
  - IAM: PassRole
  - Glue (para ETL)
  - Redshift Serverless

## 2 Deploy primer despliegue
## VPC 

-subnets públicas y privadas
-NAT Gateway
-Internet Gateway
-Security groups para Redshift, Lambdas, Aurora

aws cloudformation deploy \
  --stack-name transmi-vpc \
  --template-file template_vpc.yaml \
  --capabilities CAPABILITY_NAMED_IAM


aws cloudformation deploy \
  --stack-name transmi-aurora \
  --template-file template_aurora_db.yaml \
  --capabilities CAPABILITY_NAMED_IAM


aws cloudformation deploy \
  --stack-name transmi-redshift \
  --template-file template_redshift.yaml \
  --capabilities CAPABILITY_NAMED_IAM

## 4. Despliegue de Lambdas + Layers (SAM)

sam build
sam deploy --guided

SAM despliega:

Lambda POST: /location
Lambda GET: /location
Lambda simulador

Roles + permisos
Layers: pymongo, psycopg2

SAM generará un archivo:

## 5. API Gateway (CloudFormation)

aws cloudformation deploy \
  --stack-name transmi-api \
  --template-file template_apigateway.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides LambdaFunctionArn=<ARN_LAMBDA_POST>


## 6. Desplegar ETL (Glue Crawler + Glue Job)

aws cloudformation deploy \
  --stack-name transmi-etl \
  --template-file template_glue_jobs.yaml \
  --capabilities CAPABILITY_NAMED_IAM


aws glue start-crawler --name puj-atlas-mongodb-to-aws
aws glue start-job-run --job-name puj-etl-transmilenio
