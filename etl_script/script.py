
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
df_mongo = glueContext.create_dynamic_frame.from_catalog(
    database="db_transmilenio_glue",     
    table_name="glue_transmilenio_locations",         
    transformation_ctx="df_mongo",
    additional_options = {"database": "transmilenio", "collection": "locations"}
)

df_mongo.show()

spark_df = df_mongo.toDF()
spark_df.show()

spark_df.count()
from pyspark.sql.functions import (
    col,
    round as spark_round,
    to_timestamp,
    hour,
    dayofweek,
    to_date
)


coords_col = "location.coordinates"

spark_df_full = (
    spark_df
    .withColumn("lon", spark_round(col(coords_col)[0].cast("double"), 6))
    .withColumn("lat", spark_round(col(coords_col)[1].cast("double"), 6))
)

# ============================
# 2. NUMÉRICOS
# ============================

spark_df_full = (
    spark_df_full
    .withColumn("velocidad_kmh_num",
                spark_round(col("velocidad_kmh").cast("double"), 2))
    .withColumn("fraccion_num",
                spark_round(col("tramo.fraccion").cast("double"), 3))
    .withColumn("steps_per_segment_num",
                col("metrics_runtime.steps_per_segment").cast("int"))
    .withColumn("current_step_num",
                col("metrics_runtime.current_step").cast("int"))
    .withColumn("steps_remaining_num",
                col("metrics_runtime.steps_remaining").cast("int"))
)

# ============================
# 3. TIEMPOS
# ============================

spark_df_full = (
    spark_df_full
    .withColumn("ts_evento",  to_timestamp(col("timestamp")))
    .withColumn("ts_ingesta", to_timestamp(col("ingested_at")))
    .withColumn("latencia_seg",
                (col("ts_ingesta").cast("long") - col("ts_evento").cast("long")))
    .withColumn("fecha",      to_date(col("ts_evento")))
    .withColumn("hora",       hour(col("ts_evento")))
    .withColumn("dia_semana", dayofweek(col("ts_evento")))
)

# ============================
# 4. MOSTRAR ORDENADO
# ============================

spark_df_full.select(
    "bus_id",
    "ruta",
    "escenario",
    "estado",
    "ts_evento",
    "ts_ingesta",
    "latencia_seg",
    "fecha",
    "hora",
    "dia_semana",
    "lon",
    "lat",
    "velocidad_kmh_num",
    "fraccion_num",
    col("tramo.inicio").alias("tramo_inicio"),
    col("tramo.fin").alias("tramo_fin"),
    "steps_per_segment_num",
    "current_step_num",
    "steps_remaining_num"
).orderBy(col("timestamp").asc()) \
 .show(truncate=False)

spark_df_full.printSchema()
spark_df_full.show()


# DF final con solo las columnas que irán a Redshift
df_final = spark_df_full.select(
    col("bus_id"),
    col("ruta"),
    col("escenario"),
    col("estado"),
    col("ts_evento"),
    col("ts_ingesta"),
    col("latencia_seg"),
    col("fecha"),
    col("hora"),
    col("dia_semana"),
    col("lon"),
    col("lat"),
    col("velocidad_kmh_num"),
    col("fraccion_num"),
    col("tramo.inicio").alias("tramo_inicio"),
    col("tramo.fin").alias("tramo_fin"),
    col("steps_per_segment_num"),
    col("current_step_num"),
    col("steps_remaining_num")
)

# Ruta en S3 (ajusta bucket y prefijo)
output_path = "s3://puj-transmilenio-bucket/redshift/fact_bus_position/"

# Escribir en Parquet SIN particionar por fecha
df_final.write \
    .mode("overwrite") \
    .partitionBy("fecha")\
    .parquet(output_path)

df_final.printSchema()
print(len(df_final.columns))
print(df_final.columns)

df = spark.read.parquet("s3://puj-transmilenio-bucket/redshift/fact_bus_position/")
df.printSchema()

df.show()


df = DynamicFrame.fromDF(df, glueContext, "df")

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = df,
    catalog_connection = "puj_redshift_connection_db",
    connection_options = {
        "database": "transmi_db",
        "dbtable": "transmi.fact_bus_position_glue",
        "preactions": "truncate table transmi.fact_bus_position_glue",   # Puedes poner TRUNCATE aquí
        "postactions": ""   # Acciones después de insertar
    },
    redshift_tmp_dir = "s3://puj-transmilenio-bucket/redshift/temp/"
)

job.commit()