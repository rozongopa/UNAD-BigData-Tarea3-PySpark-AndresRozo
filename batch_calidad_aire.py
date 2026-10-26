"""
UNIVERSIDAD NACIONAL ABIERTA Y A DISTANCIA - UNAD
Escuela de Ciencias Basicas, Tecnologia e Ingenieria (ECBTI)
Curso: Big Data (202016911) - Tarea 3: Procesamiento con Apache Spark
Estudiante 1: Andres Felipe Rozo Quevedo
Script: batch_calidad_aire.py

Descripcion:
Pipeline Batch en PySpark para limpieza, transformacion y agregacion 
de datos de material particulado (PM10 y PM2.5) de la red RMCAB Bogota.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round as sp_round, when, to_timestamp, desc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def main():
    # 1. Inicializacion de la sesion distribuida de Spark
    spark = SparkSession.builder \
        .appName("Procesamiento_Batch_CalidadAire_Bogota_AndresRozo") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    # Reduccion de logs informativos para mantener limpia la consola
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "="*70)
    print(" [1/5] SESION DE PYSPARK INICIADA CORRECTAMENTE")
    print("="*70 + "\n")

    # 2. Definicion formal del esquema estructurado (Catalyst Optimizer)
    esquema_rmcab = StructType([
        StructField("id_registro", IntegerType(), False),
        StructField("estacion", StringType(), False),
        StructField("fecha_hora", StringType(), False),
        StructField("pm10", DoubleType(), True),            # ug/m3
        StructField("pm25", DoubleType(), True),            # ug/m3
        StructField("temperatura", DoubleType(), True),     # Grados Celsius
        StructField("humedad_relativa", DoubleType(), True) # Porcentaje %
    ])

    # 3. Carga de datos crudos representativos de estaciones RMCAB
    # Nota: En produccion se carga con: spark.read.csv("datos_rmcab.csv", header=True, schema=esquema_rmcab)
    datos_fuente = [
        (101, "Carvajal-Sevillana", "2026-02-01 07:00:00", 78.4, 48.2, 11.5, 88.0),
        (102, "Carvajal-Sevillana", "2026-02-01 08:00:00", 85.1, 52.6, 12.8, 82.0),
        (103, "Carvajal-Sevillana", "2026-02-01 09:00:00", None, 45.0, 14.2, 75.0), # Lectura nula por sensor
        (104, "Kennedy", "2026-02-01 07:00:00", 62.0, 38.5, 12.0, 85.0),
        (105, "Kennedy", "2026-02-01 08:00:00", 67.3, 41.2, 13.5, 80.0),
        (106, "Suba", "2026-02-01 07:00:00", 28.5, 14.1, 10.2, 90.0),
        (107, "Suba", "2026-02-01 08:00:00", 31.2, 16.0, 11.8, 84.0),
        (108, "Centro de Alto Rendimiento", "2026-02-01 07:00:00", 34.0, 20.2, 11.0, 86.0),
        (109, "Centro de Alto Rendimiento", "2026-02-01 08:00:00", 39.5, 23.8, 13.1, 79.0),
        (110, "Usaquen", "2026-02-01 07:00:00", 22.1, 11.5, 9.8, 92.0),
        (111, "Fontibon", "2026-02-01 07:00:00", 59.8, 36.4, 12.2, 83.0),
        (112, "Fontibon", "2026-02-01 08:00:00", 64.2, 39.0, 13.8, 78.0)
    ]

    df_crudo = spark.createDataFrame(datos_fuente, schema=esquema_rmcab)
    print("--- [2/5] REGISTROS CRUDOS CARGADOS (CON VALORES NULOS) ---")
    df_crudo.show(truncate=False)

    # 4. Limpieza de datos y clasificacion por norma IBOCA
    df_limpio = df_crudo \
        .filter(col("pm10").isNotNull() & col("pm25").isNotNull()) \
        .withColumn("timestamp", to_timestamp(col("fecha_hora"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("alerta_iboca", 
                    when(col("pm10") <= 54.0, "Bajo Riesgo (Favorable)")
                    .when((col("pm10") > 54.0) & (col("pm10") <= 154.0), "Riesgo Moderado")
                    .otherwise("Riesgo Alto (Alerta)"))

    print("--- [3/5] REGISTROS DEPURADOS Y CATEGORIZADOS (IBOCA) ---")
    df_limpio.select("estacion", "fecha_hora", "pm10", "pm25", "alerta_iboca").show(truncate=False)

    # 5. Analisis Exploratorio Agregado (EDA por Estacion)
    df_resumen = df_limpio.groupBy("estacion") \
        .agg(
            count("id_registro").alias("total_muestras_validas"),
            sp_round(avg("pm10"), 2).alias("promedio_pm10"),
            sp_round(avg("pm25"), 2).alias("promedio_pm25"),
            sp_round(avg("temperatura"), 2).alias("promedio_temperatura"),
            sp_round(avg("humedad_relativa"), 2).alias("promedio_humedad")
        ) \
        .orderBy(desc("promedio_pm10"))

    print("--- [4/5] METRICAS AGREGADAS POR ESTACION METEOROLOGICA ---")
    df_resumen.show(truncate=False)

    # 6. Demostracion formal del modelo RDD (Transformacion y Acciones)
    rdd_sensores = df_limpio.select("estacion", "pm10").rdd

    # Transformacion perezosa (lazy evaluation)
    rdd_criticos = rdd_sensores.filter(lambda fila: fila["pm10"] > 50.0)

    # Acciones que materializan el computo en memoria
    total_estaciones_criticas = rdd_criticos.count()
    primeras_dos_muestras = rdd_criticos.take(2)

    print("--- [5/5] VALIDACION TECNICA DEL MODELO RDD ---")
    print(f" -> Total mediciones con PM10 critico (> 50 ug/m3) [Accion count()]: {total_estaciones_criticas}")
    print(f" -> Muestra de registros recuperados [Accion take(2)]: {primeras_dos_muestras}")

    spark.stop()
    print("\n" + "="*70)
    print(" [FIN] PROCESAMIENTO BATCH EJECUTADO EXITOSAMENTE")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()