# Procesamiento de Datos Masivos con PySpark (Modo Batch)
**Curso:** Big Data (Código: 202016911) – Grupo: 202016911_124  
**Estudiante:** Andrés Felipe Rozo Quevedo  
**Institución:** Universidad Nacional Abierta y a Distancia (UNAD)  

---

## 1. Planteamiento del Problema
Análisis exploratorio, limpieza y agregación analítica de datos ambientales de la **Red de Monitoreo de Calidad del Aire de Bogotá (RMCAB)**.

### Objetivos Técnicos:
* Definición de esquema formal tipado (`StructType`) para optimización de memoria mediante el motor Catalyst.
* Depuración de lecturas erráticas y valores nulos provocados por fallas físicas de sensores en estaciones meteorológicas.
* Clasificación de muestras según el estándar ambiental normativo IBOCA ($PM_{10}$ y $PM_{2.5}$).
* Agrupación multivariada por estación para identificar zonas de alta criticidad ambiental (Carvajal-Sevillana y Kennedy).
* Demostración de transformaciones perezosas (`filter`) y acciones detonadoras (`count()`, `take(2)`) sobre el modelo de bajo nivel **RDD**.

---

## 2. Requisitos de Ejecución
* Sistema Operativo: Ubuntu Server 22.04 LTS / Linux
* Apache Spark 3.5.x
* Python 3.10+ / PySpark

---

## 3. Instrucción de Ejecución
Ejecutar a través del submiter de Spark en consola local:
```bash
/opt/spark/bin/spark-submit batch_calidad_aire.py
