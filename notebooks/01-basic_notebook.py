# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook robusto: análisis de ventas con PySpark en Databricks
# MAGIC
# MAGIC Este notebook amplía el ejemplo básico e ilustra varias de las funciones
# MAGIC destacadas de Databricks + PySpark: DataFrames, funciones de ventana,
# MAGIC agregaciones, UDFs, escritura en formato Delta, "time travel" y consultas
# MAGIC con Spark SQL sobre la misma tabla.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

spark = SparkSession.builder.appName("analisis_ventas").getOrCreate()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Creación de datos de ejemplo con esquema explícito
# MAGIC
# MAGIC En un caso real, este DataFrame vendría de un CSV, una base de datos o una
# MAGIC API. Definir el esquema explícitamente evita errores de inferencia de tipos
# MAGIC cuando el volumen de datos es grande.

# COMMAND ----------
schema = StructType([
    StructField("region", StringType(), True),
    StructField("producto", StringType(), True),
    StructField("fecha", StringType(), True),
    StructField("ventas", DoubleType(), True),
])

data = [
    ("Occidente", "Laptop",   "2026-01-05", 1200.0),
    ("Occidente", "Laptop",   "2026-02-05", 1500.0),
    ("Occidente", "Mouse",    "2026-01-05",   25.0),
    ("Andina",    "Laptop",   "2026-01-05", 1100.0),
    ("Andina",    "Teclado",  "2026-01-05",   45.0),
    ("Caribe",    "Mouse",    "2026-01-05",   30.0),
    ("Caribe",    "Laptop",   "2026-02-05", None),   # dato nulo intencional
]

df = spark.createDataFrame(data, schema)
df = df.withColumn("fecha", F.to_date("fecha", "yyyy-MM-dd"))

print("Esquema:")
df.printSchema()
df.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Limpieza de datos
# MAGIC
# MAGIC Se eliminan duplicados y se imputan valores nulos en la columna `ventas`
# MAGIC con 0, para no perder la fila completa en los agregados posteriores.

# COMMAND ----------
df_clean = (
    df.dropDuplicates()
      .fillna({"ventas": 0.0})
)
df_clean.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Función definida por el usuario (UDF)
# MAGIC
# MAGIC Ejemplo de cómo extender PySpark con lógica de Python personalizada,
# MAGIC útil cuando una transformación no existe como función nativa de Spark.

# COMMAND ----------
from pyspark.sql.types import StringType as _StringType

def clasificar_venta(monto):
    if monto is None or monto == 0:
        return "Sin venta"
    elif monto < 100:
        return "Baja"
    elif monto < 1000:
        return "Media"
    else:
        return "Alta"

clasificar_udf = F.udf(clasificar_venta, _StringType())
df_clean = df_clean.withColumn("categoria_venta", clasificar_udf(F.col("ventas")))
df_clean.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Funciones de ventana (Window Functions)
# MAGIC
# MAGIC Calculamos el acumulado de ventas por región ordenado por fecha, algo muy
# MAGIC común en reportes financieros y de negocio.

# COMMAND ----------
ventana = Window.partitionBy("region").orderBy("fecha")

df_ventana = df_clean.withColumn(
    "ventas_acumuladas", F.sum("ventas").over(ventana)
)
df_ventana.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Agregaciones y agrupamientos
# MAGIC
# MAGIC Total y promedio de ventas por región y por producto.

# COMMAND ----------
resumen_region = (
    df_clean.groupBy("region")
    .agg(
        F.sum("ventas").alias("total_ventas"),
        F.avg("ventas").alias("promedio_ventas"),
        F.count("*").alias("num_transacciones"),
    )
    .orderBy(F.desc("total_ventas"))
)
resumen_region.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Guardado en formato Delta (con manejo de entornos sin DBFS)
# MAGIC
# MAGIC Delta Lake es lo que da a Databricks transacciones ACID y "time travel".
# MAGIC Si el entorno no permite escribir en DBFS o en una ruta montada, el
# MAGIC notebook lo detecta y continúa sin fallar.

# COMMAND ----------
ruta_delta = "/tmp/ventas_delta"

try:
    (
        df_ventana.write
        .format("delta")
        .mode("overwrite")
        .save(ruta_delta)
    )
    print(f"Datos guardados en formato Delta en: {ruta_delta}")

    # Registrar como tabla para poder consultarla con SQL
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS ventas_delta
        USING DELTA
        LOCATION '{ruta_delta}'
    """)
except Exception as e:
    print("No fue posible escribir en Delta en este entorno.")
    print(f"Detalle: {e}")
    df_ventana.createOrReplaceTempView("ventas_delta")
    print("Se registró una vista temporal 'ventas_delta' para continuar con las consultas SQL.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Consulta con Spark SQL sobre la misma tabla
# MAGIC
# MAGIC Una de las ventajas de Databricks es poder mezclar Python y SQL sobre los
# MAGIC mismos datos sin moverlos entre sistemas distintos.

# COMMAND ----------
spark.sql("""
    SELECT region, categoria_venta, SUM(ventas) AS total
    FROM ventas_delta
    GROUP BY region, categoria_venta
    ORDER BY total DESC
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. "Time travel" (solo aplica si se escribió en Delta real)
# MAGIC
# MAGIC Delta Lake guarda versiones de la tabla en cada escritura, permitiendo
# MAGIC consultar el estado de los datos en un punto anterior en el tiempo.
# MAGIC
# MAGIC ```python
# MAGIC # Ejemplo (requiere al menos 2 versiones de la tabla):
# MAGIC df_version_0 = spark.read.format("delta").option("versionAsOf", 0).load(ruta_delta)
# MAGIC df_version_0.show()
# MAGIC ```

# COMMAND ----------
print("Notebook ejecutado correctamente.")
spark.stop()