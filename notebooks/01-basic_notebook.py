# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook básico: PySpark en Databricks
# MAGIC Este notebook contiene los pasos mínimos para ejecutar PySpark en Databricks. Puedes importarlo directamente en Databricks (archivo .py) o copiar/pegar su contenido en un notebook nuevo.

# COMMAND ----------
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("notebook_basic").getOrCreate()

# Crear un DataFrame de ejemplo
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
df = spark.createDataFrame(data, ["name", "age"])

print("Esquema del DataFrame:")
df.printSchema()
print("Primeras filas:")
df.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Limpieza simple y agregados

# COMMAND ----------
from pyspark.sql.functions import col

df_clean = df.filter(col("age").isNotNull())
df_clean.groupBy().avg("age").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Guardar resultados (opcional)
# MAGIC
# MAGIC En este entorno puede que el acceso a `dbfs:/FileStore/...` esté deshabilitado.
# MAGIC Si tu workspace permite escritura, reemplaza esta sección por una ruta válida en un volumen montado o en un almacenamiento configurado.

# COMMAND ----------
print("El DataFrame limpio ya se ha generado y mostrado. Omite la escritura si DBFS está deshabilitado.")

# COMMAND ----------
spark.stop()
