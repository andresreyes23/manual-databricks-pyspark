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
# MAGIC ## Guardar resultados (ejemplo Databricks)

# COMMAND ----------
output_path = "dbfs:/FileStore/notebook_result_parquet"
df_clean.write.mode("overwrite").parquet(output_path)
print(f"Resultados guardados en: {output_path}")

# COMMAND ----------
spark.stop()
