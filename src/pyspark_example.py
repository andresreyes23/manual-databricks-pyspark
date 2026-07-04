from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main() -> None:
    spark = SparkSession.builder.appName("manual_databricks_example").getOrCreate()

    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    df = spark.createDataFrame(data, ["name", "age"])

    print("Esquema del DataFrame:")
    df.printSchema()

    print("Primeras filas:")
    df.show()

    df_clean = df.filter(col("age").isNotNull())
    print("Promedio de edad:")
    df_clean.groupBy().avg("age").show()

    spark.stop()


if __name__ == "__main__":
    main()
