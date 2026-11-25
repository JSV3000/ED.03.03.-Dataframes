import os
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.appName("Analisis_Ventas").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

os.makedirs("DATOS", exist_ok=True)
csv_path = "DATOS/pdi_sales.csv"
if not os.path.exists(csv_path):
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/josepgarcia/datos/main/pdi_sales.csv",
        csv_path
    )

# Cargar CSV
df = spark.read.csv(csv_path, header=True, inferSchema=True, sep=";")

# Esquema y conteo de filas
df.printSchema()
print(f"Total de filas: {df.count()}\n")

# Total de ingresos por país (mayor a menor)
print("Ingresos totales por país:")
df.groupBy("Country") \
    .agg(F.round(F.sum("Revenue"), 2).alias("total_revenue")) \
    .orderBy(F.desc("total_revenue")) \
    .show()

# Ventas en Alemania con más de 1 unidad
print("Ventas en Alemania (Units > 1):")
df.filter((F.col("Country") == "Germany") & (F.col("Units") > 1)).show(10)

# Código postal con mayor número de ventas en Alemania
print("Código postal con más ventas en Alemania:")
df.filter(F.col("Country") == "Germany") \
    .groupBy("Zip") \
    .count() \
    .orderBy(F.desc("count")) \
    .show(1)

# Clasificación de ventas
print("Clasificación de ventas:")
df_clasificado = df.withColumn(
    "Categoria_Venta",
    F.when(F.col("Revenue") > 500, "Alta").otherwise("Normal")
)
df_clasificado.select("ProductID", "Country", "Revenue", "Categoria_Venta").show(10)

# Media de ingresos por año
print("Media de ingresos por año:")
df.withColumn("Anio", F.year(F.to_date(F.col("Date"), "M/d/yyyy"))) \
    .groupBy("Anio") \
    .agg(F.round(F.avg("Revenue"), 2).alias("media_revenue")) \
    .orderBy("Anio") \
    .show()

spark.stop()