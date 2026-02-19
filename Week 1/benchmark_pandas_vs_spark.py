import time
import pandas as pd
from pyspark.sql import SparkSession

file_path = "C:/Users/LENOVO/OneDrive/Desktop/Supply Chain Project/data/DataCoSupplyChainDataset.csv"

# ---------------- Pandas Benchmark ----------------
start_pandas = time.time()
df_pandas = pd.read_csv(file_path, encoding="latin1")
pandas_rows = len(df_pandas)
end_pandas = time.time()

pandas_time = end_pandas - start_pandas

# ---------------- Spark Benchmark ----------------
spark = SparkSession.builder \
    .appName("Week1_Benchmark") \
    .master("local[*]") \
    .getOrCreate()

start_spark = time.time()
df_spark = spark.read.csv(file_path, header=True, inferSchema=True)
spark_rows = df_spark.count()
end_spark = time.time()

spark_time = end_spark - start_spark

spark.stop()

# ---------------- Results ----------------
print("\nPERFORMANCE BENCHMARK RESULTS")
print("--------------------------------")
print("Pandas Rows Loaded :", pandas_rows)
print("Pandas Load Time   :", round(pandas_time, 2), "seconds")

print("\nSpark Rows Loaded  :", spark_rows)
print("Spark Load Time    :", round(spark_time, 2), "seconds")












#cd "C:\Users\LENOVO\OneDrive\Desktop\Supply Chain Project\Week 1"
#spark-submit benchmark_pandas_vs_spark.py
