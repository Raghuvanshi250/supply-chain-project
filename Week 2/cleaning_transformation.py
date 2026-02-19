from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("Week2_Cleaning_Transformation") \
    .master("local[*]") \
    .getOrCreate()

# Load dataset
df = spark.read.csv(
    "C:/Users/LENOVO/OneDrive/Desktop/Supply Chain Project/data/DataCoSupplyChainDataset.csv",
    header=True,
    inferSchema=True
)

# 1. Standardize column names
for c in df.columns:
    df = df.withColumnRenamed(c, c.lower().replace(" ", "_"))

# 2. Drop rows with critical null values
df = df.dropna(subset=["order_id", "customer_id", "sales"])

# 3. Convert date columns
df = df.withColumn(
    "order_date",
    to_date(col("order_date_(dateorders)"), "MM/dd/yyyy")
).withColumn(
    "shipping_date",
    to_date(col("shipping_date_(dateorders)"), "MM/dd/yyyy")
)

# 4. Remove duplicate records
df = df.dropDuplicates(["order_id", "order_item_id"])

# 5. Select required columns
df_clean = df.select(
    "order_id",
    "customer_id",
    "order_date",
    "shipping_date",
    "sales",
    "order_status",
    "shipping_mode",
    "market",
    "order_region"
)

# Output validation
print("Cleaned Dataset Schema")
df_clean.printSchema()
print("Rows after cleaning:", df_clean.count())

import time

df_clean.explain(True)

print("Spark UI will remain active for 1 hour...")
time.sleep(3600)   # 3600 seconds = 1 hour

spark.stop()







#cd "C:\Users\LENOVO\OneDrive\Desktop\Supply Chain Project\Week 2"
#spark-submit cleaning_transformation.py
