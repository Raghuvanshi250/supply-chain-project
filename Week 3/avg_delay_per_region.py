from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import pandas as pd
import os

# -------------------------------
# Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("Week3_Visualization_Export") \
    .master("local[*]") \
    .getOrCreate()

# -------------------------------
# Load Dataset
# -------------------------------
data_path = r"C:\Users\LENOVO\OneDrive\Desktop\Supply Chain Project\data\DataCoSupplyChainDataset.csv"

df = spark.read.csv(
    data_path,
    header=True,
    inferSchema=True
)

# -------------------------------
# Create Delivery Delay
# -------------------------------
df = df.withColumn(
    "delivery_delay",
    col("Days for shipping (real)") - col("Days for shipment (scheduled)")
)

# -------------------------------
# Aggregation (CONTROL TOWER DATA)
# -------------------------------
agg_df = df.groupBy("Order Region").agg(
    avg("delivery_delay").alias("avg_delivery_delay")
)

# -------------------------------
# DATA VALIDATION (MANDATORY)
# -------------------------------
spark_row_count = agg_df.count()
print("Spark Aggregated Row Count:", spark_row_count)

agg_df.show(truncate=False)

# -------------------------------
# EXPORT (WINDOWS SAFE)
# -------------------------------
output_dir = r"C:\Users\LENOVO\OneDrive\Desktop\Supply Chain Project\Week 3"
output_file = os.path.join(output_dir, "aggregated_delivery_metrics.csv")

# Convert Spark → Pandas
pdf = agg_df.toPandas()

# Validation after conversion
print("Pandas Row Count:", len(pdf))

# Save CSV
pdf.to_csv(output_file, index=False)

print("Export completed successfully:", output_file)

# -------------------------------
# Stop Spark
# -------------------------------
spark.stop()






#spark-submit "Week 3/export_visualization.py"