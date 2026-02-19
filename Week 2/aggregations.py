from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col

# ----------------------------------------
# Spark Session
# ----------------------------------------
spark = SparkSession.builder \
    .appName("Week2_Aggregations") \
    .master("local[*]") \
    .getOrCreate()

# ----------------------------------------
# Load Dataset
# ----------------------------------------
df = spark.read.csv(
    "C:/Users/LENOVO/OneDrive/Desktop/Supply Chain Project/data/DataCoSupplyChainDataset.csv",
    header=True,
    inferSchema=True
)

# ----------------------------------------
# Create Delivery Delay Column
# ----------------------------------------
df = df.withColumn(
    "Delivery_Delay",
    col("Days for shipping (real)") - col("Days for shipment (scheduled)")
)

# ----------------------------------------
# Aggregation 1: Average Delay per Region
# ----------------------------------------
avg_delay_region = df.groupBy("Order Region") \
    .agg(avg("Delivery_Delay").alias("Avg_Delivery_Delay"))

print("\nAverage Delivery Delay per Region")
avg_delay_region.show()

# ----------------------------------------
# Aggregation 2: Late Deliveries Count per Region
# ----------------------------------------
late_deliveries = df.filter(col("Late_delivery_risk") == 1) \
    .groupBy("Order Region") \
    .agg(count("*").alias("Late_Delivery_Count"))

print("\nLate Deliveries per Region")
late_deliveries.show()

# ----------------------------------------
# Aggregation 3: Avg Delay by Delivery Status
# ----------------------------------------
status_delay = df.groupBy("Delivery Status") \
    .agg(avg("Delivery_Delay").alias("Avg_Delay"))

print("\nAverage Delay by Delivery Status")
status_delay.show()

# ----------------------------------------
# Stop Spark
# ----------------------------------------
spark.stop()









# cd "C:\Users\LENOVO\OneDrive\Desktop\Supply Chain Project\Week 2"
# spark-submit aggregations.py