from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg
from pyspark.sql.window import Window

# Spark session
spark = SparkSession.builder \
    .appName("Week2_Window_Functions") \
    .master("local[*]") \
    .getOrCreate()

# Load dataset
df = spark.read.csv(
    "C:/Users/LENOVO/OneDrive/Desktop/Supply Chain Project/data/DataCoSupplyChainDataset.csv",
    header=True,
    inferSchema=True
)

# Define window specification
window_spec = Window \
    .partitionBy("Order Region") \
    .orderBy("Days for shipping (real)") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Running total
df = df.withColumn(
    "Running_Total_Shipping_Days",
    sum(col("Days for shipping (real)")).over(window_spec)
)

# Rolling average
df = df.withColumn(
    "Rolling_Avg_Shipping_Days",
    avg(col("Days for shipping (real)")).over(window_spec)
)

# Show result
df.select(
    "Order Region",
    "Days for shipping (real)",
    "Running_Total_Shipping_Days",
    "Rolling_Avg_Shipping_Days"
).show(10)

spark.stop()










#cd "C:\Users\LENOVO\OneDrive\Desktop\Supply Chain Project\Week 2"
#spark-submit window_functions.py