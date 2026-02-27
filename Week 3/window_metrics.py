from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("WindowMetricsExport") \
    .master("local[*]") \
    .getOrCreate()

data_path = r"C:\Users\LENOVO\OneDrive\Desktop\Supply Chain Project\data\DataCoSupplyChainDataset.csv"
output_path = r"C:\Users\LENOVO\OneDrive\Desktop\Supply Chain Project\Week 3\window_metrics.csv"

df = spark.read.csv(data_path, header=True, inferSchema=True)

df = df.withColumn(
    "delivery_delay",
    col("Days for shipping (real)") - col("Days for shipment (scheduled)")
)

window_spec = Window.partitionBy("Order Region").orderBy("Order Id")

result = df.withColumn(
    "running_avg_delay",
    avg("delivery_delay").over(window_spec)
).select(
    "Order Region",
    "Order Id",
    "delivery_delay",
    "running_avg_delay"
)

result.toPandas().to_csv(output_path, index=False)

spark.stop()
print("window_metrics.csv exported successfully")









#spark-submit "Week 3/window_metrics.py"