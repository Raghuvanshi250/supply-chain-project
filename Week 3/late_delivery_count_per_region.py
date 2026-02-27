from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

spark = SparkSession.builder \
    .appName("LateDeliveryCountPerRegion") \
    .master("local[*]") \
    .getOrCreate()

data_path = r"C:\Users\LENOVO\OneDrive\Desktop\Supply Chain Project\data\DataCoSupplyChainDataset.csv"
output_path = r"C:\Users\LENOVO\OneDrive\Desktop\Supply Chain Project\Week 3\late_delivery_count_per_region.csv"

df = spark.read.csv(data_path, header=True, inferSchema=True)

df = df.withColumn(
    "delivery_delay",
    col("Days for shipping (real)") - col("Days for shipment (scheduled)")
)

late_df = df.withColumn(
    "is_late",
    when(col("delivery_delay") > 0, 1).otherwise(0)
)

result = late_df.groupBy("Order Region").agg(
    count(when(col("is_late") == 1, True)).alias("late_delivery_count")
)

result.toPandas().to_csv(output_path, index=False)

spark.stop()
print("late_delivery_count_per_region.csv exported")








#spark-submit "Week 3/late_delivery_count_per_region.py"