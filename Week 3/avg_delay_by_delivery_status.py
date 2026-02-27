from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder \
    .appName("AvgDelayByDeliveryStatus") \
    .master("local[*]") \
    .getOrCreate()

data_path = r"C:\Users\LENOVO\OneDrive\Desktop\Supply Chain Project\data\DataCoSupplyChainDataset.csv"
output_path = r"C:\Users\LENOVO\OneDrive\Desktop\Supply Chain Project\Week 3\avg_delay_by_delivery_status.csv"

df = spark.read.csv(data_path, header=True, inferSchema=True)

df = df.withColumn(
    "delivery_delay",
    col("Days for shipping (real)") - col("Days for shipment (scheduled)")
)

result = df.groupBy("Delivery Status").agg(
    avg("delivery_delay").alias("avg_delivery_delay")
)

result.toPandas().to_csv(output_path, index=False)

spark.stop()
print("avg_delay_by_delivery_status.csv exported")








#spark-submit "Week 3/avg_delay_by_delivery_status.py"