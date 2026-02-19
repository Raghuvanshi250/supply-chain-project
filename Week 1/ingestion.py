from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Week1_Ingestion") \
    .master("local[*]") \
    .getOrCreate()

df1 = spark.read.csv(
    "C:/Users/LENOVO/OneDrive/Desktop/Supply Chain Project/data/DataCoSupplyChainDataset.csv",
    header=True,
    inferSchema=True
)

df2 = spark.read.csv(
    "C:/Users/LENOVO/OneDrive/Desktop/Supply Chain Project/data/DescriptionDataCoSupplyChain.csv",
    header=True,
    inferSchema=True
)

df3 = spark.read.csv(
    "C:/Users/LENOVO/OneDrive/Desktop/Supply Chain Project/data/tokenized_access_logs.csv",
    header=True,
    inferSchema=True
)

print("Dataset 1")
df1.printSchema()
print("Rows:", df1.count())

print("\nDataset 2")
df2.printSchema()
print("Rows:", df2.count())

print("\nDataset 3")
df3.printSchema()
print("Rows:", df3.count())

spark.stop()











#cd "C:\Users\LENOVO\OneDrive\Desktop\Supply Chain Project\Week 1"
#spark-submit ingestion.py
