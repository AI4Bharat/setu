from pyspark.sql import SparkSession
from pyspark import TaskContext
from pyspark.sql.functions import map_concat
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession \
        .builder \
        .appName("Convert JSONs to Parquet") \
        .getOrCreate()

sc = spark.sparkContext


data = [
    ("36636","Finance",3000,"USA"),
    ("36636","Sales",3000,"USA"), 
    ("36636","Sales",3000,"USA"), 
    ("40288","Finance",5000,"IND"), 
    ("42114","Sales",3900,"USA"), 
    ("39192","Marketing",2500,"CAN"), 
    ("34534","Sales",6500,"USA") 
]
schema = StructType([
    StructField('id', StringType(), True),
    StructField('dept', StringType(), True),
    StructField('salary', IntegerType(), True),
    StructField('location', StringType(), True)
])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)


#Convert columns to Map
from pyspark.sql.functions import col,lit,create_map
df = df.withColumn("propertiesMap",create_map(
        lit("salary"),col("salary"),
        lit("location"),col("location")
        )).drop("salary","location")
df.printSchema()
df.show(truncate=False)

df.select("propertiesMap").rdd.reduceByKey(add).collect()