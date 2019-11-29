import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from kafka import KafkaConsumer

consumer = KafkaConsumer('ttt',value_deserializer=lambda v:json.loads(v),auto_offset_reset='earliest',bootstrap_servers=["localhost:9092"])

spark = SparkSession\
     .builder\
     .appName("Python Spark SQL basic example")\
     .getOrCreate()

df=spark.read.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","kafka_topic").option("startingOffsets", "earliest").option("endingOffsets", "latest").load()
rs=df.selectExpr("CAST(value AS STRING)")
rs.show()

Structure_=StructType([
    StructField(" Call  Number", StringType()),
    StructField("Unit ID",  StringType()),
    StructField("IncidentNumber", StringType()),
    StructField(" CallType", StringType()),
    ])

def parse_data_from_kafka_message(sdf, schema):
 from pyspark.sql.functions import split
# assert sdf.isStreaming == False
 col = split(sdf['value'],',')


 for i, j in enumerate(schema,start=0):
     sdf = sdf.withColumn(j.name, col.getItem(i).cast(j.dataType))
 return sdf.select([field.name for field in schema])


df2 = parse_data_from_kafka_message(rs, Structure_)


df2.write.format('jdbc').options(
      url='jdbc:mysql://localhost:3306/kafka',
      driver='com.mysql.jdbc.Driver',
      dbtable='kj',
      user='shoaib',
      password='Tibil123*').mode('append').save()
