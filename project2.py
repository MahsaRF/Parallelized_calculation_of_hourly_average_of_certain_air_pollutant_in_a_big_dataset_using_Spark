# Mahsa Rezaei firuzkuhi

import string
import sys
import re
import os

# Creating PySpark Context
from pyspark import SparkConf, SparkContext
import time
from datetime import timedelta
from operator import add
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType

# Creating PySpark SQL Context
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, concat_ws

conf = SparkConf()
conf.setAppName('project-2')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
spark = SparkSession.builder.config('spark.cassandra.connection.host','192.168.1.105').config("spark.cassandra.auth.username",'stud25').config("spark.cassandra.auth.password", 'stud25').getOrCreate()

# The schema is encoded in a string.
schemaString = "year month day hour min region parid parname site cams value flag"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
start_time = time.time()

#CSV input
df = sqlContext.read.format('com.databricks.spark.csv').options(delimiter=',',nullValue='').load("/home2/input/2013_data_jan.csv",schema=schema)

#Casshandra input
df = spark.read.format('org.apache.spark.sql.cassandra').load(table="project-2_table", keyspace="stud25")

#df.printSchema()
df =  df.filter(df.parname == "o3")
df_1 = df.filter(df.flag.isNull()).filter("value>0")
df_invalid = df[ ((df.parname == "o3") & (df.flag == '') & (df.value < 0))|((df.parname == 'o3') & (df.flag != ' ')) ]
df_1_groupedAvg = df_1.groupBy("site", "month", "day", "hour").agg(F.mean('value')).alias('Avgvalue')
df_invalid_grouped = df_invalid.groupBy("site", "month", "day", "hour").count().filter("count >= 12")
df_invalid_groupedAvg = df_invalid_grouped.withColumn('Avgvalue', lit(-1)).drop("count")
final_avg = df_1_groupedAvg.union(df_invalid_groupedAvg)
#final_avg = final_avg.select(col("Avg(value)").alias("Avgvalue"))


elapsed_time_secs = time.time() - start_time
print("Execution took: %s secs " % elapsed_time_secs)

#write into Text file
#final_avg.coalesce(1).rdd.saveAsTextFile('/home/output/stud25/project2-results.csv')
final_avg_1column = final_avg.select(concat_ws(",", *final_avg.columns).alias('data'))
final_avg_1column.coalesce(1).write.format("text").option("header", "false").mode("append").save("/home/output/stud25/project2-results.txt")

