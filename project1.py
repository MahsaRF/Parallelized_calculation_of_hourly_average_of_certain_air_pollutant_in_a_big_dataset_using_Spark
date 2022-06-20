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
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import lit

conf = SparkConf()
conf.setAppName('project-1')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# The schema is encoded in a string
schemaString = "Year Month Day hour min region parID parName site cams value flag"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
start_time = time.time()

#CSV input
df = sqlContext.read.format('com.databricks.spark.csv').options(delimiter=',',nullValue='').load("/home2/input/2013_data_jan",schema=schema)

#Parquet input
df = sqlContext.read.parquet('/home/output/stud25/project-1_parquetRecords/part-00000-b80b8cf4-1e4a-4a01-8c5a-4e42ff787c79-c000.snappy.parquet')

#JSON input
df = sqlContext.read.json('/home/output/stud25/project-1_jsonRecords/part-00000-0b7a754d-2abb-42af-a7be-57c9864555b2-c000.json')

#Avro input
df = sqlContext.read.format("com.databricks.spark.avro").load("/home/output/stud25/project-1_avroRecords.avro")

#df.printSchema()
#df.show(truncate = False)
#df.show()

df =  df.filter(df.parName == "o3")
df_1 = df.filter(df.flag.isNull()).filter("value>0")
df_invalid = df[ ((df.parName == "o3") & (df.flag == '') & (df.value < 0))|((df.parName == 'o3') & (df.flag != ' ')) ]
df_1_groupedAvg = df_1.groupBy("site", "Month", "Day", "hour").agg(F.mean('value')).alias('Avgvalue')
df_invalid_grouped = df_invalid.groupBy("site", "Month", "Day", "hour").count().filter("count >= 12")
df_invalid_groupedAvg = df_invalid_grouped.withColumn('Avgvalue', lit(-1)).drop("count")
final_avg = df_1_groupedAvg.union(df_invalid_groupedAvg)
#final_avg = final_avg.select(col("Avg(value)").alias("Avgvalue"))

#write CSV
final_avg.coalesce(1).write.csv('/home/output/stud25/project-1_csvResult.csv', 'overwrite')

#write Parquet
final_avg.coalesce(1).write.parquet('/home/output/stud25/project-1_parquetResult.parquet', 'overwrite')

#write JSON 
final_avg.coalesce(1).write.json('/home/output/stud25/project-1_jsonRecords.json', 'overwrite')

#write avro
final_avg.coalesce(1).write.format("com.databricks.spark.avro").save("/bigd10/project-1-avro.avro")

elapsed_time_secs = time.time() - start_time
print("Execution took: %s secs " % elapsed_time_secs)