import os
#from tkinter.dnd import dnd_start
from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import *

def init_spark():
    print("Creating session")
    spark = SparkSession.builder.appName("EDEM_APP").getOrCreate()
    return spark

def main():
    spark=init_spark()
    stream_detail_df = spark.readStream.format("kafka")\
                    .option("kafka.bootstrap.servers", "kafka0:29092")\
                    .option("subscribe", "mocked_data")\
                    .option("startingOffsets", "earliest")\
                    .load() 
    
    stream_detail_df.printSchema()
    #stream_detail_df.isStreaming()
    ds = stream_detail_df.selectExpr("CAST(value AS STRING)")
    print(type(stream_detail_df))
    print(type(ds))
    print("########## RAW QUERY ##########")
    rawQuery = ds.writeStream.queryName("qraw").format("memory").start()      
    time.sleep(30)
    spark.sql("select * from qraw").show()
    rawQuery.stop()
    print("########## DATA QUERY ##########")
    dataQuery = ds.writeStream.queryName("qdata").format("memory").start()
    #alerts = spark.sql("select * from qdata")
    time.sleep(30)
    print("PRINT ALERTS")
    print("#####################")
    spark.sql("select * from qdata").show()
    #TESTEANDO QUERY.stop
    dataQuery.stop()
    #data = spark.sql("select * from qdata")
    #data.show()


if __name__ == '__main__':
  main()
