import argparse
import pyspark
import sys 
from pyspark import SparkContext, SparkConf 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import udf, col, lit, lag, avg
from pyspark.sql.window.Window
from pyspark.sql.types import *

from datetime import datetime 

ss = SparkSession.builder.appName("first_query").getOrCreate()



local_input_file = '../data/NIKE.csv'
local_output_file = '../data/'

df = ss.read.option('header', True).format('csv').load(local_input_file)

# average open price per month 

def get_month(date):
    return date[0:7]

average_open_price = df.rdd\
    .map(lambda c: ( get_month(c['Date']) , ( float(c['Open']), 1) ) )\
    .reduceByKey(lambda a , b: ( a[0] + b[0], a[1] + b[1] ) ) \
    .map(lambda c: (c[0], c[1][0] / c[1][1] ) )\
    .map(lambda c: c[0]+ "," +str(c[1]) )\
    .saveAsTextFile(local_output_file + "query1/")

    

def get_week(datestr):
    dt = datetime.strptime(datestr, "%Y-%m-%d")
    week = f"{dt.year}-{dt.isocalendar()[1]}"
    return week

# averge after hour change per week 
week_udf = udf(get_week, StringType())
average_after_change = df.withColumn("week", week_udf(col("Date")))\
    .withColumn("previous_close", lag(col("Close").over(Window.partitionBy(lit(None).orderBy(col("week"))))))\
    .withColumn("change", col("previous_close") - col("Open"))\
    .groupBy(col("week"))\
    .agg(avg("change"))\
    .write.option("header", True).mode("overwrite").format("csv").save(local_output_file + "query2/")




# standard deviation of closing prices between a 1980-12-02 and 1981-04-09

df.registerTempTable("prices")


query3 = spark.sql("""
select stddev(Close)
FROM prices
WHERE Date BETWEEN '1980-12-02' and '1981-04-09'
""")

query3.write.option("header", True).mode("overwrite").format("csv").save(local_output_file + "query3/")
