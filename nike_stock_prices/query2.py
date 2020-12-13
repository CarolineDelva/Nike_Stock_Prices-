import argparse
import pyspark
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, lag, avg
from pyspark.sql import Window
from pyspark.sql.types import *

from datetime import datetime

ss = SparkSession.builder.appName("first_query").getOrCreate()


local_input_file = "../data/NKE.csv"


local_output_file = "../data/"

df = ss.read.option("header", True).format("csv").load(local_input_file)


def get_week(datestr):
    dt = datetime.strptime(datestr, "%Y-%m-%d")
    week = f"{dt.year}-{dt.isocalendar()[1]}"
    return week


# averge after hour change per week
def average_after_hours_change(df):
    week_udf = udf(get_week, StringType())

    result = (
        df.withColumn("week", week_udf(col("Date")))
        .withColumn(
            "previous_close",
            lag(col("Close")).over(Window.partitionBy(lit(None)).orderBy(col("week"))),
        )
        .withColumn("change", col("previous_close") - col("Open"))
        .groupBy(col("week"))
        .agg(avg("change"))
    )

    return result


average_after_hours_change(df).write.option("header", True).mode("overwrite").format(
    "csv"
).save(local_output_file + "query2/")

# ~/spark-2.4.5-bin-hadoop2.6/bin/spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.5 --master local[*] query2.py ../data/*csv
