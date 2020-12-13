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

spark = (
    SparkSession.builder.master("local")
    .appName("Word Count")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

df = ss.read.option("header", True).format("csv").load(local_input_file)


# standard deviation of closing prices between a 1980-12-02 and 1981-04-09
def sqlquery(df):
    df.registerTempTable("prices")
    return spark.sql(
        """
            select stddev(Close) std_dev
            FROM prices
            WHERE Date BETWEEN '1980-12-02' and '1981-04-09'
            """
    )


sqlquery(df).write.option("header", True).mode("overwrite").format("csv").save(
    local_output_file + "query3/"
)
