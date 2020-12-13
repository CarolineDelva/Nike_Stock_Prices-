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

# average open price per month


def get_month(date):
    return date[0:7]


def average_open_price(df):
    return (
        df.rdd.map(lambda c: (get_month(c["Date"]), (float(c["Open"]), 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .map(lambda c: (c[0], c[1][0] / c[1][1]))
        .map(lambda c: c[0] + "," + str(c[1]))
    )


average_open_price(df).saveAsTextFile(local_output_file + "query1/")
