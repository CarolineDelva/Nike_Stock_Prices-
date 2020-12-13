from nike_stock_prices.query import average_open_price
from nike_stock_prices.query2 import average_after_hours_change
from nike_stock_prices.query3 import sqlquery
from unittest import TestCase
from datetime import datetime 
import pyspark 


class PySparkTest(TestCase):
    def test_average_open_price():
        spark = SparkSession.builder.appName("first_query").getOrCreate()
        df = spark.createDataFrame([
            ('1980-01-01', '0.564'),
            ('1980-01-02', '0.564'),
            ('1980-02-01', '7.904'),
            ('1980-03-01', '0.908'),
            ('1980-03-02', '0.564')
        ]).toDF("Date", "Open")

        avg_rdd = average_open_price(df)
        actual = avg_rdd.collect()

        assert(actual == [
            '1980-01,0.564',
            '1980-02,7.904',
            '1980-03,0.736'
        ])

class sqlquerytest(TestCase):
    def test_sql_query():
        spark = SparkSession.builder.appName("first_query").getOrCreate()
        df = spark.createDataFrame([
            ('1980-01-01', '0.564'),
            ('1980-01-02', '0.564'),
            ('1980-12-08', '7.904'),
            ('1981-03-01', '0.908'),
            ('1982-03-01', '0.564')
        ]).toDF("Date", "Close")

        actualDf = sqlquery(df)
        actual_std_dev = actualDf.rdd.collect()[0]["std_dev"]

        assert(actual_std_dev > 0 ) ## == expected std dev

class average_after_hour_test(TestCase):
    def test_average_after_hours():
        spark = SparkSession.builder.appName("first_query").getOrCreate()
        df = spark.createDataFrame([
            ('1980-01-01', '0.564', '0.564'),
            ('1980-01-02', '0.564', '0.908'),
            ('1980-01-03', '7.904', '0.564'),
            ('1980-01-04', '0.908', '0.564'),
            ('1980-01-05', '0.564', '0.908')
        ]).toDF("Date", "Open", "Close")

        actualDf = average_after_hours_change(df)
        actual_row = actualDf.rdd.collect()[0]

        assert(actual_row["week"] == '1980-1')
        assert(actual_row["change"] == 0.5) # put in the actual value
    
