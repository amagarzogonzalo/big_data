# Let's import the libraries we will need
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf

def session_spark():
    conf = SparkConf()
    #conf.set("spark.ui.port", "4040")
    conf.set("spark.local.ip", "172.22.102.219")
    conf.setAppName("The fifteenth")
    conf.setMaster("local[*]")
    """ conf.set("spark.driver.memory", "2G")
    conf.set("spark.driver.maxResultSize", "2g")
    conf.set("spark.executor.memory", "1G") """
    return SparkSession.builder.config(conf=conf).getOrCreate()