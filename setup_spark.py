# Let's import the libraries we will need
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf


def session_spark():
    conf = SparkConf()
    conf.set("spark.ui.port", "4050")
    conf.setAppName("DIS-lab-2")
    conf.setMaster("local[*]")
    conf.set("spark.driver.memory", "2G")
    conf.set("spark.driver.maxResultSize", "2g")
    conf.set("spark.executor.memory", "1G")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()


