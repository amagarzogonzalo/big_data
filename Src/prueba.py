from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from graphframes import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("GraphFramesExample") \
    .getOrCreate()

# Create vertices DataFrame
vertices_data = [("1",), ("2",), ("3",), ("4",), ("5",)]
vertices_schema = StructType([StructField("id", StringType(), True)])
vertices = spark.createDataFrame(vertices_data, schema=vertices_schema)

# Create edges DataFrame
edges_data = [("1", "2"), ("2", "3"), ("3", "4"), ("4", "5"), ("5", "1")]
edges_schema = StructType([StructField("src", StringType(), True), StructField("dst", StringType(), True)])
edges = spark.createDataFrame(edges_data, schema=edges_schema)

# Create GraphFrame
g = GraphFrame(vertices, edges)

# Compute connected components
connected_df = g.connectedComponents()

# Show the result
connected_df.show()