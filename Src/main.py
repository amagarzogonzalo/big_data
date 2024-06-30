import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

from processes import *
from servers import *
from dbscan import minhash_dbscan
from setup_spark import session_spark

if __name__ == "__main__":
    spark = session_spark()
    sc = spark.sparkContext

    # Read log file and create original log d
    DATASET_NAME = "data_processes_v2.json"
    #DATASET_NAME = "simple.json"
    logs_df = spark.read.json("../Data/"+DATASET_NAME)

    # Create the processes df with their associated request paths and euler strings
    processes_df = create_processes_df(spark, logs_df)

    # Add depth_from in every log, indicating the depth of the server performing the Request or giving the Response.
    logs_with_depth_df, processes_with_depth_df = add_depth_features(logs_df, processes_df)

    # Create servers df with an extra column connections that contains an array of strings.
    # String represent the connection tuples (Related_server, Request_type, Depth) but concatenating all elements as strings.
    #server_names = get_server_names(logs_rdd=logs_df.rdd)
    servers_df = create_servers_df (spark, logs_with_depth_df)

    servers_with_cluster_df, cluster_distances_df = cluster_servers(spark, servers_df)

    processes_with_depth_df = add_cluster_features(
        processes_df=processes_with_depth_df,
        servers_with_cluster_df=servers_with_cluster_df
    )
    cluster_logs_df = get_cluster_logs_df(logs_df, servers_with_cluster_df)

    processes_with_elements_df = add_processes_elements(
        spark,
        processes_df=processes_with_depth_df,
        cluster_logs_df=cluster_logs_df,
    )
    
    # Make groups of equal servers
    components = equal_processes(spark, 
                                 processes_with_elements_df = processes_with_elements_df,
                                 cluster_logs_df= cluster_logs_df,
                                 dataset_name= DATASET_NAME)

    

