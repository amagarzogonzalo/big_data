import numpy as np
from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

import processes 
import servers 
from setup_spark import session_spark

if __name__ == "__main__":
    spark = session_spark()
    sc = spark.sparkContext

    # Read log file and create original log d
    DATASET_NAME = "data_processes_v2.json"
    logs_df = spark.read.json("../Data/"+DATASET_NAME)

    # Create the processes df with their associated request paths
    processes_df = spark.createDataFrame(processes.get_processes_request_paths(logs_rdd=logs_df.rdd) \
                                         .map(lambda x: Row(process_id=x[0], request_path=x[1])))

    # Add depth_from in every log, indicating the depth of the server performing the Request or giving the Response.
    logs_with_depth_df, processes_with_depth_df = processes.add_depth_to_df(logs_df, processes_df)

    # Create servers df with an extra column connections that contains an array of strings.
    # String represent the connection tuples (Related_server, Request_type, Depth) but concatenating all elements as strings.
    server_names = servers.get_server_names(logs_rdd=logs_df.rdd)
    servers_df = spark.createDataFrame(data=servers.get_string_server_connections(logs_with_depth_rdd = logs_with_depth_df.rdd),
                                       schema=StructType([
                                                StructField("server_name", StringType(), True),
                                                StructField("connections", ArrayType(StringType()), True)
                                            ])
                                       )

    distinct_connections = servers_df.select(explode(col("connections")).alias("connection")).distinct().count()
    #print(f"Number of different connections: {distinct_connections}")

    # Add a column to servers df representing the identirty matrix of the set of connections (as numFeatures=distinct_connections)
    # If we see that number of distinct connections is too high we could set a maximun
    hashingTF = HashingTF(inputCol="connections", outputCol="features", numFeatures=distinct_connections)
    features_server_df = hashingTF.transform(servers_df)

    # Add a column to servers df indicating the hashes of the columns of the signature/representation matrix.
    mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
    #TODO: Decide the number of hash tables
    model = mh.fit(features_server_df)
    transformedData = model.transform(features_server_df)  

    # Perform clustering with LSH by giving a Jaccard distance threshold.
    approx_Jaccard_distances = model.approxSimilarityJoin(datasetA=features_server_df, datasetB=features_server_df, 
                                threshold=0.5, distCol="JaccardDistance").select(
                                    col("datasetA.server_name").alias("server_name_A"),
                                    col("datasetB.server_name").alias("server_name_B"),
                                    col("JaccardDistance")
                                )
    
    # TEST: Ensure that the approximate distances provides by the algorithm are symetric
    # assert check_distance_simetry(approx_Jaccard_distances)

    servers_with_cluster_df = servers.add_cluster_column(servers_df=servers_df, 
                                                 distances_rdd=approx_Jaccard_distances.rdd)

    """ server_distance_array = create_server_distance_array(distances_df=approx_Jaccard_distances,
                                                         server_names=server_names) """
        
    processes_with_depth_df = processes.add_cluster_request_path_and_cluster_to_depth(
        processes_df=processes_with_depth_df,
        servers_with_cluster_df=servers_with_cluster_df
        )
    processes_with_depth_df.show()