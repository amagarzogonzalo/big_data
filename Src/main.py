import numpy as np
from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

from processes import *
from servers import *
from dbscan import process_dbscan
from setup_spark import session_spark

if __name__ == "__main__":
    spark = session_spark()
    sc = spark.sparkContext

    # Read log file and create original log d
    DATASET_NAME = "data_processes_v2.json"
    logs_df = spark.read.json("../Data/"+DATASET_NAME)

    # Create the processes df with their associated request paths
    processes_df = spark.createDataFrame(get_processes_request_paths(logs_rdd=logs_df.rdd) \
                                         .map(lambda x: Row(process_id=x[0], request_path=x[1])))

    # Add depth_from in every log, indicating the depth of the server performing the Request or giving the Response.
    logs_with_depth_df, processes_with_depth_df = add_depth_to_df(logs_df, processes_df)

    # Create servers df with an extra column connections that contains an array of strings.
    # String represent the connection tuples (Related_server, Request_type, Depth) but concatenating all elements as strings.
    server_names = get_server_names(logs_rdd=logs_df.rdd)
    servers_df = spark.createDataFrame(data=get_string_server_connections(logs_with_depth_rdd = logs_with_depth_df.rdd),
                                       schema=StructType([
                                                StructField("server_name", StringType(), True),
                                                StructField("connections", ArrayType(StringType()), True)
                                            ])
                                       )

    nr_distinct_connections = servers_df.select(explode(col("connections")).alias("connection")).distinct().count()

    # Add a column to servers df representing the identirty matrix of the set of connections (as numFeatures=distinct_connections)
    # If we see that number of distinct connections is too high we could set a maximun
    server_hashingTF = HashingTF(inputCol="connections", outputCol="features", numFeatures=nr_distinct_connections)
    features_server_df = server_hashingTF.transform(servers_df)

    # Add a column to servers df indicating the hashes of the columns of the signature/representation matrix.
    server_mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
    #TODO: Decide the number of hash tables
    server_model = server_mh.fit(features_server_df)
    transformedData = server_model.transform(features_server_df)  

    # Perform clustering with LSH by giving a Jaccard distance threshold.
    approx_Jaccard_distances = server_model.approxSimilarityJoin(
                                datasetA=features_server_df, datasetB=features_server_df, 
                                threshold=0.5, distCol="JaccardDistance"
                                ).select(
                                    col("datasetA.server_name").alias("server_name_A"),
                                    col("datasetB.server_name").alias("server_name_B"),
                                    col("JaccardDistance")
                                )
    
    # TEST: Ensure that the approximate distances provides by the algorithm are symetric
    # assert check_distance_simetry(approx_Jaccard_distances)

    servers_with_cluster_df = add_cluster_column(servers_df=servers_df, 
                                                 distances_rdd=approx_Jaccard_distances.rdd)

    """ server_distance_array = create_server_distance_array(distances_df=approx_Jaccard_distances,
                                                         server_names=server_names) """
        
    processes_with_depth_df = add_cluster_request_path_and_cluster_to_depth(
        processes_df=processes_with_depth_df,
        servers_with_cluster_df=servers_with_cluster_df
        )
   
    processes_with_elements_df = add_processes_elements(processes_df=processes_with_depth_df,
                                                     logs_df=logs_df,
                                                     servers_with_cluster_df=servers_with_cluster_df)
    #processes_with_elements_df.show()

    process_hashingTF = HashingTF(inputCol="elements", outputCol="features", numFeatures=512)
    features_process_df = process_hashingTF.transform(processes_with_elements_df)

    # Add a column to servers df indicating the hashes of the columns of the signature/representation matrix.
    process_mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
    process_model = process_mh.fit(features_process_df)
    transformedData = process_model.transform(features_process_df)  

    """ approx_process_distances = process_model.approxSimilarityJoin(
                            datasetA=features_process_df.drop("depth_to_servers", "depth_to_clusters"),
                            datasetB=features_process_df.drop("depth_to_servers", "depth_to_clusters"),
                            threshold=0.3, distCol="JaccardDistance"
                            ).select(
                                col("datasetA.process_id").alias("id_A"),
                                col("datasetB.process_id").alias("id_B"),
                                col("JaccardDistance")
                            ) """
    
    process_clusters = process_dbscan(spark=spark, 
                                      df=features_process_df.drop("depth_to_servers", "depth_to_clusters"),
                                      approx_dist_model=process_model,
                                      epsilon=0.4,
                                      min_pts= 5
                                      )
    process_clusters.show()