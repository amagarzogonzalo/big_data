import logging

from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.sql.functions import *
from pyspark.sql.types import *

from dbscan import process_dbscan
from servers import get_cluster_logs_df

def create_processes_df(spark, logs_df):

    def request_path(process):
        for log in process:
            if log['action'] == 'Request':
                if log['state_from'] == 'user':
                    # Start the path with the user request
                    path = ":".join([log['state_from'], log['state_to']])
                else:
                    # Append to the path using '-' as a delimiter
                    path = "-".join([path, ":".join([log['state_from'], log['state_to']])])
        return path

    def euler_string(process):
        euler_string = ''
        for log in process:
            if log['action'] == 'Request':
                if log['state_from'] == 'user':
                    euler_string = '1'+log['state_to']
                else:
                    euler_string = '-'.join([euler_string, '1'+log['state_to']])
            elif log['action'] == 'Response':
                euler_string = '-'.join([euler_string, '0'+log['state_from']])
        return euler_string

    def process_row(process):
        return (request_path(process), euler_string(process))

    return spark.createDataFrame(logs_df.rdd.sortBy(lambda x: x['time']) \
                            .groupBy(lambda x: x['process_id']) \
                            .mapValues(process_row) \
                            .map(lambda x: Row(process_id=x[0], 
                                               request_path=x[1][0], 
                                               euler_string=x[1][1]))
                            )


def add_depth_features(logs_df, processes_df):
    """
    Creates a logs DataFrame with an additional 'depth_from' column indicating the depth of of the server that performs the 
    Request or gives the Response.

    Parameters:
    logs_df: DataFrame
        A DataFrame containing the original log entries.
    process_df: DataFrame
        A DataFrame containing processes ids and request paths.

    Returns:
    logs_with_depth_df: DataFrame
        A DataFrame that includes the original log entries and the additional 'depth_from' column.
    processes_with_depth_df: DataFrame
    """

    # STEP 1: Operations on processes

    @udf(returnType=MapType(StringType(), IntegerType()))
    def servers_depth(request_path: str):
        """
        Computes the depth of each server in a process given its request path.
        
        Parameters:
        request_path: str
            A string representing the request path.
            
        Returns:
        dict
            A dictionary with server names as keys and their depths as values.
        """
        try:
            # Split the request path into individual requests
            requests = [request.split(":") for request in request_path.split("-")]

            # Ensure the path starts from the user. True if ordered in time
            assert requests[0][0] == 'user' 
            # Initialize the server depth with 'user' at depth 0
            servers_depth = {'user': 0}

            for request in requests:
                # Ensure the current state_from is already in the servers_depth
                assert request[0] in servers_depth.keys(), f"Key '{request[0]}' not found in servers_depth keys: {servers_depth.keys()}"
                
                # Set the depth of the state_to server
                if request[1] not in servers_depth.keys():
                    servers_depth[request[1]] = servers_depth[request[0]] + 1
            return servers_depth
        
        except Exception as e:
            logging.error(f"Error processing request_path '{request_path}': {str(e)}")
            return {}


    # Apply the servers_depth function to create a new column in the Datafrane
    processes_with_depth_df = processes_df.withColumn(
        "servers_depth", 
        servers_depth(col("request_path"))
    )

    def add_depth_to_servers_map(processes_with_depth_df):
        # Explode the servers_depth map into separate rows
        exploded_df = processes_with_depth_df.select(
            col("process_id"),
            explode(col("servers_depth")).alias("server", "depth")
        )

        # Group by process_id and depth, then collect servers into a list
        grouped_df = exploded_df.groupBy("process_id", "depth").agg(
            collect_list("server").alias("servers")
        )

        # Create a map from depth to servers
        depth_to_servers_df = grouped_df.groupBy("process_id").agg(
            expr("map_from_entries(collect_list(struct(depth, servers)))").alias("depth_to_servers")
        )

        # Join back with the original DataFrame
        return processes_with_depth_df.join(depth_to_servers_df, on="process_id", how="inner")

    # Change Map(Server: Depth) to Map(Depth: List[Server])
    processes_with_depth_df = add_depth_to_servers_map(processes_with_depth_df)

    # STEP 2: Operations on Logs

    # Join the original logs_df with the request_paths_with_depth_df to get the servers_depth for each process_id
    logs_with_depth_df = logs_df.join(processes_with_depth_df, on="process_id")

    # Extract the depth_from from the servers_depth dictionary and add it to to logs_with_depth_df
    @udf(returnType=IntegerType())
    def get_depth_of_state_from(servers_depth, state_from):
        return servers_depth.get(state_from, -1)  # Return -1 if the state_from is not found
    
    logs_with_depth_df = logs_with_depth_df.withColumn(
        "depth_from",
        get_depth_of_state_from(col("servers_depth"), col("state_from"))
    )

    # Drop the servers_depth column as it is no longer needed
    logs_with_depth_df = logs_with_depth_df.drop("servers_depth", "request_path")
    processes_with_depth_df = processes_with_depth_df.drop("servers_depth")

    return logs_with_depth_df, processes_with_depth_df

def add_cluster_features(processes_df, servers_with_cluster_df):
    # Convert the servers_mapping_df to a dictionary and broadcast it
    server_to_cluster_dict = dict(servers_with_cluster_df.rdd.map(lambda x: (x['server_name'], x['cluster_id'])).distinct().collect())
    broadcast_server_to_cluster_dict = processes_df.rdd.context.broadcast(server_to_cluster_dict)

    # Step 1: Replace servers in depth_to_servers
    @udf(returnType=ArrayType(StringType()))
    def replace_servers_with_clusts_list(values_list):
        mapping = broadcast_server_to_cluster_dict.value
        return [[mapping.get(server, server)for server in server_depth_list] for server_depth_list in values_list]
   
    # Add depth_to_clusters_column
    processes_df = processes_df.withColumn(
        "depth_to_clusters", 
        map_from_arrays(map_keys(col("depth_to_servers")), 
                        replace_servers_with_clusts_list(map_values(col("depth_to_servers"))))
    )

    # Step 2: Replace servers by clusters in request_path and euler string

    @udf(returnType=ArrayType(StringType()))
    def replace_request_path(segments):
        mapping = broadcast_server_to_cluster_dict.value
        return [":".join([mapping.get(server, server) for server in segment.split(":")]) for segment in segments]

    # Split request_path into individual edges, apply udf function an reconstruct the request path
    processes_df = processes_df.withColumn(
        "cluster_request_path", 
        concat_ws("-", replace_request_path(split(col("request_path"), "-")))
    )

    @udf(returnType=ArrayType(StringType()))
    def replace_euler_sting(segments):
        mapping = broadcast_server_to_cluster_dict.value
        return [segment[0] + mapping.get(segment[1:], segment[1:]) for segment in segments]

    processes_df = processes_df.withColumn(
        "cluster_euler_string", 
        concat_ws("-", replace_euler_sting(split(col("euler_string"), "-")))
    )
    return processes_df


def add_processes_elements(spark, processes_df, cluster_logs_df):

    # Change state_to and state_from in logs to their clusters ids
    result_df = processes_df.join(cluster_logs_df.filter(cluster_logs_df["action"]=="Request"), on="process_id"     # join processes with requests
                            ).groupBy("process_id", "cluster_from" 
                            ).agg(collect_list("cluster_to").alias("cluster_to_list")) # collect list of server_to
    
    # Broadcast a dictionary that assigns to a pair process_id, cluster_from the list of all cluster_to
    broadcast_dict = spark.sparkContext.broadcast(
        {row['process_id']+'_'+row['cluster_from']: row['cluster_to_list'] for row in result_df.collect()}
    ) 

    @udf(returnType=ArrayType(StringType()))
    def create_elements(process_id, depth_to_clusters):
        mapping = broadcast_dict.value
        result = []
        for depth, clusters in depth_to_clusters.items():
            for cluster in clusters:
                key = process_id + '_' + cluster
                if key in mapping:
                    result.append(str(depth) + '_' + cluster + ":" + ','.join(mapping.get(key,key)))
        return result

    processes_df = processes_df.withColumn(
        "cluster_elements", 
        create_elements(col("process_id"), col("depth_to_clusters"))
    )

    return processes_df

def equal_processes(spark, processes_with_elements_df, cluster_logs_df, dataset_name):

    equal_processes = processes_with_elements_df.groupBy("cluster_euler_string"
                                                         ).agg(collect_list("process_id").alias("equal_processes")) \
                                                .withColumn("group_processes_id", monotonically_increasing_id())\
                                                
    exploded_processes = equal_processes.withColumn("process", explode("equal_processes"))

    logs_with_grouped_processes = cluster_logs_df.join(
        exploded_processes,
        cluster_logs_df.process_id == exploded_processes.process,
        how="left"
    ).select(
        col("cluster_from").alias("state_from"),
        col("cluster_to").alias("state_to"),
        col("time").cast(IntegerType()).alias("time"),
        col("action"),
        col("group_processes_id").alias("process_id")
    ).orderBy("time"
    ).groupBy("process_id", "state_from", "state_to", "action"
    ).agg(first(col("time")).alias("time")
    ).select(
        col("state_from"),
        col("state_to"),
        col("time"),
        col("action"),
        col("process_id").alias("process_id")
    ).orderBy("time")

    logs_with_grouped_processes.show()

    logs_with_grouped_processes.write.json(path="../Data/" + dataset_name+ "_part1Output.txt",
                                           mode="overwrite",
                                           lineSep=',')

    @udf(returnType=StringType())
    def format_text(equal_processes):
        text = f'Group: {equal_processes}\n'
        for process in equal_processes:
            text += f'{process}:\n'
            text += f'      List of all logs of {process}\n'
        return text
        
    


    """ path = "..Data/" + dataset_name
    with open(path + "_part1Output", "w") as f: """



    """ process_hashingTF = HashingTF(inputCol="cluster_elements", outputCol="features", numFeatures=512)
    features_process_df = process_hashingTF.transform(processes_with_elements_df)

    # Add a column to servers df indicating the hashes of the columns of the signature/representation matrix.
    process_mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
    process_model = process_mh.fit(features_process_df)
    #transformedData = process_model.transform(features_process_df)  
    process_clusters = process_dbscan(spark=spark, 
                                      df=features_process_df.drop("depth_to_servers", "depth_to_clusters"),
                                      approx_dist_model=process_model,
                                      epsilon=0.02,
                                      min_pts= 2
                                      ).select(col("process_id").alias("process_id"), col("component"))
    process_clusters.show()

    process_clusters = process_clusters.join(processes_with_elements_df, 
                                             process_clusters.process_id == processes_with_elements_df.process_id, 
                                             how="left").select(
        col("process_id"), col("component"), col("cluster_euler_string")
    )

    components = process_clusters.groupBy("component", "cluster_euler_string").agg(collect_list("point").alias("processes_in_component"))
    components.show() """
  