import numpy as np
from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

from setup_spark import session_spark
spark = session_spark()
sc = spark.sparkContext

def get_server_names(logs_rdd, without_user: bool = True):
    """
    Extracts distinct server names from the 'state_from' field in the logs RDD.
    
    Parameters:
    logs_rdd: RDD
        An RDD containing log entries.
    without_user: bool
        If True, the server name "user" will be removed from the result.
        
    Returns:
    list
        A list of distinct server names.
    """
    server_names = logs_rdd.map(lambda x: x['state_from']).distinct().collect()
    if without_user:
        server_names.remove("user")
    return server_names

def get_list_server_names(df):
    return [row.__getitem__("state_from") for row in df.select("state_from").distinct().collect()]

def request_path(process):
    """
    Constructs the request path for a given process from the logs.
    
    Parameters:
    process: list
        A list of log entries for a specific process.
        
    Returns:
    str
        The constructed request path.
    """
    #TODO: Refactor including a reduce function
    for log in process:
        if log['action'] == 'Request':
            if log['state_from'] == 'user':
                # Start the path with the user request
                path = ":".join([log['state_from'], log['state_to']])
            else:
                # Append to the path using '-' as a delimiter
                path = "-".join([path, ":".join([log['state_from'], log['state_to']])])
    return path

def get_processes_request_paths(logs_rdd):
    """
    Generates request paths for all processes in the logs RDD.
    
    Parameters:
    logs_rdd: RDD
        An RDD containing log entries.
        
    Returns:
    RDD
        An RDD of tuples where the first element is the process_id and the second element is the request path.
    """
    return logs_rdd.sortBy(lambda x: x['time']).groupBy(lambda x: x['process_id'])\
            .mapValues(request_path)

def servers_depth(request_path: str) -> dict:
    """
    Computes the depth of each server in a process given its request path.
    
    Parameters:
    request_path: str
        A string representing the request path.
        
    Returns:
    dict
        A dictionary with server names as keys and their depths as values.
    """
    import logging
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


def add_depth_to_df(logs_df, process_df):
    """
    Creates a logs DataFrame with an additional 'depth_from' column indicating the depth of of the server that performs the 
    Request or gives the Response.

    Parameters:
    logs_df: DataFrame
        A DataFrame containing the original log entries.
    process_df: DataFrame
        A DataFrame containing processes ids and request paths.

    Returns:
    DataFrame
        A DataFrame that includes the original log entries and an additional 'depth_from' column.
    """
    # Register UDF for servers_depth function
    servers_depth_udf = udf(servers_depth, MapType(StringType(), IntegerType()))

    # Apply the servers_depth function to the request_paths_df to create a new DataFrame with the server depths
    processes_with_depth_df = process_df.withColumn("servers_depth", servers_depth_udf(col("request_path")))

    # Join the original logs_df with the request_paths_with_depth_df to get the servers_depth for each process_id
    logs_with_depth_df = logs_df.join(processes_with_depth_df, on="process_id")

    # Define a UDF to extract the depth_from from the servers_depth dictionary
    def get_depth_from(servers_depth, state_from):
        return servers_depth.get(state_from, -1)  # Return -1 if the state_from is not found
    get_depth_from_udf = udf(get_depth_from, IntegerType())

    # Add the depth_from column to logs_with_depth_df
    logs_with_depth_df = logs_with_depth_df.withColumn(
        "depth_from",
        get_depth_from_udf(col("servers_depth"), col("state_from"))
    )

    # Drop the servers_depth column as it is no longer needed
    logs_with_depth_df = logs_with_depth_df.drop("servers_depth", "request_path")

    def add_servers_by_depth(processes_with_depth_df):
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
    processes_with_depth_df = add_servers_by_depth(processes_with_depth_df)
    processes_with_depth_df = processes_with_depth_df.drop("servers_depth")

    return logs_with_depth_df, processes_with_depth_df


def get_string_server_connections(logs_with_depth_rdd):
    """
    Generates a list of all the server connections for each server in the logs RDD.
    Each connection of a specific server X is a tuple of the form (related_server, request_type, depth) representing:
        - related_server: server with a request involving X and itself.
        - request_type: In if the request goes from related_server to X, Out if the request goes from X to related_server
        - depth of the server performing the request: depth(X) if request_type = Out, depth(related_server) if request_type = In
    Each connection is represented as an array obtained by concatenating all three elements in the tuple.
        
    Parameters:
    logs_with_depth_rdd: RDD
        An RDD containing log entries with depth information.

    Returns:
    RDD
        An RDD of tuples where each tuple contains a server name and a list of all server connections represented as strings.
    """
    server_connections = []
    # Create one entry in the array for each server except the user (incoming requests from user are captured anyways)
    for server_name in get_server_names(logs_rdd=logs_with_depth_rdd): 

        # Find incoming requests connections to the server and map them into string representations
        incoming_connections = set(logs_with_depth_rdd.filter(lambda x: x['action']=="Request" and x['state_to'] == server_name) \
                                .map(lambda x: (x['state_from'], 'In', x['depth_from']))
                                .map(lambda x: ''.join(map(str, x)))  \
                                .distinct().collect())
        
        # Find outgoing requests connections to the server and map them into string representations
        outgoing_connections = set(logs_with_depth_rdd.filter(lambda x: x['action']=="Request" and x['state_from'] == server_name) \
                                .map(lambda x: (x['state_to'], 'Out', x['depth_from']))
                                .map(lambda x: ''.join(map(str, x))) \
                                .distinct().collect())
                            
        server_connections.append((server_name, list(incoming_connections.union(outgoing_connections))))
    return sc.parallelize(server_connections)

def jaccard_similarity(a, b):
    assert type(a)==set and type(b)==set
    return len(a.intersection(b)) / len(a.union(b))

def jaccard_distance(a, b):
    return 1-jaccard_similarity(a, b)

def check_distance_simetry(distances):
    swapped_distances = distances.select(
        col("server_name_A").alias("server_name_B"),
        col("server_name_B").alias("server_name_A"),
        col("JaccardDistance").alias("swapped_JaccardDistance")
    )

    # Join the original DataFrame with the swapped DataFrame
    joined_df = distances.join(
        swapped_distances,
        (distances.server_name_A == swapped_distances.server_name_A) &
        (distances.server_name_B == swapped_distances.server_name_B)
    )

    # Check that there are now columns with different distances
    return joined_df.filter(col("JaccardDistance") != col("swapped_JaccardDistance")).rdd.isEmpty()

def lower_triangle_matrix_index(i, j):
    # Ensure that we are only accesing elements in the lower triangle matrix.
    assert j>=0 and i>j
    return int((i-1)*(i-2)*(1/2) + j)

def create_server_distance_array(distances_df, server_names):
    distance_array = np.zeros(shape=(int((len(server_names))*(len(server_names)-1)*(1/2)), ) )
    
    for i in range(1, len(server_names)):
        for j in range(0, i):
            search = distances_df.filter((col("server_name_A") == server_names[i]) & (col("server_name_B") == server_names[j]))

            # If there is no element in the distance df, then assume they are far away and set distance to 1.
            if search.isEmpty():
                distance_array[lower_triangle_matrix_index(i, j)] = 1
            else:
                distance_array[lower_triangle_matrix_index(i, j)] = search.collect()[0].__getitem__("JaccardDistance")
    return distance_array

def get_server_clusters(distances_rdd, server_names):
    def ordered_join(s1, s2):
        # Split the strings into lists
        servers = s1.split('+') + s2.split('+')
        # Sort by server_names indices and join back into a single string
        return '+'.join(sorted(servers, key=lambda x: server_names.index(x)))
    
    all_related_servers = distances_rdd.map(lambda x: x["server_name_A"]).distinct().collect()
    alone_servers = list(set(server_names) - set(all_related_servers)) 
    server_clusters = distances_rdd.filter(lambda x: x["JaccardDistance"] < 1e-3) \
                            .map(lambda x: (x['server_name_A'], x['server_name_B'])) \
                            .reduceByKey(ordered_join).collect()
    if len(alone_servers)>0:
        return server_clusters.join(sc.parallelize(alone_servers).map(lambda x: (x,x)))
    return server_clusters
 
def add_cluster_column(servers_df, distances_rdd):
    server_clusters = get_server_clusters(distances_rdd, 
                                          server_names = servers_df.select("server_name") \
                                                    .rdd.flatMap(lambda x: x).collect() )

    # Broadcast to all worker nodes a dictionary mapping server names to their clusters
    cluster_broadcast = servers_df.rdd.context.broadcast({server: cluster for server, cluster in server_clusters})
    
    # Define a UDF to get the cluster from the broadcasted dictionary
    def get_cluster(server_name):
        return cluster_broadcast.value.get(server_name, server_name)
    get_cluster_udf = udf(get_cluster, StringType())
    
    return servers_df.withColumn("cluster", get_cluster_udf(servers_df["server_name"]))

import string
from itertools import product

def get_cluster_identifiers(num_clusters):
    """ Generate as many unique identifiers using A-Z characters as number of clusters  """
    characters = string.ascii_uppercase
    # Compute how many characters will the identifiers have
    nr_characters_in_id = 1
    while num_clusters > len(characters)**nr_characters_in_id:
        nr_characters_in_id += 1
    # Select the first n=num_clusters identifiers with as many characters as computed.
    return ["".join(s) for s in product(characters, repeat=nr_characters_in_id)][:num_clusters]

def add_cluster_id_column(servers_with_cluster_df):
    unique_clusters = servers_with_cluster_df.select("cluster").distinct().rdd.flatMap(lambda x: x).collect()

    # Create a map from clusters to id's and broadcast it to all nodes.
    clusters_to_id = {cluster: identifier for cluster, identifier in zip(unique_clusters, 
                                                                         get_cluster_identifiers(num_clusters=len(unique_clusters)))}
    broadcast_cluster_to_id = sc.broadcast(clusters_to_id)

    def get_identifier(cluster):
        return broadcast_cluster_to_id.value[cluster]   
    get_identifier_udf = udf(get_identifier, StringType())

    return servers_with_cluster_df.withColumn("cluster_id", get_identifier_udf(col("cluster")))


def add_cluster_request_path_and_cluster_to_depth(processes_df, servers_with_cluster_df):
    # Convert the servers_mapping_df to a dictionary and broadcast it
    server_to_cluster_dict = dict(servers_with_cluster_df.rdd.map(lambda x: (x['server_name'], x['cluster_id'])).distinct().collect())
    broadcast_server_to_cluster_dict = sc.broadcast(server_to_cluster_dict)

    # Step 1: Replace servers in depth_to_servers
    @udf(returnType=ArrayType(StringType()))
    def replace_servers_with_clusts_list(server_list):
        mapping = broadcast_server_to_cluster_dict.value
        print(server_list)
        return [mapping.get(server[0], server[0]) for server in server_list]
    
    # Add depth_to_clusters_column
    processes_df = processes_df.withColumn(
        "depth_to_clusters", 
        map_from_arrays(map_keys(col("depth_to_servers")), 
                        replace_servers_with_clusts_list(map_values(col("depth_to_servers"))))
    )

    # Step 2: Replace servers by clusters in request_path
    @udf(returnType=ArrayType(StringType()))
    def replace_servers_with_clusters(segments):
        mapping = broadcast_server_to_cluster_dict.value
        return [":".join([mapping.get(server, server) for server in segment.split(":")]) for segment in segments]

    # Split request_path into individual edges, apply udf function an reconstruct the request path
    processes_df = processes_df.withColumn(
        "cluster_request_path", 
        concat_ws("-", replace_servers_with_clusters(split(col("request_path"), "-")))
    )

    return processes_df


if __name__ == "__main__":

    # Read log file and create original log d
    DATASET_NAME = "data_processes_v2.json"
    logs_df = spark.read.json("Data/"+DATASET_NAME)

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

    servers_with_cluster_df = add_cluster_column(servers_df=servers_df, 
                                                 distances_rdd=approx_Jaccard_distances.rdd)
    servers_with_cluster_df = add_cluster_id_column(servers_with_cluster_df)

    """ server_distance_array = create_server_distance_array(distances_df=approx_Jaccard_distances,
                                                         server_names=server_names) """
        
    processes_with_depth_df = add_cluster_request_path_and_cluster_to_depth(
        processes_df=processes_with_depth_df,
        servers_with_cluster_df=servers_with_cluster_df
        )
    processes_with_depth_df.show()