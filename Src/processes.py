from pyspark.sql.functions import *
from pyspark.sql.types import *

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

@udf(returnType=MapType(StringType(), IntegerType()))
def servers_depth_udf(request_path: str):
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


def add_depth_to_df(logs_df, processes_df):
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

    # Apply the servers_depth function to the request_paths_df to create a new DataFrame with the server depths
    processes_with_depth_df = processes_df.withColumn(
        "servers_depth", 
        servers_depth_udf(col("request_path"))
    )

    # Join the original logs_df with the request_paths_with_depth_df to get the servers_depth for each process_id
    logs_with_depth_df = logs_df.join(processes_with_depth_df, on="process_id")

    # Extract the depth_from from the servers_depth dictionary and add it to to logs_with_depth_df
    @udf(returnType=IntegerType())
    def get_depth_from_udf(servers_depth, state_from):
        return servers_depth.get(state_from, -1)  # Return -1 if the state_from is not found
    logs_with_depth_df = logs_with_depth_df.withColumn(
        "depth_from",
        get_depth_from_udf(col("servers_depth"), col("state_from"))
    )

    # Drop the servers_depth column as it is no longer needed
    logs_with_depth_df = logs_with_depth_df.drop("servers_depth", "request_path")

    def add_depth_to_servers(processes_with_depth_df):
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
    processes_with_depth_df = add_depth_to_servers(processes_with_depth_df)
    processes_with_depth_df = processes_with_depth_df.drop("servers_depth")

    return logs_with_depth_df, processes_with_depth_df

def add_cluster_request_path_and_cluster_to_depth(processes_df, servers_with_cluster_df):
    # Convert the servers_mapping_df to a dictionary and broadcast it
    server_to_cluster_dict = dict(servers_with_cluster_df.rdd.map(lambda x: (x['server_name'], x['cluster_id'])).distinct().collect())
    broadcast_server_to_cluster_dict = processes_df.rdd.context.broadcast(server_to_cluster_dict)

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