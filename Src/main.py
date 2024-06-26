from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.sql import Row
from pyspark.sql.functions import lit, udf, col, explode
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


def create_logs_with_depth_df(logs_df, process_df):
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
    request_paths_with_depth_df = process_df.withColumn("servers_depth", servers_depth_udf(col("request_path")))

    # Join the original logs_df with the request_paths_with_depth_df to get the servers_depth for each process_id
    logs_with_depth_df = logs_df.join(request_paths_with_depth_df, on="process_id")

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

    return logs_with_depth_df  

def get_string_server_connections(logs_with_depth_rdd):
    """..
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

if __name__ == "__main__":

    # Read log file and create original log df
    DATASET_NAME = "data_processes_v2.json"
    logs_df = spark.read.json("Data/"+DATASET_NAME)

    # Create the processes df with their associated request paths
    processes_df = spark.createDataFrame(get_processes_request_paths(logs_rdd=logs_df.rdd) \
                                         .map(lambda x: Row(process_id=x[0], request_path=x[1])))

    # Add depth_from in every log, indicating the depth of the server performing the Request or giving the Response.
    logs_with_depth_df = create_logs_with_depth_df(logs_df, processes_df)

    # Create servers df with an extra column connections that contains an array of strings.
    # String represent the connection tuples (Related_server, Request_type, Depth) but concatenating all elements as strings.
    servers_df = spark.createDataFrame(data=get_string_server_connections(logs_with_depth_rdd = logs_with_depth_df.rdd),
                                       schema=StructType([
                                                StructField("server_name", StringType(), True),
                                                StructField("connections", ArrayType(StringType()), True)
                                            ])
                                       )
    servers_df.show()

    distinct_connections = servers_df.select(explode(col("connections")).alias("connection")).distinct().count()
    print(f"Number of different connections: {distinct_connections}")

    # Add a column to servers df representing the identirty matrix of the set of connections (as numFeatures=distinct_connections)
    # If we see that number of distinct connections is too high we could set a maximun
    hashingTF = HashingTF(inputCol="connections", outputCol="features", numFeatures=distinct_connections)
    features_server_df = hashingTF.transform(servers_df)
    features_server_df.show()

    # Add a column to servers df indicating the hashes of the columns of the signature/representation matrix.
    mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
    model = mh.fit(features_server_df)
    transformedData = model.transform(features_server_df)
    transformedData.show()

    # Perform clustering with LSH by giving a Jaccard distance threshold.
    model.approxSimilarityJoin(datasetA=features_server_df, datasetB=features_server_df, 
                            threshold=0.7, distCol="JaccardDistance").select(
                                col("datasetA.server_name").alias("server_name_A"),
                                col("datasetB.server_name").alias("server_name_B"),
                                col("JaccardDistance")
                            ).show()   
    