import string
import itertools

from pyspark.sql.functions import *
from pyspark.sql.types import *

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
    return logs_with_depth_rdd.context.parallelize(server_connections)


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
        return server_clusters.join(distances_rdd.context.parallelize(alone_servers).map(lambda x: (x,x)))
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
    
    servers_with_cluster = servers_df.withColumn("cluster", get_cluster_udf(servers_df["server_name"]))

    def get_cluster_identifiers(num_clusters):
        """ Generate as many unique identifiers using A-Z characters as number of clusters  """
        characters = string.ascii_uppercase
        # Compute how many characters will the identifiers have
        nr_characters_in_id = 1
        while num_clusters > len(characters)**nr_characters_in_id:
            nr_characters_in_id += 1
        # Select the first n=num_clusters identifiers with as many characters as computed.
        return ["".join(s) for s in itertools.product(characters, repeat=nr_characters_in_id)][:num_clusters]

    def add_cluster_id_column(servers_with_cluster_df):
        unique_clusters = servers_with_cluster_df.select("cluster").distinct().rdd.flatMap(lambda x: x).collect()

        # Create a map from clusters to id's and broadcast it to all nodes.
        clusters_to_id = {cluster: identifier for cluster, identifier in zip(unique_clusters, 
                                                                            get_cluster_identifiers(num_clusters=len(unique_clusters)))}
        broadcast_cluster_to_id = servers_with_cluster_df.rdd.context.broadcast(clusters_to_id)

        def get_identifier(cluster):
            return broadcast_cluster_to_id.value[cluster]   
        get_identifier_udf = udf(get_identifier, StringType())

        return servers_with_cluster_df.withColumn("cluster_id", get_identifier_udf(col("cluster")))
    
    return add_cluster_id_column(servers_with_cluster_df=servers_with_cluster)