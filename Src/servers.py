import itertools
import math
import string

from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

from utils import lower_triangle_matrix_index

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


def create_servers_df(spark, logs_with_depth_df):

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
        for server_name in get_server_names(logs_rdd=logs_with_depth_rdd, without_user=False): 

            # Find incoming requests connections to the server and map them into string representations
            incoming_connections = set(logs_with_depth_rdd.filter(lambda x: x['action']=="Request" and x['state_to'] == server_name) \
                                    .map(lambda x: (x['state_from'], 'In', x['depth_from']))
                                    .map(lambda x: '_'.join(map(str, x)))  \
                                    .distinct().collect())
            
            # Find outgoing requests connections to the server and map them into string representations
            outgoing_connections = set(logs_with_depth_rdd.filter(lambda x: x['action']=="Request" and x['state_from'] == server_name) \
                                    .map(lambda x: (x['state_to'], 'Out', x['depth_from']))
                                    .map(lambda x: '_'.join(map(str, x))) \
                                    .distinct().collect())
                                
            server_connections.append((server_name, list(incoming_connections.union(outgoing_connections))))
        return spark.sparkContext.parallelize(server_connections)

    return spark.createDataFrame(
        data=get_string_server_connections(
            logs_with_depth_rdd = logs_with_depth_df.rdd),
            schema=StructType([
                StructField("server_name", StringType(), True),
                StructField("connections", ArrayType(StringType()), True)
                ])
                )

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

def cluster_servers(spark, servers_df):

    # Perform MinHash LSH on the column of connections
    nr_distinct_connections = servers_df.select(explode(col("connections")).alias("connection")).distinct().count()
    
    hashingTF = HashingTF(inputCol="connections", outputCol="features", numFeatures=nr_distinct_connections)
    features_server_df = hashingTF.transform(servers_df)

    mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
    server_model = mh.fit(features_server_df)
    #transformedData = server_model.transform(features_server_df) 
        
    # Perform clustering with LSH by giving a Jaccard distance threshold.
    close_server_distances = server_model.approxSimilarityJoin(
                                datasetA=features_server_df, datasetB=features_server_df, 
                                threshold=0.05, distCol="JaccardDistance"
                                ).select(
                                    col("datasetA.server_name").alias("server_name_A"),
                                    col("datasetB.server_name").alias("server_name_B"),
                                    col("JaccardDistance")
                                )
    
    # TEST: Ensure that the approximate distances provides by the algorithm are symetric
    # assert check_distance_simetry(close_server_distances)

    def add_cluster_column(servers_df, close_distances_df, cluster_threshold = 0.05):
        
        @udf(returnType=StringType())
        def ordered_join(servers):
            return '+'.join(sorted(servers))
        
        clusters_df = close_distances_df.select("server_name_A", "server_name_B") \
                .groupBy("server_name_A") \
                .agg(collect_list("server_name_B").alias("related_servers")) \
                .withColumn("cluster", ordered_join(col("related_servers")))

        all_related_servers_df = close_distances_df.select("server_name_A").distinct()
        # Select all rows in servers_df that do not match with any row in all_related_servers_df
        alone_servers_df = servers_df.join(
            all_related_servers_df,
            servers_df.server_name == all_related_servers_df.server_name_A, 
            "left_anti"
        ).withColumn("cluster", col("server_name"))
        
        # Union clusters_df with alone_servers_df to get all servers with their clusters
        clusters_df = clusters_df.select(col("server_name_A").alias("server_name"), "cluster") \
                                    .unionByName(alone_servers_df.select(col("server_name"), "cluster"))

        return servers_df.join(clusters_df, on="server_name", how="left")

    servers_with_cluster_df = add_cluster_column(servers_df = servers_df,
                                                 close_distances_df=close_server_distances)

    def add_cluster_id_column(servers_with_cluster_df):

        def get_cluster_identifiers(num_clusters):
            """ Generate as many unique identifiers using A-Z characters as number of clusters  """
            characters = string.ascii_uppercase
            # Compute how many characters will the identifiers have
            nr_characters_in_id = math.ceil(math.log(num_clusters, len(characters)))
            # Select the first n=num_clusters identifiers with as many characters as computed.
            return ["".join(s) for s in itertools.product(characters, repeat=nr_characters_in_id)][:num_clusters]

        # The number of unique clusters will be the number of unique ids.
        unique_clusters = servers_with_cluster_df.select("cluster").distinct().rdd.flatMap(lambda x: x).collect()

        # Create a map from clusters to id's and broadcast it to all nodes.
        broadcast_cluster_to_id = spark.sparkContext.broadcast(
            {cluster: identifier for cluster, identifier in zip(
                unique_clusters, 
                get_cluster_identifiers(num_clusters=len(unique_clusters)))}
        )

        # Create an udf for the broadcasted dict and apply it to the column
        @udf(returnType=StringType())
        def get_identifier(cluster):
            if cluster == "user":
                return "user"
            return broadcast_cluster_to_id.value[cluster]   
        
        return servers_with_cluster_df.withColumn("cluster_id", get_identifier(col("cluster")))

    servers_with_cluster_df = add_cluster_id_column(servers_with_cluster_df)  

    server_to_cluster_dict = dict(servers_with_cluster_df.rdd.map(lambda x: (x['server_name'], x['cluster_id'])).distinct().collect())
    broadcast_server_to_cluster_dict = spark.sparkContext.broadcast(server_to_cluster_dict)

    @udf(returnType=ArrayType(StringType()))
    def replace_connections(connections):
        mapping = broadcast_server_to_cluster_dict.value
        def replace_connection(connection):
            splitted_con = connection.split('_')
            return "_".join([mapping.get(splitted_con[0], splitted_con[0]), splitted_con[1], splitted_con[2]])
        return [replace_connection(con) for con in connections]
   
    servers_with_cluster_df = servers_with_cluster_df.withColumn(
        "cluster_connections", 
        replace_connections(col("connections"))
    )
    """ servers_with_cluster_df = servers_with_cluster_df.groupBy("cluster_id") \
    .agg(array_distinct(flatten(collect_list("cluster_connections"))).alias("merged_cluster_connections")).show() """

    hashingTF = HashingTF(inputCol="merged_cluster_connections", outputCol="features", numFeatures=nr_distinct_connections)
    features_server_df = hashingTF.transform(
        servers_with_cluster_df.groupBy("cluster_id") \
            .agg(array_distinct(flatten(collect_list("cluster_connections"))).alias("merged_cluster_connections"))
        )

    mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
    server_model = mh.fit(features_server_df)

    cluster_distances_df = server_model.approxSimilarityJoin(
                                datasetA=features_server_df, datasetB=features_server_df, 
                                threshold=0.6, distCol="JaccardDistance"
                                ).select(
                                    col("datasetA.cluster_id").alias("cluster_id_A"),
                                    col("datasetB.cluster_id").alias("cluster_id_B"),
                                    col("JaccardDistance")
                                )

    return servers_with_cluster_df, cluster_distances_df

def get_cluster_logs_df(logs_df, servers_with_cluster_df):
    cluster_logs_df = logs_df.join(
            servers_with_cluster_df, 
            logs_df.state_from == servers_with_cluster_df.server_name
        ).select(
            logs_df["*"], servers_with_cluster_df["cluster_id"].alias("cluster_from")
        ).drop("state_from")
    cluster_logs_df = cluster_logs_df.join(
        servers_with_cluster_df, 
        cluster_logs_df.state_to == servers_with_cluster_df.server_name
        ).select(
            cluster_logs_df["*"], servers_with_cluster_df["cluster_id"].alias("cluster_to")
        ).drop("state_to")
    return cluster_logs_df