from functools import reduce
from itertools import combinations
import math

from graphframes import *
from pyspark.sql import Row, functions as F


H = {}

def __distance_from_pivot(pivot, dist, epsilon, operations):
    def distance(x):
        pivot_dist = dist(x.value, pivot)
        if operations is not None:
            operations.add()
        partition_index = math.floor(pivot_dist / epsilon)
        rows = [Row(id=x.id, value=x.value, pivot_dist=dist(x.value, pivot))]
        out = [(partition_index, rows),
               (partition_index + 1, rows)]
        return out
    return distance


def __scan(epsilon, dist, operations):
    def scan(x):
        # out dictionary would have point id as key and a set of point ids who are within epsilon distance to
        # key point id. value is basically its neighbors
        out = {}
        # 0th index of x is partition_index
        # 1st index of x is data points
        partition_data = x[1]
        partition_len = len(partition_data)
        for i in range(partition_len):
            for j in range(i + 1, partition_len):
                if operations is not None:
                    operations.add()
                if dist(partition_data[i].value, partition_data[j].value) < epsilon:
                    # both i and j are within epsilon distance to each other
                    if partition_data[i].id in out:
                        out[partition_data[i].id].add(partition_data[j].id)
                    else:
                        out[partition_data[i].id] = set([partition_data[j].id])
                    if partition_data[j].id in out:
                        out[partition_data[j].id].add(partition_data[i].id)
                    else:
                        out[partition_data[j].id] = set([partition_data[i].id])
        # returns point and its neighbor as tuple
        return [Row(item[0], item[1]) for item in out.items()]

    return scan


def __label(min_pts):
    def label(x):
        if len(x[1]) + 1 >= min_pts:
            # use id as cluster label
            cluster_label = x[0]
            # return True for core point
            out = [(x[0], [(cluster_label, True)])]
            for idx in x[1]:
                # return False for base point
                out.append((idx, [(cluster_label, False)]))
            return out
        return []

    return label


def __combine_labels(x):
    # 0th element is the id of point
    # 1st element is the list of tuples with cluster and core point label
    point = x[0]
    core_point = False
    cluster_labels = x[1]
    clusters = []
    for (label, point_type) in cluster_labels:
        if point_type is True:
            core_point = True
        clusters.append(label)
    # if core point keep all cluster otherwise only one
    return point, clusters if core_point is True else [clusters[0]], core_point


def process(spark, df, epsilon, min_pts, dist, checkpoint_dir, operations=None):
    """
    Process given dataframe with DBSCAN parameters
    :param spark: spark session
    :param df: input data frame where each row has id and value keys
    :param epsilon: DBSCAN parameter for distance
    :param min_pts: DBSCAN parameter for minimum points to define core point
    :param dist: method to calculate distance. Only distance metric is supported.
    :param dim: number of dimension of input data
    :param checkpoint_dir: checkpoint path as required by Graphframe
    :param operations: class for managing accumulator to calculate number of distance operations
    :return: A dataframe of point id, cluster component and boolean indicator for core point
    """
    zero = df.rdd.takeSample(False, 1)[0].value
    combine_cluster_rdd = df.rdd.\
        flatMap(__distance_from_pivot(zero, dist, epsilon, operations)). \
        reduceByKey(lambda x, y: x + y).\
        flatMap(__scan(epsilon, dist, operations)). \
        reduceByKey(lambda x, y: x.union(y)).\
        flatMap(__label(min_pts)).\
        reduceByKey(lambda x, y: x + y).map(__combine_labels).cache()
    id_cluster_rdd = combine_cluster_rdd.\
        map(lambda x: Row(point=x[0], cluster_label=x[1][0], core_point=x[2]))
    try:
        id_cluster_df = id_cluster_rdd.toDF()
        vertices = combine_cluster_rdd.\
            flatMap(lambda x: [Row(id=item) for item in x[1]]).toDF().distinct()
        edges = combine_cluster_rdd. \
            flatMap(lambda x: [Row(src=item[0], dst=item[1])
                               for item in combinations(x[1], 2)]). \
            toDF().distinct()
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
        g = GraphFrame(vertices, edges)
        connected_df = g.connectedComponents()
        id_cluster_df = id_cluster_df.\
            join(connected_df, connected_df.id == id_cluster_df.cluster_label). \
            select("point", "component", "core_point")
        return id_cluster_df
    except ValueError:
        return None

def __get_dist(broadcast_distances):
    def dist(x, y):
        """,
        x: id,
        y: id
        """
        key = (x,y)
        # Return the JaccardDistance if the key exists, else return 1
        return broadcast_distances.value.get(key, 1)
    return dist
        
def minhash_dbscan(spark, df, approx_dist_model, epsilon, min_pts, 
                   id_column_name="group_processes_id", value_column_name="group_processes_id"):

    distances = approx_dist_model.approxSimilarityJoin(
                        datasetA=df,
                        datasetB=df,
                        threshold=epsilon, distCol="distance"
                        ).select(
                            F.col("datasetA."+id_column_name).alias("id_A"),
                            F.col("datasetB."+id_column_name).alias("id_B"),
                            F.col("distance")
                        )
    broadcast_distances = spark.sparkContext.broadcast(
        { (row.id_A, row.id_B): row.distance for row in distances.collect() }
    )
    df = df.withColumnRenamed(id_column_name, "id")
    if id_column_name == value_column_name:
        df = df.withColumn("value", df.id)
    else:
        df = df.withColumnRenamed(value_column_name, "value")

    dist = __get_dist(broadcast_distances=broadcast_distances)
    return process(spark, df, epsilon, min_pts, dist, checkpoint_dir="checkpoint")

from utils import process_string_edit_distance
def __get_process_edit_distance():
    def dist(s, t):
        return process_string_edit_distance(s, t)
    return dist

def process_edit_distance_dbscan(spark, df, epsilon, min_pts, 
                                 id_column_name="group_processes_id", value_column_name="cluster_euler_string"):
    df = df.withColumnRenamed(id_column_name, "id")
    df = df.withColumnRenamed(value_column_name, "value")
    dist = __get_process_edit_distance()
    return process(spark, df, epsilon, min_pts, dist, checkpoint_dir="checkpoint")

__all__ = [process, minhash_dbscan]