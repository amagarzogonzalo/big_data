from pyspark.sql.functions import col

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