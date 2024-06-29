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

def process_string_edit_distance(s, t, H, broadcasted_distances):
    assert type(s)==str and type(t)==str
    S = []

    # If the pair is already in the associative array, returns its distance.
    if (s, t) in H.keys():
        return H[(s,t)]
    
    # We work with edges as base elements
    splitted_s = s.split("-")
    splitted_t = t.split("-")

    # Trivial cases
    if len(s)==0:
        return 2*len(splitted_t)
    if len(t)==0:
        return 2*len(splitted_s)
    
    # Recursivity with one edge less
    s_prime = "-".join(splitted_s[:-1])
    t_prime = "-".join(splitted_t[:-1])
    k_a, H = process_string_edit_distance(s_prime, t_prime, H)  #substitution
    k_b, H = process_string_edit_distance(s_prime, t, H) + 2    #insertion
    k_c, H = process_string_edit_distance(s, t_prime, H) + 2    #deletion

    # Account for server substitutions
    if not splitted_s[-1] == splitted_t[-1]:
        s_0 = splitted_s[-1].split(":")[0]
        t_0 = splitted_t[-1].split(":")[0]
        if not s_0 == t_0:
            k_a += 1
        s_1 = splitted_s[-1].split(":")[1]
        t_1 = splitted_t[-1].split(":")[1]    
        if not s_1 == t_1:
            k_a += 1
    
    # Choose the minimum of the three costs
    c = min([k_a, k_b, k_c])

    # If the operation selected is a substitution, append 
    if c == k_a:
        S.append(broadcasted_distances.get((s_0, t_0), 1))
        S.append(broadcasted_distances.get((s_1, t_1), 1))

    c = c - len(S) + sum(S)

    H[(s,t)] = c

    return c, H

from setup_spark import session_spark
import random

spark = session_spark()
sc = spark.SparkContext()

# Create a dictionary with keys (server_1, server_2) and random values in [0, 1]
servers = [chr(i) for i in range(ord('A'), ord('Z') + 1)]
distances = {(s1, s2): random.uniform(0, 1) for s1 in servers for s2 in servers}

# Broadcast the dictionary
broadcasted_distances = sc.broadcast(distances)

process_1 = "user:A-A:C-C:B"
process_2 = "user:A-A:C-C:D"

d, H = process_string_edit_distance(process_1, process_2, H={}, broadcasted_distances=broadcasted_distances)

print(d)
print(H)