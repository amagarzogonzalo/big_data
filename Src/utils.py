from pyspark.sql.functions import col

def lower_triangle_matrix_index(i, j):
    # Ensure that we are only accesing elements in the lower triangle matrix.
    assert j>=0 and i>j
    return int((i-1)*(i-2)*(1/2) + j)

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

def process_string_edit_distance(s, t, H):
    assert type(s)==str and type(t)==str

    # If the pair is already in the associative array, returns its distance.
    if (s, t) in H.keys():
        return H[(s,t)]
    
    # We work with edges as base elements
    splitted_s = s.split("-")
    splitted_t = t.split("-")

    # Trivial cases
    if len(s)==0:
        if len(splitted_t) == 1 and not splitted_t[0]:
            return 0
        return len(splitted_t)
    if len(t)==0:
        if len(splitted_s) == 1 and not splitted_s[0]:
            return 0
        return len(splitted_s)
    
    # Recursivity with one edge less
    s_prime = "-".join(splitted_s[:-1])
    t_prime = "-".join(splitted_t[:-1]) 
    k_a = process_string_edit_distance(s_prime, t_prime, H)  #substitution
    k_b = process_string_edit_distance(s_prime, t, H) + 1    #insertion
    k_c = process_string_edit_distance(s, t_prime, H) + 1    #deletion

    # Account for server substitutions
    if splitted_s[-1] == splitted_t[-1]:
        k_d = k_a
    else:
        k_d = k_a + 1

        """ s_0 = splitted_s[-1].split(":")[0]
        t_0 = splitted_t[-1].split(":")[0]
        if not s_0 == t_0:
            distance_0 = broadcasted_distances.value.get((s_0, t_0), 1)
            k_d += 1
        t_1 = splitted_s[-1].split(":")[1]
        t_1 = splitted_t[-1].split(":")[1]    
        if not t_1 == t_1:
            distance_1 = broadcasted_distances.value.get((t_1, t_1), 1)
            k_d += 1 """
    
    # Choose the minimum of the three costs
    c = min([k_d, k_b, k_c])

    # If the operation selected is a substitution, append 
    """ if c == k_d and not splitted_s[-1] == splitted_t[-1]:
        if k_d - k_a < 2:
            if not s_0 == t_0:
                S.append(((s_0, t_0), distance_0))
            if not t_1 == t_1:
                S.append(((t_1, t_1), distance_1)) """

    #c = c - len(S) + sum([substitution[1] for substitution in S])
    H[(s, t)] = c
    return c

if __name__ == "__main__":
    from setup_spark import session_spark
    import random

    spark = session_spark()
    sc = spark.sparkContext

    # Create a dictionary with keys (server_1, server_2) and random values in [0, 1]
    servers = [chr(i) for i in range(ord('A'), ord('Z') + 1)]
    distances = {(s1, s2): random.uniform(0, 1) for s1 in servers for s2 in servers}

    # Broadcast the dictionary
    broadcasted_distances = sc.broadcast(distances)

    process_1 = "1A-1C-0C-1B-1E-1F-0F-0E-0B-0A"
    process_2 = "1A-1B-1E-1F-0F-0E-1T-0T-0B-1C-1L-0L-0C-0A"

    memoization_dict = {}
    substitution_list = []

    d = process_string_edit_distance(process_1, process_2, 
                                    H=memoization_dict)

    print(d)
