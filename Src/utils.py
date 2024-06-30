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

""" def process_string_edit_distance(s, t, H):
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

    # Choose the minimum of the three costs
    c = min([k_d, k_b, k_c])

    #c = c - len(S) + sum([substitution[1] for substitution in S])
    H[(s, t)] = c
    return c """

import numpy as np
def process_string_edit_distance(s, t):
    assert type(s)==str and type(t)==str
    
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
    

    D = np.zeros(shape=(len(splitted_s)+1, len(splitted_t)+1))
    for i in range(0, len(splitted_s)+1):
        D[i,0] = i
    for j in range(0, len(splitted_t)+1):
        D[0,j] = j

    for i in range(1, len(splitted_s)):
        for j in range(1, len(splitted_t)):
            if splitted_s[i] == splitted_t[j]:
                sub_cost = 0
            else:
                sub_cost = 1
            
            D[i][j] = min(
                D[i - 1][j] + 1,   # Deletion
                D[i][j - 1] + 1,   # Insertion
                D[i-1][j - 1] + sub_cost  # Substitution
            )
    return D[len(splitted_s), len(splitted_t)]


