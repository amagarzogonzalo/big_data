from setup_spark import session_spark
spark = session_spark()
sc = spark.sparkContext

import json
def retrieve_data(file_name: str):
    with open("Data/"+file_name) as json_file:
        return json.load(json_file)

DATASET_NAME = "data_processes.json"
rdd = sc.parallelize(retrieve_data(DATASET_NAME))

def get_server_names(rdd, without_user: bool = True):
    server_names = rdd.map(lambda x: x['state_from']).distinct().collect()
    if without_user:
        server_names.remove("user")
    return server_names

print(get_server_names(rdd))

def request_path(process):
    for log in process:
        if log['action'] == 'Request':
            if log['state_from'] == 'user':
                path = ":".join([log['state_from'], log['state_to']])
            else:
                path = "-".join([path, ":".join([log['state_from'], log['state_to']])])
    return path

def get_processes_request_paths(rdd):
    return rdd.sortBy(lambda x: x['time']).groupBy(lambda x: x['process_id'])\
            .mapValues(request_path).collect()

#print(get_processes_request_paths(rdd))

def servers_depth(request_path: str):
    requests = [request.split(":") for request in request_path.split("-")]
    assert requests[0][0] == 'user' #This is true if ordered in time
    servers_depth = {'user': 0}
    for request in requests:
        assert request[0] in servers_depth.keys()
        if request[1] not in servers_depth.keys():
            servers_depth[request[1]] = servers_depth[request[0]] + 1
    return servers_depth

def get_processes_servers_depth(rdd):
    return rdd.sortBy(lambda x: x['time']).groupBy(lambda x: x['process_id']) \
            .mapValues(request_path).mapValues(servers_depth).collect()

def add_depth_to_logs(process, depths):
    process_id = process[0]['process_id']
    depth_info = depths.get(process_id, {})
    for log in process:
        log['depth_from'] = depth_info.get(log['state_from'], -1)
    return process

def get_logs_with_depth(rdd):
    depths = dict(get_processes_servers_depth(rdd))
    return rdd.groupBy(lambda x: x['process_id']) \
            .mapValues(list) \
            .mapValues(lambda logs: add_depth_to_logs(logs, depths)) \
            .flatMap(lambda x: x[1])
    
def get_server_connections(rdd):
    reqs_with_depth = get_logs_with_depth(rdd).filter(lambda x: x['action']=="Request")
    server_connections = {}
    for server_name in get_server_names(rdd):
        server_connections[server_name] =  set(reqs_with_depth.filter(lambda x: x['state_to'] == server_name) \
                                            .map(lambda x: (x['state_from'], 'X', x['depth_from'])) \
                                            .distinct().collect()).union(
                                        set(reqs_with_depth.filter(lambda x: x['state_from'] == server_name) \
                                        .map(lambda x: ('X', x['state_to'], x['depth_from'])) \
                                        .distinct().collect())
                                            )   
    return server_connections

def jaccard_similarity(a, b):
    assert type(a)==set and type(b)==set
    return len(a.intersection(b)) / len(a.union(b))

def jaccard_distance(a, b):
    return 1-jaccard_similarity(a, b)





""" conn = get_server_connections(rdd)
print(conn)
print(jaccard_distance(conn['S5'], conn['S8'])) """





def print_common_transitions(rdd):

    # Filtrar los datos para quedarnos solo con las acciones "Request"
    requests_rdd = rdd.filter(lambda x: x['action'] == "Request")

    # Transformar los datos en la estructura deseada (process_id, [state_from, state_to])
    mapped_rdd = requests_rdd.map(lambda x: (x['process_id'], [x['state_from'], x['state_to']]))

    # Agrupar por process_id y combinar las listas de transiciones
    grouped_rdd = mapped_rdd.groupByKey().mapValues(lambda x: [item for sublist in x for item in sublist])

    # Función para convertir lista de transiciones a un contador de transiciones
    def transitions_counter(transitions):
        from collections import Counter
        return Counter(transitions)

    # Contar las transiciones para cada proceso
    transition_counts_rdd = grouped_rdd.mapValues(transitions_counter)

    # Definir una función de reducción para agrupar procesos similares si tienen al menos tres transiciones iguales
    def reduce_similar_processes(a, b):
        # Comparar los dos contadores y verificar si tienen al menos tres transiciones iguales
        common_transitions = a & b  # Intersección de los contadores
        if sum(common_transitions.values()) >= 3:
            return a + b  # Unir los contadores si tienen al menos tres transiciones iguales
        return a

    # Aplicar reduceByKey para agrupar procesos con transiciones similares
    similar_processes_rdd = transition_counts_rdd.map(lambda x: (frozenset(x[1].items()), [x[0]])) \
                                                .reduceByKey(lambda a, b: a + b) \
                                                .filter(lambda x: len(x[1]) > 1)  # Filtrar para obtener solo aquellos con al menos dos procesos

    # Recoger el resultado
    result = similar_processes_rdd.collect()

    # Imprimir los procesos similares
    for common_transitions, process_ids in result:
        print("Common transitions:")
        for transition, count in common_transitions:
            print(f"  {transition}: {count}")
        print("Process IDs:")
        for process_id in process_ids:
            print(f"  {process_id}")