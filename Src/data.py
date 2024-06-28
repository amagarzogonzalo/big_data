import random
from states import user, str_to_class, create_hierarchy_classes, create_tasks, create_classes
import json
import os


class Timer:
    def __init__ (self, time):
        self.curr_time = time

def get_next_states(prev_state, num_max_child, list_states, tasks_servers, control=10):
    num_possible_next_states = min(len(prev_state.next_state), num_max_child) # set max child per server call
    num_next_states = random.randint(1, num_possible_next_states)
    #num_next_states = max(0, num_possible_next_states)
    states, servers = [], []
    index_visited = []
    for _ in range(num_next_states):
        e = 0
        while True:
            index_state = random.randint(0, len(prev_state.next_state) - 1)
            state_name = prev_state.next_state[index_state]
            e+=1
            #print(list_states, "ej; ", state_name)
            print(list_states, "::", state_name)
            
            if index_state not in index_visited and state_name not in list_states and e < control:
                index_visited.append(index_state)
                state = str_to_class(str(state_name))
                states.append(state)
                list_states.append(states)
                servers.append(random.choice(tasks_servers[int(state.__name__)]) if isinstance(tasks_servers[int(state.__name__)], list) else tasks_servers[int(state.__name__)])
                break  
        
    return states, servers, True

def iterate_state(prev_state, subtasks, Timer, process_id, num_max_child, curr_depth, max_depth, list_states, tasks_servers, prev_server):
    states, servers, check = get_next_states(prev_state, num_max_child, list_states, tasks_servers)
    
    print(states)
    for state,server in zip(states,servers):
        print(state, server)

        #print(state, "_", type(state))
        Timer.curr_time += state.time
        
        subtasks.append([prev_server, server, "Request", Timer.curr_time, process_id])
        list_states.append(int(state.__name__))
        if len(state.next_state) != 0 and curr_depth<= max_depth :
            iterate_state(state, subtasks, Timer, process_id, num_max_child, curr_depth+1, max_depth, list_states, tasks_servers, server)
        Timer.curr_time  += state.time
        subtasks.append([server, prev_server, "Response", Timer.curr_time, process_id])
    



            



    

def init_data( num_child_user = 5, num_states = 500, namefile = "data_processes.json",distinct_process = 1, num_process=1000, num_max_child = 2, max_depth = 3,
               n_tasks= 10, n_servers=100):
    tasks_servers, tasks = create_tasks(n_tasks, n_servers)
    classes, prev_state = create_classes(tasks, n_tasks)
    
    global_time = 0
    print(tasks)
    aux_processes, processes = [], []
    for i in range(distinct_process):
        processs_i = []

        process_id = f"process{i+1}"
        timer = Timer(global_time)
        list_states = [prev_state]
        iterate_state(prev_state, processs_i, timer, process_id, num_max_child, 0, max_depth, list_states, tasks_servers, "user")
        #print("proce    ", processs_i)
        aux_processes.append(processs_i)

        global_time+=random.randint(0,1000)
    print(aux_processes)

init_data()

#create_data()
