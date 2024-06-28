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
    if num_next_states == 0:
        return None, None, False
    #num_next_states = max(0, num_possible_next_states)
    states, servers = [], []
    index_visited = []
    for _ in range(num_next_states):
        e = 0
        while True:
            index_state = random.randint(0, len(prev_state.next_state) - 1)
            state_name = prev_state.next_state[index_state]
            e+=1
            if e>control:
                break
            if index_state not in index_visited and state_name not in list_states :
                index_visited.append(index_state)
                state = str_to_class(str(state_name))
                states.append(state)
                servers.append(random.choice(tasks_servers[int(state.__name__)]) if isinstance(tasks_servers[int(state.__name__)], list) else tasks_servers[int(state.__name__)])
                #print(states,"_--", servers)
                break  
        
    return states, servers, True

def iterate_state(prev_state, subtasks, Timer, process_id, num_max_child, curr_depth, max_depth, list_states, tasks_servers, prev_server):
    states, servers, check = get_next_states(prev_state, num_max_child, list_states, tasks_servers)
    
    if check is True:
        
        #print(states)
        for state,server in zip(states,servers):
            #print(state, server)

            #print(state, "_", type(state))
            Timer.curr_time += state.time
            list_states.add(int(state.__name__))
            #print(prev_server, "---", server)
            prev_server, server = str(prev_server), "S"+str(server)
            subtasks.append([prev_server, server, "Request", Timer.curr_time, process_id])
            if len(state.next_state) != 0 and curr_depth< max_depth :
                iterate_state(state, subtasks, Timer, process_id, num_max_child, curr_depth+1, max_depth, list_states, tasks_servers, server)
            Timer.curr_time  += state.time
            subtasks.append([server, prev_server, "Response", Timer.curr_time, process_id])
        




            



    

def init_data( num_child_user = 5, num_states = 500, namefile = "data1.json",distinct_process = 1, num_process=1, num_max_child = 1, max_depth = 4,
               n_tasks= 10, n_servers=100):
    tasks_servers, tasks = create_tasks(n_tasks, n_servers)
    classes, prev_state = create_classes(tasks, n_tasks)
    
    global_time = 0
    #print(tasks)
    aux_processes, processes = [], []
    for i in range(distinct_process):
        processs_i = []

        process_id = f"process{i+1}"
        timer = Timer(global_time)
        list_states = set()
        list_states.add(prev_state.__name__)
        iterate_state(prev_state, processs_i, timer, process_id, num_max_child, 0, max_depth, list_states, tasks_servers, "user")
        #print("proce    ", processs_i)
        aux_processes.append(processs_i)
        #print("ii" *100, processs_i)

        global_time+=random.randint(0,1000)
    #print(aux_processes)

    real_time = 0

    for i in range(num_process):
        processs_i = []

        timer = Timer(real_time)
        process_id = f"process{i+1}"

        process_i = random.choice(aux_processes)
        process_i = [[real_time + j * random.randint(100,110) if isinstance(process_i[j][i], (int, float)) and j == 4 else process_i[j][i] for i in range(len(process_i[j]))] for j in range(len(process_i))]

        #process_i = [[process_id if isinstance(process_i[j][i], str) and  process_i[j][i].startswith("process") else process_i[j][i] for i in range(len(process_i[j]))] for j in range(len(process_i))]

        #print(process_i)
        real_time+=100
        processes.append( process_i)
    #print(processes)
    #print("ffff")
    #print(aux_processes)
    serializable_subtasks = []
    for p in processes:
        for subtask in p:
            state_from, state_to, action, time, process_id = subtask
            subtask_dict = {
                'state_from': state_from,
                'state_to': state_to,
                'action': action,'time': time,'process_id': process_id
            }
            serializable_subtasks.append(subtask_dict)

    #print("................................................", processes)
    path = os.path.join(os.getcwd(), "Data", namefile)
    

    with open(path, 'w') as f:
        json.dump(serializable_subtasks, f, indent=4)


init_data()

#create_data()
