import random
from states import user, str_to_class, create_tasks, create_classes
import json
import copy
import os


class Timer:
    def __init__ (self, time):
        self.curr_time = time

def get_next_states(prev_state, num_max_child, list_states, tasks_servers, control=10):
    num_possible_next_states = min(len(prev_state.next_state), num_max_child) # set max child per server call
    num_next_states = random.randint(1, num_possible_next_states)
    if prev_state.__name__ == "user":
        num_next_states = 1
    if num_next_states == 0:
        return None, None, False
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
                break  
        
    return states, servers, True

def iterate_state(prev_state, subtasks, Timer, process_id, num_max_child, curr_depth, max_depth, list_states, tasks_servers, prev_server):
    states, servers, check = get_next_states(prev_state, num_max_child, list_states, tasks_servers)
    
    if check is True:
        
        for state,server in zip(states,servers):

            Timer.curr_time += state.time
            list_states.add(int(state.__name__))
            prev_server, server = str(prev_server), "S"+str(server)
            subtasks.append([prev_server, server, "Request", Timer.curr_time, process_id, prev_state, state])
            if len(state.next_state) != 0 and curr_depth< max_depth :
                iterate_state(state, subtasks, Timer, process_id, num_max_child, curr_depth+1, max_depth, list_states, tasks_servers, server)
            Timer.curr_time  += state.time
            subtasks.append([server, prev_server, "Response", Timer.curr_time, process_id, prev_state, state])
        




            



    

def init_data(namefile = "data1.json",distinct_process = 1, num_process=2, num_max_child = 2, max_depth = 2,
               n_tasks= 1000, n_servers=50000):
    tasks_servers, tasks = create_tasks(n_tasks, n_servers)
    classes, prev_state = create_classes(tasks, n_tasks)
    
    global_time = 0
    aux_processes = []
    for i in range(distinct_process):
        processs_i = []

        process_id = f"process{i+1}"
        timer = Timer(global_time)
        list_states = set()
        list_states.add(prev_state.__name__)
        iterate_state(prev_state, processs_i, timer, process_id, num_max_child, 0, max_depth, list_states, tasks_servers, "user")
        aux_processes.append(processs_i)

        global_time+=random.randint(0,1000)

    real_time = 0
    processesreal = [None] * num_process 
    timer = Timer(real_time)
    real_time = 0
    process_i = None
    for i in range(0, num_process):

        id = f"process{i+1}"

        #  + j * random.randint(100,110) 
        process_i = random.choice(aux_processes)
        for k in range(len(process_i)):
            for j in range(len(process_i[k])):
                if j == 3:
                    f = random.randint(5,11) 
                    process_i[k][j] = real_time + f
                    real_time += f
                if j == 4:
                    #print(k, "pro: ", process_i[k][j], " por ", id)
                    process_i[k][j] = id
       
        #print(id)
        real_time+=10
        p = copy.deepcopy(process_i)
        processesreal[i] = p
    
    #print(processesreal)
    serializable_subtasks = []
    for p in range(len(processesreal)):
        dictt = {}
        for subtask in processesreal[p]:
            #print(subtask)
            state_from, state_to, action, time, process_id, taskin, taskout = subtask
            if state_from not in dictt:
                if state_from == "user":
                    dictt[state_from] = "user"

                if isinstance(state_from,str) and state_from != "user":

                    dictt[state_from] = random.choice(tasks_servers[int(taskin.__name__)]) if isinstance(tasks_servers[int(taskin.__name__)], list) else tasks_servers[int(taskin.__name__)]
            if state_from == "user":
                serverfrom =str(dictt[state_from])
            if isinstance(state_from,str) and state_from != "user":
                serverfrom ="S"+str(dictt[state_from])


            if state_to not in dictt:
                if state_to == "user":
                    dictt[state_to] = "user"
                if isinstance(state_to,str) and state_to != "user":
                    dictt[state_to] = random.choice(tasks_servers[int(taskout.__name__)]) if isinstance(tasks_servers[int(taskout.__name__)], list) else tasks_servers[int(taskout.__name__)]
            if state_to == "user":
                serverto= str(dictt[state_to])
            if isinstance(state_to,str) and state_to != "user":
                serverto = "S"+str(dictt[state_to])

            subtask_dict = {
                'state_from ': serverfrom,
                'state_to ': serverto,
                'action': action,'time': time,'process_id': process_id
            }
            serializable_subtasks.append(subtask_dict)
        #print(dictt)

    path = os.path.join(os.getcwd(), "Data", namefile)
    
    #os.remove(path)

    with open(path, 'w') as f:
        json.dump(serializable_subtasks, f, indent=4)
    print(f"File {namefile} created.")


def create_data():
    names = ["complexity.json", "variety.json", "homogeneity.json", "simple.json"]
    distinct_process = [100, 100, 100, 2]
    num_process = [1000, 1000, 1000, 4]
    num_max_child = [2, 2, 2, 2]
    max_depth = [3, 3, 3, 2]
    n_tasks=  [100, 100, 100, 10] 
    n_servers= [1000, 1000, 1000, 1000]

    for name, distinct, nump, numchild, depth, ntask, nserver in zip(names, distinct_process, num_process, num_max_child, max_depth, n_tasks, n_servers):
        init_data(namefile=name, distinct_process=distinct, num_process=nump,
                   num_max_child=numchild, max_depth=depth, n_tasks=ntask, n_servers=nserver)

    

create_data()

