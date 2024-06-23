import random
from states import user, str_to_class
import json
import os

class Timer:
    def __init__ (self, time):
        self.curr_time = time

def get_next_states(prev_state):
    num_possible_next_states = min(len(prev_state.next_state), 2)
    num_next_states = random.randint(1, num_possible_next_states)
    #num_next_states = max(0, num_possible_next_states)
    states = []
    index_visited = []
    for _ in range(num_next_states):
        index_state = next(i for i in iter(lambda: random.randint(0, len(prev_state.next_state) - 1), None) if i not in index_visited)
        index_visited.append(index_state)
        state_name = prev_state.next_state[index_state]
        state = str_to_class(state_name)
        states.append(state)
    return states

def iterate_state(prev_state, subtasks, Timer, process_id):
    states = get_next_states(prev_state)
    for state in states:
      
        Timer.curr_time += state.time
        subtasks.append([prev_state, state, "Request", Timer.curr_time, process_id])
        if len(state.next_state) != 0:
            iterate_state(state, subtasks, Timer, process_id)
        Timer.curr_time  += state.time
        subtasks.append([state,prev_state, "Response", Timer.curr_time, process_id])
    

def create_data(namefile = "data_processes.json",distinct_process = 10, num_process=2):
    global_time = 0
    aux_processes, processes = [], []


    for i in range(distinct_process):
        processs_i = []
        prev_state = user

        process_id = f"process{i+1}"
        timer = Timer(global_time)
        iterate_state(prev_state, processs_i, timer, process_id)
        #print("proce    ", processs_i)
        aux_processes.append(processs_i)

        global_time+=random.randint(0,1000)


    real_time = 0

    for i in range(num_process):
        processs_i = []

        timer = Timer(real_time)

        process_i = random.choice(aux_processes)
        process_i = [[real_time + j * random.randint(100,110) if isinstance(process_i[j][i], (int, float)) else process_i[j][i] for i in range(len(process_i[j]))] for j in range(len(process_i))]


        print(process_i)
        real_time+=100
        processes.append( process_i)


            



    
    serializable_subtasks = []
    for p in processes:
        for subtask in p:
            state_from, state_to, action, time, process_id = subtask
            subtask_dict = {
                'state_from': state_from.__name__ if hasattr(state_from, '__name__') else state_from,
                'state_to': state_to.__name__ if hasattr(state_to, '__name__') else state_to,
                'action': action,'time': time,'process_id': process_id
            }
            serializable_subtasks.append(subtask_dict)


    path = os.path.join(os.path.dirname(os.getcwd()), "Data", namefile)
    with open(path, 'w') as f:
        json.dump(serializable_subtasks, f, indent="")




create_data()
