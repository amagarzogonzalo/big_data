import random
from states import user, str_to_class, create_hierarchy_classes
import json
import os


class Timer:
    def __init__ (self, time):
        self.curr_time = time

def get_next_states(prev_state, num_max_child):
    num_possible_next_states = min(len(prev_state.next_state), num_max_child) # set max child per server call
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

def iterate_state(prev_state, subtasks, Timer, process_id, num_max_child, curr_depth, max_depth):
    states = get_next_states(prev_state, num_max_child)
    for state in states:
      
        Timer.curr_time += state.time
        subtasks.append([prev_state, state, "Request", Timer.curr_time, process_id])
        if len(state.next_state) != 0 and curr_depth<= max_depth :
            iterate_state(state, subtasks, Timer, process_id, num_max_child, curr_depth+1, max_depth)
        Timer.curr_time  += state.time
        subtasks.append([state,prev_state, "Response", Timer.curr_time, process_id])
    

def create_data(num_child_user = 5, num_states = 500, namefile = "data_processes.json",distinct_process = 100, num_process=1000, num_max_child = 2, max_depth = 3):
    global_time = 0
    aux_processes, processes = [], []
    states, init_state = create_hierarchy_classes(num_states, num_child_user)
    #print(init_state)
    for i in range(distinct_process):
        processs_i = []
        prev_state = init_state

        process_id = f"process{i+1}"
        timer = Timer(global_time)
        iterate_state(prev_state, processs_i, timer, process_id, num_max_child, 0, max_depth)
        #print("proce    ", processs_i)
        aux_processes.append(processs_i)

        global_time+=random.randint(0,1000)


    real_time = 0

    for i in range(num_process):
        processs_i = []

        timer = Timer(real_time)
        process_id = f"process{i+1}"

        process_i = random.choice(aux_processes)
        process_i = [[real_time + j * random.randint(100,110) if isinstance(process_i[j][i], (int, float)) else process_i[j][i] for i in range(len(process_i[j]))] for j in range(len(process_i))]

        process_i = [[process_id if isinstance(process_i[j][i], str) and  process_i[j][i].startswith("process") else process_i[j][i] for i in range(len(process_i[j]))] for j in range(len(process_i))]

        #print(process_i)
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

    path = os.path.join(os.getcwd(), "Data", namefile)
    with open(path, 'w') as f:
        for i, d in enumerate(serializable_subtasks):
            # Serialize dictionary to JSON string
            json_str = json.dumps(d)
            # Write the JSON string to file
            f.write(json_str)
            # Add a comma if it's not the last dictionary
            if i < len(serializable_subtasks) - 1:
                f.write(',\n')

"""   for task in serializable_subtasks:
    json.dump(task, f, indent="") """

create_data()
