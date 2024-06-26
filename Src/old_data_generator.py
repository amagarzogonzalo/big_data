import json
import sys
import random

def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)

class State:
    next_state = None
    time = 100 #random.randint(50,100)

class user(State):
    next_state = ["S1"]

class S1(State):
    next_state = ["S2", "S3"]

class S2(State):
    next_state = ["S7"]

class S3(State):
    next_state = ["S5", "S6", "S8"]

class S5(State):
    next_state = []

class S6(State):
    next_state = ["S7"]

class S7(State):
    next_state = []

class S8(State):
    next_state = []

class T():
    nex_state = []


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
    

def create_data(N=1000):
    global_time = 0
    subtasks = []

    for i in range(N):
        subtasks_i = []
        prev_state = user

        process_id = f"process{i+1}"
        timer = Timer(global_time)
        iterate_state(prev_state, subtasks_i, timer, process_id)
        subtasks += subtasks_i

        global_time+=random.randint(0,1000)

    serializable_subtasks = []
    for subtask in subtasks:
        state_from, state_to, action, time, process_id = subtask
        subtask_dict = {
            'state_from': state_from.__name__ if hasattr(state_from, '__name__') else state_from,
            'state_to': state_to.__name__ if hasattr(state_to, '__name__') else state_to,
            'action': action,'time': time,'process_id': process_id
        }
        serializable_subtasks.append(subtask_dict)

    with open("Data/data_processes_v2.json", 'w') as f:  
        for i, d in enumerate(serializable_subtasks):
            # Serialize dictionary to JSON string
            json_str = json.dumps(d)
            # Write the JSON string to file
            f.write(json_str)
            # Add a comma if it's not the last dictionary
            if i < len(serializable_subtasks) - 1:
                f.write(',\n')


create_data()