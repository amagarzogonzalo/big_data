import random
from states import user, str_to_class
import json

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
    

def create_data(N=100000):
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
            'action': action,
            'time': time,
            'process_id': process_id
        }
        serializable_subtasks.append(subtask_dict)

    with open("data_processes.json", 'w') as f:
        json.dump(serializable_subtasks, f, indent=4)




create_data()
