import random
from states import user, S1, S2, S3, S5, S7, str_to_class

def get_next_states(prev_state):
    num_possible_next_states = min(len(prev_state.next_state), 2)
    #num_next_states = random.randint(0, num_possible_next_states)
    num_next_states = max(0, num_possible_next_states)
    print(prev_state, "Num next states", num_next_states)
    states = []
    index_visited = []
    for _ in range(num_next_states):
        index_state = next(i for i in iter(lambda: random.randint(0, len(prev_state.next_state) - 1), None) if i not in index_visited)
        index_visited.append(index_state)
        state_name = prev_state.next_state[index_state]
        state = str_to_class(state_name)
        states.append(state)
    return states

def iterate_state(prev_state, subtasks, time, process_id):
    states = get_next_states(prev_state)
    print("Num states: ", states)
    for state in states:
        print("State: ", state)
        subtasks.append([prev_state, state, time, process_id])
        time +=100
        if len(state.next_state) == 0:
            print("No iterar")
        else: 
            iterate_state(state, subtasks, time, process_id)
        time += 100
        subtasks.append([state,prev_state, time, process_id])
    

def create_processes(N=1):
    init_time = 100
    subtasks = []

    for i in range(N):
        subtasks_i = []
        prev_state = user

        process_id = f"process{i+1}"
        iterate_state(prev_state, subtasks, init_time, process_id)

        init_time+=100

    print(subtasks)




create_processes()



"""
  while len(state.next_state) != 0:
            
            prev_state = state
            state = get_next_states(prev_state)
            sequence.append(state)

            subtasks.append([prev_state, state, aux_time])
            aux_time += 100

"""