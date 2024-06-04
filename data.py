import random
from states import user, S1, S2, S3, S5, S7, str_to_class

def get_next_states(prev_state):
    num_possible_next_states = 2
    num_next_states = random.randint(0, num_possible_next_states)
    states = []
    for _ in range(num_next_states):
        index_state = random.randint(0, len(prev_state.next_state)-1)
        state_name = prev_state.next_state[index_state]
        state = str_to_class(state_name)
        states.append(state)
    return states

def iterate_state(prev_state, state, subtasks, time, sequence):
    states = get_next_states(prev_state)

    for state in states:
        sequence.append(state)
        subtasks.append([prev_state, state, time])
        aux_time = time+100

        states = get_next_states(prev_state)


        if len(state.next_state) != 0:
            print("No iterar")
        else: 
            subtasks, time, sequence = iterate_state(prev_state, state, subtasks, time, sequence)



      

    return subtasks, time, sequence

def create_processes(N=1):
    time = 100
    subtasks = []

    for i in range(N):
        prev_state = user
        sequence = [prev_state]

   

           


        time+=100

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