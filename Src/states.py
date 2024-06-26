import sys
import random
import inspect
import string 


def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)


class State:
    next_state = None
    time = 100 #random.randint(50,100)

class user(State):
    next_state = ["S1"]




def create_Sx_class(x, next_states=[]):
    class_name = f"S{x}"
    
    attrs = {
        'next_states': next_states
    }
    
    new_class = type(class_name, (State,), attrs)
    
    globals()[class_name] = new_class
    
    return new_class


def create_tasks(n_task, n_servers, max_servers_per_task=500):
    unique_tasks = set()  
    tasks = []
    task_server = {"user": "user"}
    server_count = 1
    while len(tasks) < n_task:
        t = random.randint(0, n_task * 10000)  
        if t not in unique_tasks:
            unique_tasks.add(t)
            tasks.append(t)
            n_servers = random.randint(max_servers_per_task/10, max_servers_per_task)
            servers_for_t = [server_count + i for i in range(n_servers)]
            server_count+=n_servers

            task_server[t] = servers_for_t


    return task_server, tasks



def create_classes(tasks, n_task):

    classes  =[]


    attrs = {'next_state': tasks}
    user = type("user", (State,), attrs)
    setattr(sys.modules[__name__], "user", user)
    classes.append(user) # we first add user
    for t in tasks:
        class_name = str(t)
        attrs = {'next_state': tasks}
        new_class = type(class_name, (State,), attrs)
        setattr(sys.modules[__name__], class_name, new_class)
        classes.append(new_class)
    return classes, user

