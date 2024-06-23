import sys
import random
import inspect


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


def create_Sx_class(x, next_states=[]):
    class_name = f"S{x}"
    
    attrs = {
        'next_states': next_states
    }
    
    new_class = type(class_name, (State,), attrs)
    
    globals()[class_name] = new_class
    
    return new_class


def create_hierarchy_classes(num_classes, num_child_user):
    classes = {}
    class_name = "user"
    next_states = []


    for j in range(1, num_child_user):
        next_states.append(f"S{j}")

    attrs = {
        'next_state': next_states
    }
    #print("childs of user ", attrs)
    init_class = type(class_name, (State,), attrs)
    classes[class_name] = init_class
    setattr(sys.modules[__name__], class_name, init_class) 

    for i in range(1, num_classes + 1):
        class_name = f"S{i}"
        next_states = []

        for j in range(i + 1, num_classes + 1):
            if random.randint(0, 10) > 5 and j > num_child_user:
                next_states.append(f"S{j}")

        attrs = {
            'next_state': next_states
        }
        #print(attrs)
        new_class = type(class_name, (State,), attrs)
        #classes[class_name] = new_class
        setattr(sys.modules[__name__], class_name, new_class) 

                # Obtener todos los miembros del módulo actual (__name__)
    """members = inspect.getmembers(sys.modules['states'])

    # Filtrar solo las clases
    classes = [member for member in members if inspect.isclass(member[1])]

    # Imprimir los nombres de las clases encontradas
    for name, _ in classes:
        print(name)"""

    
    return classes, init_class