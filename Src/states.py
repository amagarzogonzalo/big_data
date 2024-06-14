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
