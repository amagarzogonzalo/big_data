
import sys

def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)

class State:
    next_state = None

class user(State):
    next_state = ["S2", "S3"]

class S1(State):
    next_state = ["S2", "S5"]

class S2(State):
    next_state = ["S3", "S7"]

class S3(State):
    next_state = ["S5"]

class S5(State):
    next_state = []

class S7(State):
    next_state = []

class T():
    nex_state = []
