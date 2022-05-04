#!/bin/python3
from pytests.helloworld.runner2 import *

def main(*args):
    ''' This is the main runner to feed persist the data on a set of days for a given config
    @:param args should contain at least one date, one config number and one s3_region
    '''

    args = list(*args)

    arg_count = len(args)
    print("Hello Vic  from my docker image !")
    greetMe()