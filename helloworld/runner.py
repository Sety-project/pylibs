#!/bin/python3
from helloworld.runner2 import greetMe
import time

def main(*args):
    ''' This is the main runner to feed persist the data on a set of days for a given config
    @:param args should contain at least one date, one config number and one s3_region
    '''

    args = list(*args)
    arg_count = len(args)
    sleep_time = 30

    print("Hello from my docker image !")
    print(f"Sleeping for {sleep_time}s...")
    time.sleep(sleep_time)
    greetMe()