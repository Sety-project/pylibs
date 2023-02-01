#!/bin/python3
import sys
from utils.api_utils import extract_args_kwargs

from histfeed.history import main

if __name__ == "__main__":
    args, kwargs = extract_args_kwargs(sys.argv)
    main(*args,**kwargs)