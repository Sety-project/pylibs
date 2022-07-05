#!/bin/python3
import sys
from pfoptimizer.portoflio_optimizer import main


if __name__ == "__main__":
    args = [arg.split('=')[0] for arg in sys.argv if len(arg.split('=')) == 1]
    kwargs = dict([arg.split('=') for arg in sys.argv if len(arg.split('=')) == 2])
    main(*args,**kwargs)