import sys
from ftx_ws_execute import log_reader

if __name__ == "__main__":
    log_reader(*sys.argv[1:])