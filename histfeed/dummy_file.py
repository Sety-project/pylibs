from utils.io_utils import api


@api
def rien():
    print('__name__')
    return '__name__'

def wrapit():
    return rien()
if __name__ == "__main__":
    wrapit()