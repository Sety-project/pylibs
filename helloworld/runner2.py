from os import environ

def greetMe():
    if environ.get("USERNAME", None):
        print("Hello, I can read my system name and it is : " + str(environ["USERNAME"]))
    else:
        print("Hello, I cannot read my system username :(")