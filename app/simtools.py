import os.path
import sys
from utility.core_app import automate_app

if __name__ == "__main__":
    input_file = sys.argv[1]
    check_file = os.path.exists(input_file)
    if not check_file:
        print("Cannot find file {}, please check again".format(input_file))
        exit()
    else:
        automate_app(input_file)

