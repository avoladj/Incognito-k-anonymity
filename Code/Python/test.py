import os
import time
from datetime import datetime

def execute(k):
    k_str = str(k)
    for i in range(2, 10):
        i_str = str(i)
        start = datetime.timestamp(datetime.now())
        os.system("python3 main.py -d adult -D dimension_tables_" + i_str + "qi.json -k " + k_str)
        stop = datetime.timestamp(datetime.now())
        print()
        print("Execution time with " + i_str + " QIs and k= " + k_str + ": " + str(stop - start))
        print()
        print("Waiting for 3 seconds...\n")
        time.sleep(3)


if __name__ == "__main__":

    execute(2)
    execute(10)
