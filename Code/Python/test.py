import os
import matplotlib.pyplot as plt
from datetime import datetime

def execute(k, execution_times):
    k_str = str(k)
    for i in QIs:
        i_str = str(i)
        print("Starting with " + i_str + " QIs and k= " + k_str + " ...")
        start = datetime.timestamp(datetime.now())
        os.system("python3 main.py -d ../datasets/test_adults.csv -D dimension_tables_" + i_str + "qi.json -k " + k_str)
        stop = datetime.timestamp(datetime.now())
        execution_time = stop - start
        print()
        print("Execution time with " + i_str + " QIs and k= " + k_str + ": " + str(execution_time))
        print()
        execution_times.append(execution_time)
        """
        yes = input("Continue? y/n\n")
        if not (yes == "y" or yes == "Y"):
            break
        """


def plot_with_k(k):
    execution_times = list()
    execute(k, execution_times)
    if len(execution_times) == len(QIs):
        plt.plot(QIs, execution_times)
        plt.xlabel("Number of qi")
        plt.ylabel("Time [s]")
        plt.title("Execution Time with k=" + str(k))
        plt.grid()
        plt.show()


if __name__ == "__main__":

    QIs = range(2, 6)
    plot_with_k(2)
    #plot_with_k(10)
