import os
from multiprocessing import Process, Manager
import time
from pycarbon.CarbonReader import CarbonReader

start = time.time()

process_num = 16

CARBON_DIR = "/Users/panfengfeng/Dataset/imageNet/carbon/dataset_small_new"

carbon_files = []
def addImages():
    for dir_path, _, files in os.walk(CARBON_DIR):
            for file in files:
                if "carbondata" in file:
                    carbon_files.append(os.path.join(dir_path, file))

addImages()
num_carbon_files = len(carbon_files)
print(num_carbon_files)
print(carbon_files)

result_queue = []
for i in range(process_num):
    result_queue.append(Manager().Queue(40))

def consumer(idx):
    print("consumer " + str(idx))
    index = 0
    num = 0
    while True:
        if index == num_carbon_files:
            break
        elif index % process_num != idx:
            index += 1
            continue
        reader = CarbonReader().builder().withFile(carbon_files[index]).build()
        result = []
        while (reader.hasNext()):
            row = reader.readNextRow()
            num += 1
            result.append(row)
        reader.close()
        index += 1
    print("consumer " + str(idx) + " num " + str(num))

if __name__ == "__main__":
    import jnius_config
    jnius_config.set_classpath('../../jars/carbondata-sdk.jar')

    print("START")

    readc = []
    for i in range(process_num):
        readc.append(Process(target=consumer, args=(i,)))

    for i in range(process_num):
        readc[i].start()

    for i in range(process_num):
        readc[i].join()

    end = time.time()
    print("all time " + str(end - start))
    print("END")

