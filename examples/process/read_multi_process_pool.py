from multiprocessing import Pool
import os
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
    return num

if __name__=="__main__":
    import jnius_config

    jnius_config.set_classpath('../../jars/carbondata-sdk.jar')
    jnius_config.add_options('-Xrs', '-Xmx6096m')

    print("START")

    pool = Pool(processes=process_num)
    result = []
    for i in range(process_num):
        result.append(pool.apply_async(consumer, args=(i,)))

    pool.close()
    pool.join()

    num = 0
    for i in result:
        num += i.get()

    end = time.time()
    print("all time " + str(end - start))
    print("END " + str(num))
