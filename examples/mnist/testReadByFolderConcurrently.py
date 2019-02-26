import os

from petastorm.carbon import CarbonDatasetPiece, CarbonDataset
from pycarbon.CarbonReader import CarbonReader
import sys
import time
from multiprocessing.dummy import Pool as ThreadPool


def main():
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/jdk1.8.0_181'
    print("Start")
    start = time.time()
    import jnius_config

    # Configure carbonData java SDK jar path
    jnius_config.add_options('-Xrs', '-Xmx3g')
    jnius_config.set_classpath(
        "/home/root1/Documents/ab/workspace/historm_xubo/historm/store/sdk/target/carbondata-sdk.jar")

    pool = ThreadPool(1)
    carbon_dataset = CarbonDataset("/home/root1/Documents/ab/workspace/historm_xubo/historm/store/sdk/target/voc/")
    pieces = carbon_dataset.pieces

    def readLogic(CarbonDatasetPiece):
        data = CarbonDatasetPiece.read_all(("imageId","imageName","imageBinary","txtName","txtContent"))
        print("lennn: " + str(len(data)))
        return len(data)
    results = pool.map(readLogic, pieces)
    sum = 0
    for r in results:
        sum += r
    print("total image count" + str(sum))
    pool.close()


    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")




if __name__ == '__main__':
    main()
