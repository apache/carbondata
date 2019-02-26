import os

from petastorm.carbon import CarbonDatasetPiece, CarbonDataset
from pycarbon.CarbonReader import CarbonReader
import sys
import time
import multiprocessing


def main():
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/jdk1.8.0_181'
    print("Start")
    start = time.time()
    import jnius_config
    # Configure carbonData java SDK jar path
    jnius_config.add_options('-Xrs', '-Xmx3g')
    jnius_config.set_classpath(
        "/home/root1/Documents/ab/workspace/historm_xubo/historm/store/sdk/target/carbondata-sdk.jar")


    def readLogic(CarbonDatasetPiece):
        print("*** path" + CarbonDatasetPiece.path)
        data = CarbonDatasetPiece.read_all(("imageId","imageName","imageBinary","txtName","txtContent"))
        print("lennn: " + str(len(data)))
        return len(data)

    carbon_dataset = CarbonDataset("/home/root1/Documents/ab/workspace/historm_xubo/historm/store/sdk/target/voc/")
    pieces = carbon_dataset.pieces

    i = 0
    jobs = []
    for i in range(len(pieces)):
        piece = pieces[i]
        p = multiprocessing.Process(name='p_'+str(i), target=readLogic, args=(piece,))
        jobs.append(p)
        p.start()

    # sum = 0
    # for r in results:
    #     sum += r
    # print("total image count" + str(sum))

    for j in jobs:
        j.join()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")




if __name__ == '__main__':
    main()
