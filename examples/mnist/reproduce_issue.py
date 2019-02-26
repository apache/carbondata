import os

from pycarbon.CarbonReader import CarbonReader
import sys
import time
from multiprocessing.dummy import Pool as ThreadPool


def main(argv):
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/jdk1.8.0_181'
    print("Start")
    start = time.time()
    # print(argv)
    import jnius_config

    # Configure carbonData java SDK jar path
    jnius_config.set_classpath("/home/root1/Documents/ab/workspace/historm_xubo/historm/store/sdk/target/carbondata-sdk.jar")
    jnius_config.add_options('-Xrs', '-Xmx6g')

    path = "/home/root1/Documents/ab/workspace/historm_xubo/historm/store/sdk/target/voc/"

    carbon_splits = CarbonReader().builder().withFolder(path).getSplits()


    pool = ThreadPool(len(carbon_splits))

    def readLogic(split):

        carbonReader = CarbonReader() \
            .builder() \
            .withFile(split) \
            .withBatch(1000) \
            .build()

        i = 0
        while (carbonReader.hasNext()):
            rows = carbonReader.readNextBatchRow()
            for row in rows:
                i = i + 1
                # if 0 == i % 1000:
                #     print(i)
                for column in row:
                    column

        print(i)
        carbonReader.close()
        return i

    results = pool.map(readLogic, carbon_splits)
    sum = 0
    for r in results:
        sum += r
    print(sum)
    pool.close()




    # for eachReader in readers:
    #     i = 0
    #     while (eachReader.hasNext()):
    #         rows = eachReader.readNextBatchRow()
    #         for row in rows:
    #             i = i + 1
    #             if 0 == i % 1000:
    #                 print(i)
    #             for column in row:
    #                 column
    #
    #     print(i)
    #     eachReader.close()



    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")




if __name__ == '__main__':
    main(sys.argv)
