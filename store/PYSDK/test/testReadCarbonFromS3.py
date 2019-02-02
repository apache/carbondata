from pycarbon.CarbonReader import CarbonReader
from pycarbon.JavaGateWay import JavaGateWay
import sys
import time

def main(argv):
    print("Start")
    start = time.time()
    gateway = JavaGateWay()
    reader = CarbonReader(gateway.get_java_entry()) \
        .builder() \
        .withBatch(1000) \
        .withFolder(
        "s3a://modelartscarbon/voc/vocCarbon1000/voc1000/") \
        .withHadoopConf("fs.s3a.access.key", argv[1]) \
        .withHadoopConf("fs.s3a.secret.key", argv[2]) \
        .withHadoopConf("fs.s3a.endpoint", argv[3]) \
        .build()

    num=0
    while (reader.hasNext()):
        object = reader.readNextBatchRow()
        for rows in object:
            num = num + 1
            if(0==(num%1000)):
                print(num)
            for row in rows:
                row

    print(num)
    reader.close()
    end = time.time()
    print(end-start)
    print("Finish")


if __name__ == '__main__':
    main(sys.argv)
