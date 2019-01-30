from pycarbon.CarbonReader import CarbonReader
from pycarbon.javagateway import JavaGateWay
import sys


def main(argv):
    print("Start")
    print(argv)
    print(argv[1])
    print(argv[2])
    print(argv[3])
    gateway = JavaGateWay()
    reader = CarbonReader(gateway.get_java_entry()) \
        .builder() \
        .withBatch(200) \
        .withFile(
        "s3a://modelartscarbon/test/flowersCarbon/part-0-173838118202852_batchno0-0-null-173837348004479.carbondata") \
        .withHadoopConf("fs.s3a.access.key", argv[1]) \
        .withHadoopConf("fs.s3a.secret.key", argv[2]) \
        .withHadoopConf("fs.s3a.endpoint", argv[3]) \
        .build()

    while (reader.hasNext()):
        object = reader.readNextRow();
        print
        for row in object:
            print(row)
    reader.close()
    print("Finish")


if __name__ == '__main__':
    main(sys.argv)
