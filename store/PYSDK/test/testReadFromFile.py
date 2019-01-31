from pycarbon.CarbonReader import CarbonReader
from pycarbon.JavaGateWay import JavaGateWay
import sys


def main(argv):
    print("Start")
    print(argv)
    gateway = JavaGateWay()
    reader = CarbonReader(gateway.get_java_entry()) \
        .builder() \
        .withFile(
        "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers/part-0-72301447989333_batchno0-0-null-72300450978611.carbondata") \
        .withBatch(200) \
        .build()

    while (reader.hasNext()):
        object = reader.readNextBatchRow()
        print
        print
        i = 0
        for rows in object:
            print("rows")
            i = i + 1
            print(i)
            j = 0;
            for row in rows:
                j = j + 1
                print("column:" + str(j))
                row

    reader.close()
    print("Finish")


if __name__ == '__main__':
    main(sys.argv)
