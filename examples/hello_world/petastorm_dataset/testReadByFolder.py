# from pycarbon.CarbonReader import CarbonReader
from py4j.java_gateway import JavaGateway
import sys
import time

from petastorm.carbon import CarbonDatasetPiece


def main(argv):
    print("Start")
    start = time.time()
    print(argv)
    dataset = CarbonDatasetPiece("/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers/part-0-69194464707979_batchno0-0-null-69193536591188.carbondata")
    table = dataset.read_all(("imageId","imageName","imageBinary","txtName","txtContent"))
    print(len(table))

    end = time.time()

    print("all time: " + str(end - start))
    print("Finish")

if __name__ == '__main__':
    main(sys.argv)
