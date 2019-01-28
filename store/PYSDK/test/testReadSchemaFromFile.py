from pycarbon.CarbonReader import CarbonReader
from pycarbon.java_gateway import java_gateway
import sys


def main(argv):
    print("Start")
    print(argv)
    gateway = java_gateway()
    carbonSchemaReader = gateway.gateway.jvm.org.apache.carbondata.sdk.file.CarbonSchemaReader
    schema = carbonSchemaReader.readSchema("/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers/part-0-54142179118994_batchno0-0-null-54141188855741.carbondata")
    for each in schema.getFields():
        print(each)
        # print(each[0].getFieldName())
        # for columnName in :
        #     print(columnName)

    print("Finish")


if __name__ == '__main__':
    main(sys.argv)
