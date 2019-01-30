from pycarbon.CarbonSchemaReader import CarbonSchemaReader
from pycarbon.Configuration import Configuration
from pycarbon.javagateway import JavaGateWay
import sys


def main(argv):
    print("Start")
    print("\n\ntest1:")
    javaGateWay = JavaGateWay()
    carbonSchemaReader = CarbonSchemaReader(javaGateWay.gateway)
    schema = carbonSchemaReader.readSchema(
        "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers/part-0-64449395812322_batchno0-0-null-64448026510510.carbondata")
    print(schema.getFieldsLength())
    for each in schema.getFields():
        print(each)
        print(each.getFieldName())
        print(each.getDataType())
        print(each.getSchemaOrdinal())

    print("\n\ntest2:")
    schema = carbonSchemaReader.readSchema(
        "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers/")
    print(schema.getFieldsLength())
    for each in schema.getFields():
        print(each)
        print(each.getFieldName())
        print(each.getDataType())
        print(each.getSchemaOrdinal())

    print("\n\ntest3:")
    schema = carbonSchemaReader.readSchema(
        "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers/", True)
    print(schema.getFieldsLength())
    for each in schema.getFields():
        print(each)
    print(each.getFieldName())
    print(each.getDataType())
    print(each.getSchemaOrdinal())

    print("\n\ntest4:")
    configuration = Configuration(javaGateWay.gateway)
    configuration.set("fs.s3a.access.key", argv[1])
    configuration.set("fs.s3a.secret.key", argv[2])
    configuration.set("fs.s3a.endpoint", argv[3])

    schema = carbonSchemaReader.readSchema(
        "s3a://modelartscarbon/test/flowersCarbon/", True, configuration.conf)
    print(schema.getFieldsLength())
    for each in schema.getFields():
        print(each)
        print(each.getFieldName())
        print(each.getDataType())
        print(each.getSchemaOrdinal())
    print("Finish")


if __name__ == '__main__':
    main(sys.argv)
