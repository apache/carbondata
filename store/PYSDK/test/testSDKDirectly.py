from py4j.java_gateway import JavaGateway

print("Start")
gateway = JavaGateway()  # connect to the JVM
list = gateway.jvm.java.util.LinkedList

addition_app = gateway.entry_point  # get the AdditionApplication instance
# reader = addition_app.read("/Users/xubo/Desktop/xubo/git/carbondata1/testWriteFiles")

reader = addition_app.builder() \
    .withFile(
    "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers/part-0-7548049181287_batchno0-0-null-7548045761060.carbondata") \
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
