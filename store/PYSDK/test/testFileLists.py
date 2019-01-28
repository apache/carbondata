from pycarbon.CarbonReader import CarbonReader
from pycarbon.java_gateway import java_gateway

print("Start")
java_gate_way = java_gateway()

java_list = java_gate_way.gateway.jvm.java.util.ArrayList()
java_list.append(
    "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers/part-0-54142179118994_batchno0-0-null-54141188855741.carbondata")
java_list.append(
    "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers/part-0-54142492312606_batchno0-0-null-54142455443385.carbondata")

projection_list = java_gate_way.gateway.jvm.java.util.ArrayList()
# projection_list.append("imageName")
# projection_list.append("imageBinary")
# projection_list.append("txtName")
projection_list.append("txtcontent")

reader = CarbonReader(java_gate_way.get_java_entry())\
    .builder() \
    .withFileLists(java_list) \
    .withBatch(100) \
    .projection(projection_list) \
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
