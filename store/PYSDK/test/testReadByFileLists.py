from pycarbon.CarbonReader import CarbonReader
from pycarbon.JavaGateWay import JavaGateWay

print("Start")
java_gate_way = JavaGateWay()

java_list = java_gate_way.gateway.jvm.java.util.ArrayList()
java_list.append("/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers/part-0-64449395812322_batchno0-0-null-64448026510510.carbondata")
java_list.append("/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers/part-0-64455426141135_batchno0-0-null-64454873535049.carbondata")

projection_list = java_gate_way.gateway.jvm.java.util.ArrayList()
projection_list.append("imageName")
projection_list.append("imageBinary")
projection_list.append("txtName")
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
            print(row)

reader.close()
print("Finish")
