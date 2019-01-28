from py4j.java_gateway import JavaGateway

print("Start")
gateway = JavaGateway()  # connect to the JVM
random = gateway.jvm.java.util.Random()  # create a java.util.Random instance

addition_app = gateway.entry_point  # get the AdditionApplication instance
# reader =addition_app.read("/Users/xubo/Desktop/xubo/git/carbondata1/testWriteFiles")
reader =addition_app.read("/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers")
while (reader.hasNext()):
    object = reader.readNextRow();
    print
    for row in object:
        row
reader.close()
print("Finish")
