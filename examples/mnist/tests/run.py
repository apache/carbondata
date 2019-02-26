import jnius_config

from mnist import tf_example_carbon_just_read

jnius_config.set_classpath(
        "/home/root1/Documents/ab/workspace/historm_xubo/historm/store/sdk/target/carbondata-sdk.jar")
jnius_config.add_options('-Xrs', '-Xmx6g')


for i in range(100):
    print("\n\n\n number of " + str(i))
    tf_example_carbon_just_read.main()