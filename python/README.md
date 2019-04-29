# PyCarbon

PyCarbon provides python API for integrating CarbonData with AI framework like  TensorFlow, PyTorch, MXNet. By using PyCarbon, AI framework can read training data faster by leveraging CarbonData's indexing and caching ability. Since CarbonData is a columnar storage, AI developer can also perform projection and filtering to pick required data for training efficiently.

## PyCarbon install

$ git clone https://github.com/apache/carbondata.git

$ cd python/pycarbon

$ pip install . --user


## how to use

if you have a CarbonData dataset, you can use PyCarbon to read data. For the generation of CarbonData dataset, you can see the examples:
`generate_dataset_carbon.py` in test/hello_world and `generate_pycarbon_dataset.py` in test/hello_world.
But user should do some config first:

 - config pyspark and add carbon assembly jar to pyspark/jars folder, which can be compiled from CarbonData project.
 - default Java sdk jar is in carbondata/store/sdk/target,  user also can specific the jar location like by --carbon-sdk-path in generate_pycarbon_dataset.py.

 - set JAVA_HOME, PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON in you system environment.

#### PySpark and SQL

#### Generating the data:
```

    python generate_pycarbon_mnist.py --carbon-sdk-path  /your_path/carbondata/store/sdk/target/carbondata-sdk.jar 

```
##### Part of code:
```
    # Create a dataframe object from carbon files
    spark.sql("create table readcarbon using carbon location '" + str(dataset_path) + "'")
    dataframe = spark.sql("select * from readcarbon")

    # Show a schema
    dataframe.printSchema()

    # Count all
    dataframe.count()

    # Show just some columns
    dataframe.select('id').show()

    # Also use a standard SQL to query a dataset
    spark.sql('SELECT count(id) from carbon.`{}` '.format(dataset_url)).collect()
```
some details are illustrated in `pyspark_hello_world_carbon.py` in test/hello_world.

#### TensorFlow Dataset API


##### Running train and test based on mnist:

```
    python  tf_example_carbon_unified_api.py --carbon-sdk-path  /your_path/carbondata/store/sdk/target/carbondata-sdk.jar 

```
##### Part or code:
```
    with make_reader('file:///some/localpath/a_dataset') as reader:
        dataset = make_dataset(reader)
        iterator = dataset.make_one_shot_iterator()
        tensor = iterator.get_next()
        with tf.Session() as sess:
            sample = sess.run(tensor)
            print(sample.id)

some details are illustrated in `tf_example_carbon_unified_api.py` in test/mnist. 
```

#####  Part of result:

```
2020-01-20 21:12:31 INFO  DictionaryBasedVectorResultCollector:72 - Direct pagewise vector fill collector is used to scan and collect the data
2020-01-20 21:12:32 INFO  UnsafeMemoryManager:176 - Total offheap working memory used after task 2642c969-6c43-4e31-b8b0-450dff1f7821 is 0. Current running tasks are 
2020-01-20 21:12:32 INFO  UnsafeMemoryManager:176 - Total offheap working memory used after task 67ecf75e-e097-486d-b787-8b7db5f1d7c1 is 0. Current running tasks are 
After 0 training iterations, the accuracy of the model is: 0.27
After 10 training iterations, the accuracy of the model is: 0.48
After 20 training iterations, the accuracy of the model is: 0.78
After 30 training iterations, the accuracy of the model is: 0.69
After 40 training iterations, the accuracy of the model is: 0.73
After 50 training iterations, the accuracy of the model is: 0.79
After 60 training iterations, the accuracy of the model is: 0.85
After 70 training iterations, the accuracy of the model is: 0.73
After 80 training iterations, the accuracy of the model is: 0.86
After 90 training iterations, the accuracy of the model is: 0.80
After 99 training iterations, the accuracy of the model is: 0.79
all time: 185.28250288963318
Finish
```