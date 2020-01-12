# PyCarbon

PyCarbon provides python API for integrating CarbonData with AI framework like  TensorFlow, PyTorch, MXNet. By using PyCarbon, AI framework can read training data faster by leveraging CarbonData's indexing and caching ability. Since CarbonData is a columnar storage, AI developer can also perform projection and filtering to pick required data for training efficiently.

## PyCarbon install

$ git clone https://github.com/apache/carbondata.git

$ cd python/pycarbon

$ pip install . --user

## how to use

if you have a CarbonData dataset, you can use PyCarbon to read data. For the generation of CarbonData dataset, you can see the examples:
`generate_external_dataset_carbon.py` in test/hello_world and `generate_pycarbon_dataset.py` in test/hello_world.

#### PySpark and SQL
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

some details are illustrated in `pyspark_hello_world_carbon.py` in test/hello_world.

#### TensorFlow Dataset API
    with make_carbon_reader('file:///some/localpath/a_dataset') as reader:
        dataset = make_pycarbon_dataset(reader)
        iterator = dataset.make_one_shot_iterator()
        tensor = iterator.get_next()
        with tf.Session() as sess:
            sample = sess.run(tensor)
            print(sample.id)

some details are illustrated in `tf_example_carbon.py` in test/mnist. 

#### PyTorch API
    with DataLoader(make_carbon_reader('file:///localpath/mnist/train', num_epochs=10,
                            transform_spec=transform), batch_size=64) as train_loader:
        train(model, device, train_loader, 10, optimizer, 1)

some details are illustrated in `pytorch_example_carbon.py` in test/mnist. 
