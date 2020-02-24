# PyCarbon

PyCarbon provides python API for integrating CarbonData with AI framework like  TensorFlow, PyTorch, MXNet. By using PyCarbon, AI framework can read training data faster by leveraging CarbonData's indexing and caching ability. Since CarbonData is a columnar storage, AI developer can also perform projection and filtering to pick required data for training efficiently.

## PyCarbon install

$ git clone https://github.com/apache/carbondata.git

$ cd python/pycarbon

$ pip install . --user


## how to use

if you have a CarbonData dataset, you can use PyCarbon to read data. For the generation of CarbonData dataset, you can see the examples:
`generate_dataset_carbon.py` in tests/hello_world/dataset_with_unischema.
But user should do some config first:

 - config pyspark and add carbon assembly jar to pyspark/jars folder, which can be compiled from CarbonData project.
 - default Java sdk jar is in carbondata/sdk/sdk/target,  user also can specific the jar location like by --carbon-sdk-path in generate_pycarbon_dataset.py.

 - set JAVA_HOME, PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON in you system environment.

### Generating the data:
```

    python generate_pycarbon_mnist.py --carbon-sdk-path  /your_path/carbondata/sdk/sdk/target/carbondata-sdk.jar 

```
#### Part of code:
```
      spark.createDataFrame(sql_rows, MnistSchema.as_spark_schema()) \
        .coalesce(carbon_files_count) \
        .write \
        .save(path=dset_output_url, format='carbon')
```
some details are illustrated in `generate_pycarbon_mnist.py <https://github.com/apache/carbondata/blob/master/python/pycarbon/tests/mnist/dataset_with_unischema/generate_pycarbon_mnist.py>` in test/hello_world.

### PyCarbon Reader API

```

def make_reader(dataset_url=None,
                workers_count=10,
                results_queue_size=100,
                num_epochs=1,
                shuffle=True,
                is_batch=True
                ):
  """
  an unified api for different data format dataset

  :param dataset_url: an filepath or a url to a carbon directory,
      e.g. ``'hdfs://some_hdfs_cluster/user/yevgeni/carbon8'``, or ``'file:///tmp/mydataset'``
      or ``'s3a://bucket/mydataset'``.
  :param workers_count: An int for the number of workers to use in the reader pool. This only is used for the
      thread or process pool. Defaults to 10
  :param results_queue_size: Size of the results queue to store prefetched rows. Currently only applicable to
      thread reader pool type.
  :param shuffle: Whether to shuffle partition (the order in which full partition are read)
  :param num_epochs: An epoch is a single pass over all rows in the dataset. Setting ``num_epochs`` to
      ``None`` will result in an infinite number of epochs.
  :param is_batch: return single record or batch records (default: True)
  :return: A :class:`Reader` object
  """
```

### TensorFlow Dataset API
```python

def make_dataset(reader):
  """
  Creates a `tensorflow.data.Dataset <https://www.tensorflow.org/api_docs/python/tf/data/Dataset>`_ object from

  :param reader: An instance of PyCarbon Reader object that would serve as a data source.
  :return: A ``tf.data.Dataset`` instance.
  """
```

### TensorFlow Tensor API
```python

def make_tensor(reader):
  """Bridges between python-only interface of the Reader (next(Reader)) and tensorflow world.

  This function returns a named tuple of tensors from the dataset, 

  :param reader: An instance of Reader object used as the data source
  """
```

#### Running train and test based on mnist:

```
    python  tf_example_carbon_unified_api.py --carbon-sdk-path  /your_path/carbondata/sdk/sdk/target/carbondata-sdk.jar 

```
#### Part or code:
```
    with make_reader('file:///some/localpath/a_dataset') as reader:
        dataset = make_dataset(reader)
        iterator = dataset.make_one_shot_iterator()
        tensor = iterator.get_next()
        with tf.Session() as sess:
            sample = sess.run(tensor)
            print(sample.id)

```
some details are illustrated in `tf_example_carbon_unified_api.py <https://github.com/apache/carbondata/blob/master/python/pycarbon/tests/mnist/dataset_with_unischema/tf_example_carbon_unified_api.py>` in tests/mnist. 

####  Part of result:

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

### PyTorch Dataset API

```python
def make_data_loader(reader, batch_size=1):
  """
  Initializes a data loader object, with a default collate.

  Number of epochs is defined by the configuration of the reader argument.

  :param reader: PyCarbon Reader instance
  :param batch_size: the number of items to return per batch; factored into the len() of this reader
  """
```

####  Part or code:
```python

  with make_data_loader(make_reader('{}/train'.format(args.dataset_url), is_batch=False, num_epochs=reader_epochs,
                                      transform_spec=transform),
                          batch_size=args.batch_size) as train_loader:
      train(model, device, train_loader, args.log_interval, optimizer, epoch)
      
```
some details are illustrated in `pytorch_example_carbon_unified_api.py <https://github.com/apache/carbondata/blob/master/python/pycarbon/tests/mnist/dataset_with_unischema/pytorch_example_carbon_unified_api.py>` in tests/mnist. 

####  Part of result:
```python
Train Epoch: 10 [55680]	Loss: 0.255295
Train Epoch: 10 [56320]	Loss: 0.132586
Train Epoch: 10 [56960]	Loss: 0.197574
Train Epoch: 10 [57600]	Loss: 0.280921
Train Epoch: 10 [58240]	Loss: 0.072130
Train Epoch: 10 [58880]	Loss: 0.027580
Train Epoch: 10 [59520]	Loss: 0.036734
Test set: Average loss: 0.0508, Accuracy: 9843/10000 (98%)

```
