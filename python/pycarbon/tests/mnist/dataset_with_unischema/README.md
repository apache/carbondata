# Pycarbon Tensorflow, Pytorch, Mxnet Example (carbon dataset has unischema)

using make_carbon_reader to read data

## Setup
```bash
cd python/
PYTHONPATH=/yourpath/carbondata/python/
pip install . --user
```

## Generating a Pycarbon Dataset from MNIST Data

This creates both a `train` and `test` pycarbon datasets in `/tmp/mnist`:

```bash
cd pycarbon/tests/mnist/dataset_with_unischema
python generate_pycarbon_mnist.py 
```

## Pytorch training using the PyCarbon MNIST Dataset

This will invoke a 10-epoch training run using MNIST data in pycarbon form,
stored by default in `/tmp/mnist`, and show accuracy against the test set:

```bash
python pytorch_example_carbon.py
```

```
usage: pytorch_example_carbon.py [-h] [--dataset-url S] [--batch-size N] [--test-batch-size N]
               [--epochs N] [--all-epochs] [--lr LR] [--momentum M]
               [--no-cuda] [--seed S] [--log-interval N]

Pycarbon Pytorch MNIST Example

optional arguments:
  -h, --help           show this help message and exit
  --dataset-url S      hdfs:// or file:/// URL to the MNIST pycarbon dataset
                       (default: file:///tmp/mnist)
  --batch-size N       input batch size for training (default: 64)
  --test-batch-size N  input batch size for testing (default: 1000)
  --epochs N           number of epochs to train (default: 10)
  --all-epochs         train all epochs before testing accuracy/loss
  --lr LR              learning rate (default: 0.01)
  --momentum M         SGD momentum (default: 0.5)
  --no-cuda            disables CUDA training
  --seed S             random seed (default: 1)
  --log-interval N     how many batches to wait before logging training status
```

## Tensorflow training using the Pycarboned MNIST Dataset

This will invoke a training run using MNIST data in pycarbon form,
for 100 epochs, using a batch size of 100, and log every 10 intervals.

```bash
python tf_example_carbon_unified_api.py
```

```
python tf_example_carbon_unified_api.py -h
usage: tf_example_carbon_unified_api.py [-h] [--dataset-url S] [--training-iterations N]
                     [--batch-size N] [--evaluation-interval N]

Pycarbon Tensorflow MNIST Example

optional arguments:
  -h, --help            show this help message and exit
  --dataset-url S       hdfs:// or file:/// URL to the MNIST pycarbon
                        dataset(default: file:///tmp/mnist)
  --training-iterations N
                        number of training iterations to train (default: 100)
  --batch-size N        input batch size for training (default: 100)
  --evaluation-interval N
                        how many batches to wait before evaluating the model
                        accuracy (default: 10)
```

## Mxnet training using the Pycarboned MNIST Dataset

This will invoke a training run using MNIST data in pycarbon form,
for 1 epoch, using a batch size of 100.

```bash
python mxnet_example_carbon.py
```

```
python mxnet_example_carbon.py -h
usage: mxnet_example_carbon.py [-h] [--dataset-url S] [--num-epoch N]
                     [--batch-size N]

Pycarbon Mxnet MNIST Example

optional arguments:
  -h, --help            show this help message and exit
  --dataset-url S       hdfs:// or file:/// URL to the MNIST pycarbon
                        dataset(default: file:///tmp/mnist)
  --num-epoch N
                        the number of epoch for training (default: 1)
  --batch-size N        input batch size for training (default: 100)
```