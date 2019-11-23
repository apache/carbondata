# Pycarbon Tensorflow, Pytorch, Mxnet Example (carbon dataset does not have unischema)

using make_batch_carbon_reader to read data

## Setup
```bash
PYTHONPATH=~/dev/pycarbon  # replace with your pycarbon install path
```

## Generating a Pycarbon Dataset from MNIST Data

This creates both a `train` and `test` carbon datasets in `/tmp/mnist_external`:

```bash
python generate_external_mnist_carbon.py
```

## TODO: Pytorch training using the Carbon MNIST Dataset


## Tensorflow training using the Carbon MNIST Dataset

This will invoke a training run using MNIST carbondata,
for 1 epochs, using a batch size of 100, and log every 10 intervals.

```bash
python tf_external_example_carbon.py
```

```
python tf_external_example_carbon.py -h
usage: tf_external_example_carbon.py [-h] [--dataset-url S] [--num-epochs N]
                     [--batch-size N] [--evaluation-interval N]

Pycarbon Tensorflow MNIST Example

optional arguments:
  -h, --help            show this help message and exit
  --dataset-url S       hdfs:// or file:/// URL to the MNIST pycarbon
                        dataset(default: file:///tmp/mnist_external)
  --num-epochs N
                        number of epochs to train (default: 1)
  --batch-size N        input batch size for training (default: 100)
  --evaluation-interval N
                        how many batches to wait before evaluating the model
                        accuracy (default: 10)
```

## TODO: Mxnet training using the Carbon MNIST Dataset
