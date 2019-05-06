#  Copyright (c) 2018-2019 Huawei Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import time
import argparse

import jnius_config
import mxnet as mx
import mxnet.ndarray as nd
from examples.mnist.pycarbon_dataset import DEFAULT_MNIST_DATA_PATH
from examples import DEFAULT_CARBONSDK_PATH

from pycarbon.carbon_reader import make_carbon_reader

logging.getLogger().setLevel(logging.INFO)


class MNISTIter:
    def __init__(self, dataset_path, data_name='data', label_name='softmax_label'):
        self.path = dataset_path
        self._provide_data = []
        self._provide_label = []

        reader = make_carbon_reader(dataset_path, num_epochs=1)
        self.iter = iter(reader)
        next_iter = next(self.iter)
        data = nd.array(next_iter.image).reshape(1, 1, 28, 28) / 255
        label = nd.array([next_iter.digit]).reshape(1, )
        self._provide_data = [mx.io.DataDesc(data_name, data.shape, data.dtype)]
        self._provide_label = [mx.io.DataDesc(label_name, label.shape, label.dtype)]

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        next_iter = next(self.iter)
        image = nd.array(next_iter.image).reshape(1, 1, 28, 28) / 255
        digit = nd.array([next_iter.digit])
        return mx.io.DataBatch(data=[image], label=[digit])

    @staticmethod
    def _decode_data(data):
        return data

    @staticmethod
    def _decode_label(label):
        return label

    def reset(self):
        pass

    @property
    def provide_data(self):
        return self._provide_data

    @property
    def provide_label(self):
        return self._provide_label


def mnist_iter_test(dataset_path, num_epoch, batch_size):
    train_data = MNISTIter(dataset_path)
    num_classes = 10
    data = mx.symbol.Variable('data')
    data = mx.sym.Flatten(data=data)

    fc1 = mx.symbol.FullyConnected(data=data, name='fc1', num_hidden=128)
    act1 = mx.symbol.Activation(data=fc1, name='relu1', act_type='relu')

    fc2 = mx.symbol.FullyConnected(data=act1, name='fc2', num_hidden=64)
    act2 = mx.symbol.Activation(data=fc2, name='relu2', act_type='relu')

    fc3 = mx.symbol.FullyConnected(data=act2, name='fc3', num_hidden=num_classes)
    mlp = mx.symbol.SoftmaxOutput(data=fc3, name='softmax')

    mx.viz.plot_network(mlp)
    mx.viz.print_summary(mlp, {'data': (32, 1, 28, 28), })

    devs = mx.cpu()
    model = mx.mod.Module(context=devs, symbol=mlp)

    kv = mx.kvstore.create('local')
    learning_rate = 0.01
    weight_decay = 0.0001
    optimizer_params = {'learning_rate': learning_rate, 'wd': weight_decay}
    initializer = mx.init.Xavier(rnd_type='gaussian', factor_type='in', magnitude=2)
    batch_size = batch_size
    batch_end_callbacks = [mx.callback.Speedometer(batch_size, 50)]

    model.fit(train_data=train_data,
              eval_data=None,
              begin_epoch=0,
              num_epoch=num_epoch,
              eval_metric=['accuracy'],
              kvstore=kv,
              optimizer='sgd',
              optimizer_params=optimizer_params,
              initializer=initializer,
              arg_params=None,
              aux_params=None,
              batch_end_callback=batch_end_callbacks,
              epoch_end_callback=None,
              allow_missing=True)
    print('train over.')


if __name__ == '__main__':
  print("Start")
  start = time.time()

  parser = argparse.ArgumentParser(description='Pycarbon MXNET MNIST Example')
  default_dataset_url = 'file://{}'.format(DEFAULT_MNIST_DATA_PATH)
  parser.add_argument('--dataset-url', type=str,
                      default=default_dataset_url, metavar='S',
                      help='hdfs:// or file:/// URL to the MNIST pycarbon dataset'
                           '(default: %s)' % default_dataset_url)
  parser.add_argument('--num-epoch', type=int, default=1, metavar='N',
                      help='the number of epoch for training (default: 1)')
  parser.add_argument('--batch-size', type=int, default=100, metavar='N',
                      help='input batch size for training (default: 100)')
  parser.add_argument('--carbon-sdk-path', type=str, default=DEFAULT_CARBONSDK_PATH,
                      help='carbon sdk path')
  args = parser.parse_args()

  jnius_config.set_classpath(args.carbon_sdk_path)

  mnist_iter_test(
    dataset_path=args.dataset_url + '/train',
    num_epoch=args.num_epoch,
    batch_size=args.batch_size
  )

  end = time.time()

  print("all time: " + str(end - start))
  print("Finish")
