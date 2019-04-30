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

###
# Adapted to pycarbon dataset using original contents from
# https://github.com/tensorflow/tensorflow/blob/master/tensorflow/examples/tutorials/mnist/mnist_softmax.py
###

from __future__ import division, print_function

import time

import argparse
import jnius_config

from pycarbon.carbon_reader import make_carbon_reader, make_batch_carbon_reader

from examples import DEFAULT_CARBONSDK_PATH


def just_read(dataset_url='file:///tmp/benchmark_dataset'):
  result = list()
  with make_carbon_reader(dataset_url, num_epochs=1, workers_count=16, shuffle_row_drop_partitions=10) as train_reader:
    i = 0
    for schema_view in train_reader:
      result.append(schema_view.id)
      i += 1
    print(i)
    print(result)


def just_read_batch(dataset_url='file:///tmp/benchmark_dataset'):
  with make_batch_carbon_reader(dataset_url, num_epochs=1, workers_count=16,
                                shuffle_row_drop_partitions=5) as train_reader:
    result = list()
    i = 0
    for schema_view in train_reader:
      i += len(schema_view.id)
      for id in schema_view.id:
        result.append(id)
    print(i)
    print(result)


def main():
  parser = argparse.ArgumentParser(description='Tensorflow hello world')
  parser.add_argument('-c', '--carbon-sdk-path', type=str, default=DEFAULT_CARBONSDK_PATH,
                      help='carbon sdk path')

  args = parser.parse_args()

  jnius_config.set_classpath(args.carbon_sdk_path)

  jnius_config.add_options('-Xrs', '-Xmx6096m')
  # jnius_config.add_options('-Xrs', '-Xmx6096m', '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5555')
  print("Start")
  start = time.time()

  just_read()

  just_read_batch()

  end = time.time()
  print("all time: " + str(end - start))
  print("Finish")


if __name__ == '__main__':
  main()
