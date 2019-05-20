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

import argparse
import time
import os

import jnius_config

from examples.benchmark.external_dataset.generate_benchmark_external_dataset import ROW_COUNT
from pycarbon.Constants import LOCAL_FILE_PREFIX
from pycarbon.carbon_reader import make_batch_carbon_reader

from examples import DEFAULT_CARBONSDK_PATH


def just_read(dataset_url=LOCAL_FILE_PREFIX + os.path.abspath('../') + '/data/binary1558365345315.manifest'):
  with make_batch_carbon_reader(dataset_url, num_epochs=1) as train_reader:
    i = 0
    start = time.time()
    for schema_view in train_reader:
      print(schema_view.name)
      i += len(schema_view.name)
      if i % ROW_COUNT == 0:
        end = time.time()
        print("time is " + str(end - start))
        start = end
    print(i)
    assert 20 == i


def just_read_batch(
        dataset_url=LOCAL_FILE_PREFIX + os.path.abspath('../') + '/data/binary1558365345315.manifest'):
  with make_batch_carbon_reader(dataset_url, num_epochs=1) as train_reader:
    i = 0
    start = time.time()
    for schema_view in train_reader:
      for j in range(len(schema_view.name)):
        print(schema_view.name[j])
        i += 1
        if i % ROW_COUNT == 0:
          end = time.time()
          print("time is " + str(end - start))
          start = end

    print(i)
    assert 20 == i


def main():
  parser = argparse.ArgumentParser(description='Support read carbon by manifest path in python code')
  parser.add_argument('-c', '--carbon-sdk-path', type=str, default=DEFAULT_CARBONSDK_PATH,
                      help='carbon sdk path')

  args = parser.parse_args()

  jnius_config.set_classpath(args.carbon_sdk_path)

  jnius_config.add_options('-Xrs', '-Xmx6096m')
  print("Start")
  start = time.time()

  just_read()

  just_read_batch()

  end = time.time()
  print("all time: " + str(end - start))
  print("Finish")


if __name__ == '__main__':
  main()
