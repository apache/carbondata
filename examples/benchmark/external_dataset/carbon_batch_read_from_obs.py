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
from obs import ObsClient

from pycarbon.carbon_reader import make_batch_carbon_reader

from unified.reader import make_reader

from examples import DEFAULT_CARBONSDK_PATH
from examples.benchmark.external_dataset.generate_benchmark_external_dataset import ROW_COUNT


def just_read_batch_obs(dataset_url="s3a://modelarts-carbon/test/benchmark_external_dataset/",
                        key=None, secret=None, endpoint=None, num_epochs=1):
  with make_batch_carbon_reader(dataset_url, key=key, secret=secret, endpoint=endpoint, num_epochs=num_epochs,
                                workers_count=16) as train_reader:
    i = 0
    for schema_view in train_reader:
      i += len(schema_view.id)

    assert i == ROW_COUNT * num_epochs
    return i


def just_unified_read_batch_obs(dataset_url="s3a://modelarts-carbon/test/benchmark_external_dataset/",
                                key=None, secret=None, endpoint=None, num_epochs=1):
  obs_client = ObsClient(
    access_key_id=key,
    secret_access_key=secret,
    server=endpoint,
    long_conn_mode=True
  )

  with make_reader(dataset_url, obs_client=obs_client, num_epochs=num_epochs,
                   workers_count=16) as train_reader:
    i = 0
    for schema_view in train_reader:
      i += len(schema_view.id)

    assert i == ROW_COUNT * num_epochs
    return i


def main():
  parser = argparse.ArgumentParser(description='Tensorflow hello world')
  parser.add_argument('-c', '--carbon-sdk-path', type=str, default=DEFAULT_CARBONSDK_PATH,
                      help='carbon sdk path')
  parser.add_argument('-ak', '--access_key', type=str, required=True,
                      help='access_key of obs')
  parser.add_argument('-sk', '--secret_key', type=str, required=True,
                      help='secret_key of obs')
  parser.add_argument('-endpoint', '--end_point', type=str, required=True,
                      help='end_point of obs')

  args = parser.parse_args()

  jnius_config.set_classpath(args.carbon_sdk_path)

  jnius_config.add_options('-Xrs', '-Xmx6096m')
  # jnius_config.add_options('-Xrs', '-Xmx6096m', '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5555')
  print("Start")
  start = time.time()

  just_read_batch_obs(key=args.access_key, secret=args.secret_key, endpoint=args.end_point)

  just_unified_read_batch_obs(key=args.access_key, secret=args.secret_key, endpoint=args.end_point)

  end = time.time()
  print("all time: " + str(end - start))
  print("Finish")


if __name__ == '__main__':
  main()
