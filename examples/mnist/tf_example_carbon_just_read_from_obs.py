#  Copyright (c) 2017-2018 Uber Technologies, Inc.
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
# Adapted to petastorm dataset using original contents from
# https://github.com/tensorflow/tensorflow/blob/master/tensorflow/examples/tutorials/mnist/mnist_softmax.py
###

from __future__ import division, print_function

import os
import time

import jnius_config

from petastorm import make_carbon_reader, make_batch_carbon_reader


def just_read_obs(dataset_url, key, secret, endpoint):
    with make_carbon_reader(dataset_url, key=key, secret=secret, endpoint=endpoint, num_epochs=1, workers_count=16) as train_reader:
        i = 0
        for schema_view in train_reader:
            # print(schema_view.imagename)
            i = i + 1
        print(i)

def just_read_batch_obs(dataset_url, key, secret, endpoint):
    with make_batch_carbon_reader(dataset_url, key=key, secret=secret, endpoint=endpoint, num_epochs=1, workers_count=16) as train_reader:
        i = 0
        for schema_view in train_reader:
            # print(schema_view.imagename)
            i += len(schema_view.imagename)
        print(i)

def main():
    # os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
    # os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
    # os.environ['JAVA_HOME'] = '/usr/lib/jvm/jdk1.8.0_181'
    # TODO: fill this in argument
    jnius_config.set_classpath(
        "")
    jnius_config.add_options('-Xrs', '-Xmx6096m')
    # jnius_config.add_options('-Xrs', '-Xmx6096m', '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5555')
    print("Start")
    start = time.time()

    key = "OF0FTHGASIHDTRYHBCWU"
    secret = "fWWjJwh89NFaMDPrFdhu68Umus4vftlIzcNuXvwV"
    endpoint = "http://obs.cn-north-5.myhuaweicloud.com"

    # just_read_obs("s3a://modelarts-carbon/imageNet_resize/carbon/", key, secret, endpoint)
    just_read_batch_obs("s3a://modelarts-carbon/imageNet_resize/carbon/", key, secret, endpoint)

    end = time.time()
    print("all time: " + str(end - start))
    print("Finish")

if __name__ == '__main__':
    main()
