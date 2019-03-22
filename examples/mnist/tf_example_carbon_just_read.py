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
import numpy as np
import cv2 as cv

import jnius_config

from petastorm import make_carbon_reader, make_batch_carbon_reader


def just_read(dataset_url):
    with make_carbon_reader(dataset_url, num_epochs=1, workers_count=1, reader_pool_type='process') as train_reader:
        i = 0
        start = time.time()
        for schema_view in train_reader:
            # print(schema_view.imagename)
            schema_view.imagebinary
            i += 1
            if i % 1281167 == 0:
                end = time.time()
                print("time is " + str(end - start))
                start = end
        print(i)


def just_read_batch(dataset_url):
    with make_batch_carbon_reader(dataset_url, num_epochs=1, workers_count=1,
                                  reader_pool_type='thread') as train_reader:
        i = 0
        start = time.time()
        for schema_view in train_reader:
            # print(schema_view.imagename)
            # i += len(schema_view.imagename)
            for j in range(len(schema_view.imagename)):
                schema_view.imagebinary[j]
                i += 1
                if i % 1281167 == 0:
                    end = time.time()
                    print("time is " + str(end - start))
                    start = end

        print(i)

def main():
    # os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
    # os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
    # os.environ['JAVA_HOME'] = '/usr/lib/jvm/jdk1.8.0_181'
    # TODO: fill this in argument
    jnius_config.set_classpath(
        "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/carbondata-sdk.jar")
    jnius_config.add_options('-Xrs', '-Xmx6096m')
    # jnius_config.add_options('-Xrs', '-Xmx6096m', '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5555')
    print("Start")
    start = time.time()

    # just_read("file:///tmp/mnistcarbon/train")
    path = "/Users/xubo/Desktop/xubo/data/VOCdevkit/carbon/voc"

    # path = "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers"
    reader = make_batch_carbon_reader("file://" + path, reader_pool_type='thread')
    # just_read_batch("file:///home/root1/Documents/ab/workspace/historm_xubo/historm/store/sdk/target/voc/")
    i = 0
    for schema_view in reader:
        # print(schema_view.imagebinary)
        for binary in schema_view.imagebinary:
            nparr = np.fromstring(binary, np.uint8)
            img_np = cv.imdecode(nparr, cv.IMWRITE_PAM_FORMAT_RGB)

            cv.imshow("Image", img_np)
            cv.waitKey(0)
            # i += len(schema_view.idx)
    print(i)
    end = time.time()
    print("all time: " + str(end - start))
    print("Finish")


if __name__ == '__main__':
    main()
