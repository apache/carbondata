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

import os

from examples import DEFAULT_CARBONSDK_PATH
from pycarbon.pysdk.CarbonReader import CarbonReader
import sys
import time
from multiprocessing.dummy import Pool as ThreadPool

from pycarbon.tests import DATA_PATH


def main(argv):
  # os.environ['JAVA_HOME'] = '/usr/lib/jvm/jdk1.8.0_181'
  print("Start")
  start = time.time()
  print(argv)
  import jnius_config

  # Configure carbonData java SDK jar path
  jnius_config.set_classpath(DEFAULT_CARBONSDK_PATH)
  jnius_config.add_options('-Xrs', '-Xmx3g')

  reader = CarbonReader() \
    .builder() \
    .withFolder(DATA_PATH) \
    .withBatch(1000) \
    .build()

  # TODO support thread pool to read
  readers = reader.splitAsArray(int(3))
  pool = ThreadPool(len(readers))

  def readLogic(carbonReader):
    i = 0
    while (carbonReader.hasNext()):
      rows = carbonReader.readNextBatchRow()
      for row in rows:
        i = i + 1
        if 0 == i % 1000:
          print(i)
        for column in row:
          column

    print(i)
    carbonReader.close()

  results = pool.map(readLogic, readers)
  pool.close()

  end = time.time()
  print("all time is " + str(end - start))
  print("Finish")


if __name__ == '__main__':
  main(sys.argv)
