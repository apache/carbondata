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
import argparse

from examples import DEFAULT_CARBONSDK_PATH
from pycarbon.pysdk.CarbonSchemaReader import CarbonSchemaReader
from pycarbon.pysdk.Configuration import Configuration

from pycarbon.tests import S3_DATA_PATH


def main():
  print("Start")

  import jnius_config

  parser = argparse.ArgumentParser(description='test Read Carbon From Local')
  parser.add_argument('-c', '--carbon-sdk-path', type=str, default=DEFAULT_CARBONSDK_PATH,
                      help='carbon sdk path')
  parser.add_argument('-d', '--data-path', type=str, default=S3_DATA_PATH,
                      help='carbon sdk path')
  parser.add_argument('-ak', '--access_key', type=str, required=True,
                      help='access_key of obs')
  parser.add_argument('-sk', '--secret_key', type=str, required=True,
                      help='secret_key of obs')
  parser.add_argument('-endpoint', '--end_point', type=str, required=True,
                      help='end_point of obs')

  args = parser.parse_args()

  jnius_config.set_classpath(args.carbon_sdk_path)

  print("\n\ntest4:")
  configuration = Configuration()

  configuration.set("fs.s3a.access.key", args.access_key)
  configuration.set("fs.s3a.secret.key", args.secret_key)
  configuration.set("fs.s3a.endpoint", args.end_point)

  carbonSchemaReader = CarbonSchemaReader()
  schema = carbonSchemaReader.readSchema(
    "s3a://sdk/binary/sub2", False, True, configuration.conf)
  print(schema.getFieldsLength())
  assert 5 == schema.getFieldsLength()
  for each in schema.getFields():
    print(each.getFieldName(), end="\t")
    print(each.getDataType().toString(), end="\t")
    print(each.getSchemaOrdinal())

  print("Finish")


if __name__ == '__main__':
  main()
