from examples.benchmark import carbon_read_from_local

import pytest
import os
import jnius_config

jnius_config.set_classpath(pytest.config.getoption("--carbon-sdk-path"))

jnius_config.add_options('-Xrs', '-Xmx6g')


def test_carbon_read():
  for i in range(2):
    print("\n number of " + str(i))
    carbon_read_from_local.just_read()
    carbon_read_from_local.just_read_batch()
