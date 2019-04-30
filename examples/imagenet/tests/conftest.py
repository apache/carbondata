from examples import DEFAULT_CARBONSDK_PATH

def pytest_addoption(parser):
  parser.addoption('--pyspark-python', type=str, default=None,
                   help='pyspark python env variable')
  parser.addoption('--pyspark-driver-python', type=str, default=None,
                   help='pyspark driver python env variable')
  parser.addoption('--carbon-sdk-path', type=str, default=DEFAULT_CARBONSDK_PATH,
                   help='carbon sdk path')
