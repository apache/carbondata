from examples import DEFAULT_CARBONSDK_PATH


def pytest_addoption(parser):
  parser.addoption('--carbon-sdk-path', type=str, default=DEFAULT_CARBONSDK_PATH,
                   help='carbon sdk path')
