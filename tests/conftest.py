import pytest

def pytest_addoption(parser):
  parser.addoption("--prepdata", action="store_true",
    help="prepare test data on Broker")


def pytest_runtest_setup(item):
  if 'prepdata' in item.keywords and not item.config.getoption("--prepdata"):
    pytest.skip("need --prepdata option to run this test")
