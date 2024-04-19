import pytest


def pytest_addoption(parser):
    parser.addoption("--debug_dir", action="store", default="")
