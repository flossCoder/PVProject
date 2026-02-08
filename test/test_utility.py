# Copyright (C) 2025, 2026 flossCoder
#
# This file is part of PVProject.
#
# PVProject is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# PVProject is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program.  If not, see <http://www.gnu.org/licenses/>.

import os

get_file_path = lambda : os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]

FILE_PATH_AUX = get_file_path()

SRC_PATH = "src"
TEST_PATH = "test"
TEST_RESOURCES_PATH = "test_resources"
TEST_DATA_PATH = "test_data"
TEST_RESULTS_PATH = "test_results"

def __check_path(path: str):
    """
    Internal function to create the given path, if it doesn't exist.

    Args:
        path (str): The input path.

    Returns:
        str: The input path.
    """
    if not os.path.exists(path):
        os.mkdir(path)
    return path

def get_src_path():
    """
    Get the path of the productive sources.

    Returns:
        str: The path of the productive sources.
    """
    return os.path.join(FILE_PATH_AUX, SRC_PATH)

def get_test_path():
    """
    Get the path of the test sources.

    Returns:
        str: The path of the test sources.
    """
    return os.path.join(FILE_PATH_AUX, TEST_PATH)

def get_test_resources_path():
    """
    Get the path of the test resources.

    Returns:
        str: The path of the test resources.
    """
    return __check_path(os.path.join(FILE_PATH_AUX, TEST_RESOURCES_PATH))

def get_test_data_path():
    """
    Get the path of the test data.

    Returns:
        str: The path of the test data.
    """
    return os.path.join(FILE_PATH_AUX, TEST_RESOURCES_PATH, TEST_DATA_PATH)

def get_test_results_path():
    """
    Get the path of the test results.

    Returns:
        str: The path of the test results.
    """
    return __check_path(os.path.join(FILE_PATH_AUX, TEST_RESOURCES_PATH, TEST_RESULTS_PATH))


def remove_file(file_path: str):
    """
    The function removes a file from the system.

    Args:
        file_path (str): The absolute file path of the file to remove.
    """
    if os.path.exists(file_path) and os.path.isfile(file_path):
        os.remove(file_path)

if __name__ == "__main__":
    print(get_test_results_path())
