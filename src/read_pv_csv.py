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

import pandas as pd
import os
import re

csvRegex = re.compile(r'\d{4}-\d{2}.csv')

def read_csv_file(wd: str, filename: str, separator: str) -> pd.core.frame.DataFrame:
    """
    Read the csv file.

    Args:
        wd (str): The working directory.
        filename (str): The name of the csv file.
        separator (str): The separator to parse the columns of the file.

    Returns:
        pd.core.frame.DataFrame: The DataFrame containing the data of the file.
    """
    filepath = os.path.join(wd, filename)
    data = pd.read_csv(filepath, sep = separator)
    return data

def aggregate_csv_data(pv_data: pd.core.frame.DataFrame, data: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    """
    Aggregate the input data to the pv_data

    Args:
        pv_data (pd.core.frame.DataFrame): The DataFrame containing the aggregated data.
        data (pd.core.frame.DataFrame): The data of a single file.

    Returns:
        pd.core.frame.DataFrame: The DataFrame containing the aggregated data.
    """
    if pv_data.columns.equals(data.columns) or pv_data.columns.stop == 0:
        pv_data = pd.concat([pv_data, data], ignore_index = True)
    return pv_data

def aggregate_csv_file_data(wd: str, filename: str, separator: str, pv_data: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    """
    Aggregate the csv data of the input file to the result.

    Args:
        wd (str): The working directory.
        filename (str): The name of a single csv file.
        separator (str): The separator to parse the columns of the file.
        pv_data (pd.core.frame.DataFrame): The DataFrame containing the aggregated data.

    Returns:
        pd.core.frame.DataFrame: The DataFrame containing the aggregated data.
    """
    if csvRegex.fullmatch(filename):
        data = read_csv_file(wd, filename, separator)
        pv_data = aggregate_csv_data(pv_data, data)
    return pv_data

def search_csv_files(wd: str, separator: str) -> pd.core.frame.DataFrame:
    """
    Read all csv files in the given working directory, where the data columns are equals to the given index.

    Args:
        wd (str): The working directory.
        separator (str): The separator to parse the columns of the file.

    Returns:
        pd.core.frame.DataFrame: The DataFrame containing the data of the files found in the working directory.
    """
    pv_data = pd.DataFrame()
    for filename in os.listdir(wd):
        pv_data = aggregate_csv_file_data(wd, filename, separator, pv_data)
    return pv_data
