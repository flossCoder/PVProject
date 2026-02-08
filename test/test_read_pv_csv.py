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

import test_utility as tu
import sys
sys.path.append(tu.get_src_path())
import pytest
import os

import pandas as pd

from read_pv_csv import search_csv_files

DATA_DIR = "data"
SEPARATOR = ";"

def test_search_csv_files():
    """
    Test searing csv files.
    """
    pv_data = search_csv_files(os.path.join(tu.get_test_data_path(), DATA_DIR), SEPARATOR)
    assert ["timestamp", "1.1", "1.2", "1.3", "Production", "Consumption"] == pv_data.columns.tolist()
    assert [["2023-03-02 16:00", 1, 2, 3, 6, 7], ["2023-03-02 16:15", 1, 2, 3, 6, 7], ["2023-03-02 16:30", 1, 2, 3, 6, 7], ["2023-03-02 16:45", 1, 2, 3, 6, 7], ["2023-03-02 17:00", 1, 2, 3, 6, 7], ["2023-03-02 17:15", 1, 2, 3, 6, 7], ["2023-04-02 16:00", 1, 2, 3, 6, 7], ["2023-04-02 16:15", 1, 2, 3, 6, 7], ["2023-04-02 16:30", 1, 2, 3, 6, 7], ["2023-04-02 16:45", 1, 2, 3, 6, 7], ["2023-04-02 17:00", 1, 2, 3, 6, 7], ["2023-04-02 17:15", 1, 2, 3, 6, 7]] == pv_data.values.tolist()

if __name__ == "__main__":
    test_search_csv_files()
