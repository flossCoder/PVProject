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

from config import Config

CONFIG_FILENAME_VALID = "config_valid.json"

def test_config_valid():
    """
    Test a valid config file.
    """
    conf = Config(os.path.join(tu.get_test_data_path(), CONFIG_FILENAME_VALID))
    __validate_constants(conf)
    assert os.path.join(tu.get_test_data_path(), CONFIG_FILENAME_VALID) == conf.filename
    assert "testpath" == conf.wd
    assert all(["timestamp", "1.1", "1.2", "1.3", 'Production', 'Consumption'] == conf.data_columns)
    assert all(["timestamp", "Production_1_1", "Production_1_2", "Production_1_3", 'Production', 'Consumption'] == conf.db_columns)
    assert ";" == conf.separator
    assert "pvdb.db" == conf.db_name
    assert {'timestamp': 'DATE', 'Production_1_1': 'REAL', 'Production_1_2': 'REAL', 'Production_1_3': 'REAL', 'Production': 'REAL', 'Consumption': 'REAL', 'tracker_name': 'TEXT', "direction": "REAL", 'inclination_angle': 'REAL', 'latitude': 'REAL', 'longitude': 'REAL', 'solar_panel_width': 'REAL', 'solar_panel_height': 'REAL', 'solar_panel_energy_conversion_efficiency': 'REAL', 'solar_panel_number': 'REAL'} == conf.db_types
    assert ['main.raw', 'tracker.raw', 'tracker.meta'] == list(conf.tables.keys())
    assert 'main_raw' == conf.tables['main.raw'].table_name
    assert all(pd.core.indexes.base.Index(['timestamp', 'Production', 'Consumption'], dtype='object') == conf.tables['main.raw'].data_columns)
    assert ['DATE', 'REAL', 'REAL'] == conf.tables['main.raw'].data_types
    assert ['timestamp'] == conf.tables['main.raw'].primary_key_list
    assert 'tracker_raw' == conf.tables['tracker.raw'].table_name
    assert all(pd.core.indexes.base.Index(['timestamp', 'tracker_name', 'Production'], dtype='object') == conf.tables['tracker.raw'].data_columns)
    assert ['DATE', 'TEXT', 'REAL'] == conf.tables['tracker.raw'].data_types
    assert ['timestamp', 'tracker_name'] == conf.tables['tracker.raw'].primary_key_list
    assert 'tracker_meta' == conf.tables['tracker.meta'].table_name
    assert all(pd.core.indexes.base.Index(['tracker_name', 'direction', 'inclination_angle', 'latitude', 'longitude', 'solar_panel_width', 'solar_panel_height', 'solar_panel_energy_conversion_efficiency', 'solar_panel_number'], dtype='object') == conf.tables['tracker.meta'].data_columns)
    assert ['TEXT', 'REAL', 'REAL', 'REAL', 'REAL', 'REAL', 'REAL', 'REAL', 'REAL'] == conf.tables['tracker.meta'].data_types
    assert ['tracker_name'] == conf.tables['tracker.meta'].primary_key_list
    assert ['Production_1_1', 'Production_1_2', 'Production_1_3'] == conf.tracker_names
    assert ['tracker.meta'] == list(conf.meta_data.keys())
    assert all(pd.core.indexes.base.Index(['tracker_name', 'direction', 'inclination_angle', 'latitude', 'longitude', 'solar_panel_width', 'solar_panel_height', 'solar_panel_energy_conversion_efficiency', 'solar_panel_number'], dtype='object') == conf.meta_data['tracker.meta'].columns)
    meta_data = conf.meta_data['tracker.meta'].values.tolist()
    assert 1 == len(meta_data)
    assert ["A", "59", "53", "52.37352", "7.10110", "1755", "1038", "19.9", "10"] == meta_data[0]

def __validate_constants(conf: Config):
    """
    Validate the internal constants of the config.

    Args:
        conf (Config): The input config object.
    """
    assert "wd" == conf.WD_NAME
    assert "data" == conf.DATA
    assert "data.columns" == conf.DATA_COLUMNS
    assert "data.db.columns" == conf.DB_COLUMNS
    assert "separator" == conf.SEPARATOR
    assert "db.name" == conf.DB_NAME
    assert "db.types" == conf.DB_TYPES
    assert "columns" == conf.DB_TABLE_COLUMNS
    assert "primary" == conf.DB_TABLE_PRIMARY_KEY
    assert "table.name" == conf.TABLE_NAME
    assert "tracker.names" == conf.TRACKER_NAMES

if __name__ == "__main__":
    test_config_valid()
