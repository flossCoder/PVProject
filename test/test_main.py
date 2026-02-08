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
import sqlite3

from main import Main

CONFIG_FILENAME_VALID = "config_valid.json"
DB_NAME = "pvdb.db"
TABLE_NAMES = ["main_raw", "tracker_raw", "tracker_meta"]
DATA_DIR = "data"
META_DATA = ('A', 59.0, 53.0, 52.37352, 7.1011, 1755.0, 1038.0, 19.9, 10.0)
MAIN_DATA = [("2023-03-02 16:00", 6, 7), ("2023-03-02 16:15", 6, 7), ("2023-03-02 16:30", 6, 7), ("2023-03-02 16:45", 6, 7), ("2023-03-02 17:00", 6, 7), ("2023-03-02 17:15", 6, 7), ("2023-04-02 16:00", 6, 7), ("2023-04-02 16:15", 6, 7), ("2023-04-02 16:30", 6, 7), ("2023-04-02 16:45", 6, 7), ("2023-04-02 17:00", 6, 7), ("2023-04-02 17:15", 6, 7)]
TRACKER_DATA = [("2023-03-02 16:00", "1.1", 1.0), ("2023-03-02 16:15", "1.1", 1.0), ("2023-03-02 16:30", "1.1", 1.0), ("2023-03-02 16:45", "1.1", 1.0), ("2023-03-02 17:00", "1.1", 1.0), ("2023-03-02 17:15", "1.1", 1.0), ("2023-03-02 16:00", "1.2", 2.0), ("2023-03-02 16:15", "1.2", 2.0), ("2023-03-02 16:30", "1.2", 2.0), ("2023-03-02 16:45", "1.2", 2.0), ("2023-03-02 17:00", "1.2", 2.0), ("2023-03-02 17:15", "1.2", 2.0), ("2023-03-02 16:00", "1.3", 3.0), ("2023-03-02 16:15", "1.3", 3.0), ("2023-03-02 16:30", "1.3", 3.0), ("2023-03-02 16:45", "1.3", 3.0), ("2023-03-02 17:00", "1.3", 3.0), ("2023-03-02 17:15", "1.3", 3.0), ("2023-04-02 16:00", "1.1", 1.0), ("2023-04-02 16:15", "1.1", 1.0), ("2023-04-02 16:30", "1.1", 1.0), ("2023-04-02 16:45", "1.1", 1.0), ("2023-04-02 17:00", "1.1", 1.0), ("2023-04-02 17:15", "1.1", 1.0), ("2023-04-02 16:00", "1.2", 2.0), ("2023-04-02 16:15", "1.2", 2.0), ("2023-04-02 16:30", "1.2", 2.0), ("2023-04-02 16:45", "1.2", 2.0), ("2023-04-02 17:00", "1.2", 2.0), ("2023-04-02 17:15", "1.2", 2.0), ("2023-04-02 16:00", "1.3", 3.0), ("2023-04-02 16:15", "1.3", 3.0), ("2023-04-02 16:30", "1.3", 3.0), ("2023-04-02 16:45", "1.3", 3.0), ("2023-04-02 17:00", "1.3", 3.0), ("2023-04-02 17:15", "1.3", 3.0)]

def test_insert_raw_data():
    main = __test_create_tables()
    main.insert_raw_data()
    file_path = os.path.join(tu.get_test_data_path(), DATA_DIR, DB_NAME)
    assert os.path.exists(file_path) and os.path.isfile(file_path)
    conn = sqlite3.connect(file_path)
    cur = conn.cursor()
    try:
        main_data = cur.execute("""SELECT * FROM main_raw ORDER BY timestamp""").fetchall()
        assert 12 == len(main_data)
        assert MAIN_DATA == main_data
    except Exception as e:
        conn.close()
        raise e
    try:
        tracker_data = cur.execute("""SELECT * FROM tracker_raw ORDER BY timestamp, tracker_name""").fetchall()
        assert 36 == len(tracker_data)
        assert all([i in tracker_data for i in TRACKER_DATA]) and all([i in TRACKER_DATA for i in tracker_data])
    except Exception as e:
        conn.close()
        raise e
    conn.close()
    tu.remove_file(os.path.join(tu.get_test_data_path(), DATA_DIR, DB_NAME))

def test_create_tables():
    __test_create_tables()
    tu.remove_file(os.path.join(tu.get_test_data_path(), DATA_DIR, DB_NAME))

def __test_create_tables():
    main = Main(os.path.join(tu.get_test_data_path(), CONFIG_FILENAME_VALID))
    main.config.wd = os.path.join(tu.get_test_data_path(), DATA_DIR)
    main.db_connector.wd = os.path.join(tu.get_test_data_path(), DATA_DIR)
    main.db_connector.db_fullpath = os.path.join(main.db_connector.wd, main.db_connector.db_name)
    main.create_tables()
    file_path = os.path.join(tu.get_test_data_path(), DATA_DIR, DB_NAME)
    assert os.path.exists(file_path) and os.path.isfile(file_path)
    conn = sqlite3.connect(file_path)
    cur = conn.cursor()
    try:
        for table_name in TABLE_NAMES:
            table_list = cur.execute("""SELECT name FROM sqlite_master WHERE type='table' AND name='%s';"""%table_name).fetchall()
            assert 1 == len(table_list)
            assert 1 == len(table_list[0])
            assert table_name == table_list[0][0]
    except Exception as e:
        conn.close()
        raise e
    try:
        for table_name in TABLE_NAMES:
            table_list = cur.execute("""SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='%s' AND name='%s';"""%(table_name, "idx_" + table_name)).fetchall()
            assert 1 == len(table_list)
            assert 1 == len(table_list[0])
            assert "idx_" + table_name == table_list[0][0]
    except Exception as e:
        conn.close()
        raise e
    try:
        meta_data = cur.execute("""SELECT * FROM tracker_meta""").fetchall()
        assert 1 == len(meta_data)
        assert 9 == len(meta_data[0])
        assert META_DATA == meta_data[0]
    except Exception as e:
        conn.close()
        raise e
    conn.close()
    return main

if __name__ == "__main__":
    test_insert_raw_data()
