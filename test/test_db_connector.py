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

from db_connector import DBConnector, DBTable

CREATE_DB_NAME = "test.db"
TABLE_NAME = "test"
DATA_COLUMNS = pd.core.indexes.base.Index(["timestamp", "tracker_name", "Production"])
DATA_TYPES = ["DATE", "TEXT", "REAL"]
PRIMARY_KEY_LIST = ["timestamp", "tracker_name"]
DATA = [["2023-03-02 16:00", "a", 6], ["2023-03-02 16:00", "b", 7], ["2023-03-02 16:15", "a", 5], ["2023-03-02 16:15", "b", 8]]
DATA_DF = pd.DataFrame(DATA, columns = DATA_COLUMNS)

def test_DBTable():
    dbTable = __test_DBTable(TABLE_NAME, DATA_COLUMNS, DATA_TYPES,PRIMARY_KEY_LIST)
    column_dict = dbTable.get_column_dict()
    assert all(list(column_dict.keys()) == DATA_COLUMNS)
    assert list(column_dict.values()) == DATA_TYPES

def __test_DBTable(table_name: str, data_columns: pd.core.indexes.base.Index, data_types: list[str], primary_key_list: list[str] = []):
    dbTable = DBTable(table_name, data_columns, data_types, primary_key_list)
    assert dbTable.table_name == table_name
    assert all(dbTable.data_columns == data_columns)
    assert dbTable.data_types == data_types
    assert dbTable.primary_key_list == primary_key_list
    return dbTable

def test_insert_into_table():
    __test_insert_into_table()
    tu.remove_file(os.path.join(tu.get_test_results_path(), CREATE_DB_NAME))

def test_create_table():
    __test_create_table()
    tu.remove_file(os.path.join(tu.get_test_results_path(), CREATE_DB_NAME))

def __test_insert_into_table():
    dbConnector, dbTable = __test_create_table()
    dbConnector.insert_data(dbTable, DATA_DF)
    data = dbConnector.select_data_unfiltered(dbTable.table_name)
    assert all(DATA_DF.columns == data.columns)
    assert all(DATA_DF == data)
    return dbConnector, dbTable

def __test_create_table():
    dbConnector = DBConnector(tu.get_test_results_path(), CREATE_DB_NAME)
    assert dbConnector.wd == tu.get_test_results_path()
    assert dbConnector.db_name == CREATE_DB_NAME
    assert dbConnector.db_fullpath == os.path.join(tu.get_test_results_path(), CREATE_DB_NAME)
    dbTable = __test_DBTable(TABLE_NAME, DATA_COLUMNS, DATA_TYPES,PRIMARY_KEY_LIST)
    dbConnector.create_table(dbTable)
    file_path = os.path.join(tu.get_test_results_path(), CREATE_DB_NAME)
    assert os.path.exists(file_path) and os.path.isfile(file_path)
    conn = sqlite3.connect(file_path)
    cur = conn.cursor()
    try:
        table_list = cur.execute("""SELECT name FROM sqlite_master WHERE type='table' AND name='%s';"""%TABLE_NAME).fetchall()
        assert 1 == len(table_list)
        assert 1 == len(table_list[0])
        assert TABLE_NAME == table_list[0][0]
    except Exception as e:
        conn.close()
        raise e
    dbConnector.create_index("idx_" + dbTable.table_name, dbTable.table_name, dbTable.primary_key_list)
    try:
        table_list = cur.execute("""SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='%s' AND name='%s';"""%(TABLE_NAME, "idx_" + TABLE_NAME)).fetchall()
        assert 1 == len(table_list)
        assert 1 == len(table_list[0])
        assert "idx_" + TABLE_NAME == table_list[0][0]
    except Exception as e:
        conn.close()
        raise e
    conn.close()
    return dbConnector, dbTable

if __name__ == "__main__":
    test_insert_into_table()
