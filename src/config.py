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

import json
import pandas as pd

from db_connector import DBTable

class Config:
    """
    The base config class.
    """
    WD_NAME = "wd"
    DATA = "data"
    DATA_COLUMNS = "data.columns"
    DB_COLUMNS = "data.db.columns"
    SEPARATOR = "separator"
    DB_NAME = "db.name"
    DB_TYPES = "db.types"
    DB_TABLES = "db.tables"
    DB_TABLE_COLUMNS = "columns"
    DB_TABLE_PRIMARY_KEY = "primary"
    TABLE_NAME = "table.name"
    TRACKER_NAMES = "tracker.names"

    def __init__(self, filename: str):
        """
        Initialize the config object.

        Args:
            filename (str): The filename of the json config.
        
        Raises:
            Exception: The exception is thrown in case no valid json has been found.
        """
        self.filename = filename
        self.wd = None
        self.data_columns = None
        self.separator = None
        self.db_name = None
        self.db_types = None
        self.db_columns = None
        self.tables = {}
        self.tracker_names = None
        self.meta_data = {}
        self.__read_config()

    def get_db_column_name(self, data_name: str) -> str:
        """
        Get the db representation of a data column.

        Args:
            data_name (str): The input data columne name.

        Raises:
            Exception: The exception is raised in case an invalid input is given.

        Returns:
            str: The db representation of the data column.
        """
        if data_name in self.data_columns:
            return self.db_columns[self.data_columns == data_name][0]
        else:
            raise Exception("The input data %s is not in the data columns"%(data_name))

    def get_data_column_name(self, db_name: str) -> str:
        """
        Get the data representation of a db column.

        Args:
            db_name (str): The input db columne name.

        Raises:
            Exception: The exception is raised in case an invalid input is given.

        Returns:
            str: The data representation of the db column.
        """
        if db_name in self.db_columns:
            return self.data_columns[self.db_columns == db_name][0]
        else:
            raise Exception("The input data %s is not in the db columns"%(db_name))

    def __read_config(self):
        """
        Read the json config file.

        Raises:
            Exception: The exception is thrown in case no valid json has been found.
        """
        data = None
        with open(self.filename, 'r') as file:
            data = json.load(file)
        
        if data == None:
            raise Exception("No json config found")
        
        self.__parse_config(data)

    def __parse_config(self, data):
        """
        Parse the json data into the internal data structure.

        Args:
            data (dict): The data from the input json file.
        """
        if self.WD_NAME in data:
            self.wd = data[self.WD_NAME]
        if self.DATA in data:
            if self.DATA_COLUMNS in data[self.DATA]:
                self.data_columns = pd.core.indexes.base.Index(data[self.DATA][self.DATA_COLUMNS])
            if self.DB_COLUMNS in data[self.DATA]:
                self.db_columns = pd.core.indexes.base.Index(data[self.DATA][self.DB_COLUMNS])
        if self.SEPARATOR in data:
            self.separator = data[self.SEPARATOR]
        if self.DB_NAME in data:
            self.db_name = data[self.DB_NAME]
        if self.DB_TYPES in data:
            self.db_types = data[self.DB_TYPES]
        else:
            raise Exception("No db types found!")
        if self.DB_TABLES in data:
            for table_key in data[self.DB_TABLES].keys():
                self.__generate_dbtable(table_key, data[self.DB_TABLES][table_key])
                table_data_keys = [i for i in data if table_key in i and self.DATA in i]
                if len(table_data_keys) > 0:
                    meta_data = pd.DataFrame(columns = self.tables[table_key].data_columns)
                    for table_data_key in table_data_keys:
                        for meta_data_values in data[table_data_key].values():
                            meta_data_entry = [meta_data_values[i] if i in meta_data_values else False for i in meta_data.columns]
                            if False not in meta_data_entry:
                                meta_data = pd.concat([meta_data, pd.DataFrame([meta_data_entry], columns = meta_data.columns)], ignore_index = True)
                    if len(meta_data) > 0:
                        self.meta_data[table_key] = meta_data
        if self.TRACKER_NAMES in data.keys():
            self.tracker_names = data[self.TRACKER_NAMES]
    
    def __generate_dbtable(self, table_name: str, table: dict):
        """
        Generate the DBTable object for the given table.

        Args:
            table_name (str): The name of the table.
            table (dict): The input of the table parsed from json.

        Raises:
            Exception: The exception is raised in case the json of the table is invalid.
        """
        if (len(table.keys()) > 3):
            raise Exception("Invalid number of entries for table %s found: %s"%(table_name, str(table.keys())))
        invalid_keys = [key for key in table.keys() if key not in [self.DB_TABLE_COLUMNS, self.DB_TABLE_PRIMARY_KEY, self.TABLE_NAME]]
        if len(invalid_keys) != 0:
            raise Exception("%s is undefined for table %s"%(str(invalid_keys), table_name))
        if self.TABLE_NAME not in table.keys():
            raise Exception("No table name provided for table %s(table_name)")
        if self.DB_TABLE_COLUMNS not in table.keys():
            raise Exception("No columns defined for table %s"%(table_name))
        columns = pd.core.indexes.base.Index(table[self.DB_TABLE_COLUMNS])
        data_types = []
        for column in columns:
            if column in self.db_types.keys():
                data_types.append(self.db_types[column])
            else:
                raise Exception("No data type found for column %s"%(column))
        self.tables[table_name] = DBTable(table[self.TABLE_NAME], columns, data_types, table[self.DB_TABLE_PRIMARY_KEY] if self.DB_TABLE_PRIMARY_KEY in table.keys() else [])

