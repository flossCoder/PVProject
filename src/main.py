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

from config import Config
from db_connector import DBConnector, DBTable
from read_pv_csv import search_csv_files

import pandas as pd

class Main:
    """
    The main class
    """
    TRACKER_KEY = "tracker_name"
    META_KEY = "meta"

    def __init__(self, config_path: str):
        """
        Initialize the main class.

        Args:
            config_path (str): The full path to the config file.
        """
        self.config = Config(config_path)
        self.db_connector = DBConnector(self.config.wd, self.config.db_name)
    
    def create_tables(self):
        """
        Setup the database tables according to the config file.
        """
        for table_name in self.config.tables.keys():
            table = self.config.tables[table_name]
            self.db_connector.create_table(table)
            if len(table.primary_key_list) != 0:
                self.db_connector.create_index("idx_" + table.table_name, table.table_name, table.primary_key_list)
            if table_name in self.config.meta_data.keys():
                self._insert_table_data(table, self.config.meta_data[table_name])
    
    def insert_raw_data(self):
        """
        Insert the raw input data into the database.

        Raises:
            Exception: The exception is raised, in case the insertion of the raw data failed.
        """
        data = search_csv_files(self.config.wd, self.config.separator)
        # check, if the columns match to the config
        if not self.config.data_columns.equals(data.columns):
            raise Exception("The data columns of the config %s does not match the actual data columns %s!"%(
                ", ".join(self.config.data_columns.to_list()),
                ", ".join(data.data_columns.to_list())
            ))
        # fill the tables
        for table_name in self.config.tables.keys():
            table = self.config.tables[table_name]
            table_data = pd.DataFrame()
            # check, if all columns of the table are in the data => insert
            if all([col in self.config.db_columns for col in table.data_columns]):
                table_data = data[[self.config.get_data_column_name(col) for col in table.data_columns]]
            elif self.TRACKER_KEY in table.data_columns and self.META_KEY not in table.table_name:
                if (len(table.primary_key_list) + 1) != len(table.data_columns):
                    raise Exception("Invalid tracker table " + table.table_name)
                data_column_name = [i for i in table.data_columns if i not in table.primary_key_list][0]
                for tracker_name in self.config.tracker_names:
                    tracker_table_data = pd.DataFrame()
                    for p_col in table.primary_key_list:
                        if p_col != self.TRACKER_KEY:
                            x = data[[self.config.get_data_column_name(p_col)]]
                            tracker_table_data = pd.concat([tracker_table_data, x], axis = 1)
                        else:
                            x = pd.DataFrame(data.shape[0] * [self.config.get_data_column_name(tracker_name)], columns = [p_col])
                            tracker_table_data = pd.concat([tracker_table_data, x], axis = 1)
                    # Map the tracker data
                    data_name = self.config.get_data_column_name(tracker_name)
                    x = data[[data_name]]
                    x.columns = [data_column_name]
                    tracker_table_data = pd.concat([tracker_table_data, x], axis = 1)
                    table_data = pd.concat([table_data, tracker_table_data], ignore_index = True)
            else:
                break
            # save table
            table_data.columns = [col if col in table.data_columns else self.config.get_db_column_name(col) for col in table_data.columns]
            self._insert_table_data(table, table_data)

    def _insert_table_data(self, table: DBTable, data: pd.core.frame.DataFrame):
        """
        Insert the data into the table. If the table does not exist, it is created.

        Args:
            table (DBTable): The DBTable object of the table.
            data (pd.core.frame.DataFrame): The input data frame.
        """
        self._check_table_exists(table.table_name)
        self.db_connector.insert_data(table, data)

    def _check_table_exists(self, table_name: str):
        """
        Check, if the given table_name exists in the database, if not create it.

        Args:
            table_name (str): The input table name.
        """
        if not self.db_connector.test_table_exists(table_name):
            self.db_connector.create_table(self.config[table_name])
