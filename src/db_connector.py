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

import sqlite3
import os
import pandas as pd
import re
import numpy as np

class DBTable:
    """
    Definition of a database table.
    """
    def __init__(self, table_name: str, data_columns: pd.core.indexes.base.Index, data_types: list[str], primary_key_list: list[str] = []):
        """
        Initialize the database table.

        Args:
            table_name (str): The name of the table.
            data_columns (pd.core.indexes.base.Index): The column names of the table.
            data_types (list[str]): The data types of the data columns in the data base.
            primary_key_list (list[str], optional): A list of primary keys, is empty, if the table has none. Defaults to [].

        Raises:
            Exception: The exception is raised in case the number of data columns is not equals to the numer of data types or the primary key is invalid.
        """
        self.table_name = table_name
        self.data_columns = data_columns
        self.data_types = data_types
        self.primary_key_list = primary_key_list
        if len(data_columns) != len(data_types):
            raise Exception("Invalid data_columns = %s and data_types = %s given!"%(str(data_columns.tolist()), str(data_types)))
        not_in_data_columns = [i for i in primary_key_list if i not in data_columns]
        if len(not_in_data_columns) != 0:
            raise Exception("The following primary keys are not in the data_columns defined: %s"%(str(not_in_data_columns)))
    
    def get_column_dict(self) -> dict:
        """
        Map the column names to the data types in the database.

        Returns:
            dict: A dictionary mapping the column names to their data types in the data base.
        """
        column_data_type = {}
        for i in range(len(self.data_columns)):
            column_data_type[self.data_columns[i]] = self.data_types[i]
        return column_data_type

class DBConnector:
    """
    The DBConnector defines a connection to a sqlite DB.
    """
    PRIMARY_KEY = "PRIMARY KEY"

    def __init__(self, wd: str, db_name: str):
        """
        Initialize the DBConnector

        Args:
            wd (str): The path to the database.
            db_name (str): The name of the database.
        """
        self.wd = wd
        self.db_name = db_name
        self.db_fullpath = os.path.join(wd, db_name)
    
    def create_table(self, table: DBTable, data: pd.core.frame.DataFrame):
        """
        Create an empty table and insert the data.

        Args:
            table (DBTable): The DBTable object of the table.
            data (pd.core.frame.DataFrame): The input data frame.
        """
        with self.ConnectorContextManager(self.db_fullpath) as ccm:
            cur = ccm.get_cursor()
            if not self._test_table_exists(cur, table.table_name):
                self._create_table(cur, table)
            self._insert_table_rows(cur, table, data)
            ccm.commit()

    def create_table(self, table: DBTable):
        """
        Create an empty table.

        Args:
            table (DBTable): The DBTable object of the table.
        """
        with self.ConnectorContextManager(self.db_fullpath) as ccm:
            cur = ccm.get_cursor()
            if not self._test_table_exists(cur, table.table_name):
                self._create_table(cur, table)
                ccm.commit()

    def create_index(self, index_name: str, table_name: str, column_list: list[str]):
        """
        Function for creating indexes.

        Args:
            index_name (str): The name of the index.
            table_name (str): The name of the table.
            column_list (list[str]): The list of columns to include in the index.
        
        Raises:
            Exception: The exception is raised, if the table does not exist or invalid columns are given.
        """
        with self.ConnectorContextManager(self.db_fullpath) as ccm:
            self._create_index(ccm.get_cursor(), index_name, table_name, column_list)
            ccm.commit()

    def insert_data(self, table: DBTable, data: pd.core.frame.DataFrame):
        """
        Insert the data into the table.

        Args:
            table (DBTable): The DBTable object of the table.
            data (pd.core.frame.DataFrame): The input data frame.
        """
        with self.ConnectorContextManager(self.db_fullpath) as ccm:
            self._insert_table_rows(ccm.get_cursor(), table, data)
            ccm.commit()
    
    def select_data_unfiltered(self, table_name: str, select_columns: list[str] = [], order_by: dict[str, str] = {}) -> pd.core.frame.DataFrame:
        """
        Internal function for selecting data.

        Args:
            cur (sqlite3.Cursor): The Cursor object of the database.
            table_name (str): The input table name.
            select_columns (list[str], optional): The columns of the table to select. Defaults to [].
            order_by (dict[str, str], optional): The order by of the columns. Defaults to {}.

        Returns:
            pd.core.frame.DataFrame: The resulting data.
        """
        with self.ConnectorContextManager(self.db_fullpath) as ccm:
            return self._select_data_unfiltered(ccm.get_cursor(), table_name, select_columns, order_by)

    def test_table_exists(self, table_name: str) -> bool:
        """
        Test, if the input table name exists in the database.

        Args:
            table_name (str): The input table name.
        
        Returns:
            bool: True, if the given table exists in the database, False otherwise.
        """
        with self.ConnectorContextManager(self.db_fullpath) as ccm:
            return self._test_table_exists(ccm.get_cursor(), table_name)

    def _test_table_exists(self, cur: sqlite3.Cursor, table_name: str) -> bool:
        """
        Test, if the input table name exists in the given database.

        Args:
            cur (sqlite3.Cursor): The Cursor object of the database.
            table_name (str): The input table name.
        
        Returns:
            bool: True, if the given table exists in the input database, False otherwise.
        """
        table_list = cur.execute("""SELECT name FROM sqlite_master WHERE type='table' AND name='%s';"""%table_name).fetchall()
        return table_list != []
    
    def _create_table(self, cur: sqlite3.Cursor, table: DBTable):
        """
        Internal function to create a table in the database.

        Args:
            cur (sqlite3.Cursor): The Cursor object of the database.
            table (DBTable): The DBTable object of the table.
        
        Raises:
            Exception: The exception is raised in case no column data exist.
        """
        column_data_type = table.get_column_dict()
        if len(column_data_type) == 0:
            raise Exception("Column data found!")
        column_statement = ", ".join(["%s %s"%(key, column_data_type[key]) for key in column_data_type.keys()])
        if len(table.primary_key_list) != 0:
            column_statement += ", %s (%s)"%(self.PRIMARY_KEY, ", ".join([i for i in table.primary_key_list]))
        create_statement = """CREATE TABLE %s(%s);"""%(table.table_name, column_statement)
        cur.execute(create_statement)

    def _create_index(self, cur: sqlite3.Cursor, index_name: str, table_name: str, column_list: list[str]):
        """
        Internal function for creating indexes.

        Args:
            cur (sqlite3.Cursor): The Cursor object of the database.
            index_name (str): The name of the index.
            table_name (str): The name of the table.
            column_list (list[str]): The list of columns to include in the index.
        
        Raises:
            Exception: The exception is raised, if the table does not exist or invalid columns are given.
        """
        if not(self._test_table_exists(cur, table_name)):
            raise Exception("The table %s does not exist!"%(table_name))
        if len(column_list) != 0:
            table_columns = self._get_table_column_names(cur, table_name)
            missing_columns = [i for i in column_list if i not in table_columns]
            if len(missing_columns) != 0:
                raise Exception("The input columns %s do not exist in table %s!"%(", ".join(missing_columns), table_name))
            if not self._test_index_exists(cur, index_name, table_name):
                cur.execute("""CREATE INDEX %s ON %s(%s);"""%(index_name, table_name,", ".join(column_list)))
        else:
            raise Exception("Missing columns for creating an index!")

    def _test_index_exists(self, cur: sqlite3.Cursor, index_name: str, table_name: str) -> bool:
        """
        Test, if the index exists.
        
        Args:
            cur (sqlite3.Cursor): The Cursor object of the database.
            index_name (str): The name of the index.
            table_name (str): The name of the table.
        
        Returns:
            bool: True, if the index exists, false otherwise.
        """
        return len(cur.execute("""SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='%s' AND name='%s';"""%(table_name, index_name)).fetchall()) != 0
    
    def _insert_table_rows(self, cur: sqlite3.Cursor, table: DBTable, data: pd.core.frame.DataFrame):
        """
        Insert the data into the table of the database.

        Args:
            cur (sqlite3.Cursor): The Cursor object of the database.
            table (DBTable): The DBTable object of the table.
            data (pd.core.frame.DataFrame): The input data frame.

        Raises:
            Exception: The exception is raised in case the data could not be inserted due to an internal exception.
        """
        if (len(table.data_columns) != len(data.columns)):
            raise Exception("The number of columns %i in table %s is different from the number of columns in the data %i"%(len(table.data_columns), table.table_name, len(data.columns)))
        insert_statement = self._prepare_insert_column_statement(cur, table)
        data_statement = self._prepare_data_statement(cur, table, data)
        if data_statement == None:
            return
        execute_statement = """%s\n%s"""%(insert_statement, data_statement)
        cur.execute(execute_statement)

    def _prepare_insert_column_statement(self, cur: sqlite3.Cursor, table: DBTable) -> str:
        """
        This function generates the insert into table with columns statements.

        Args:
            cur (sqlite3.Cursor): The Cursor object of the database.
            table (DBTable): The DBTable object of the table.

        Raises:
            Exception: The exception is raised in case the table does not exist or invalid columns are given.

        Returns:
            str: The insert statement.
        """
        if not self._test_table_exists(cur, table.table_name):
            raise Exception("The table %s does not exist!"%(table.table_name))
        column_matches = self._get_column_raw_data(cur, table.table_name)
        table_column_names = self._get_table_column_names(cur, table.table_name, column_matches, False)
        table_primary_column_names = self._get_table_column_names(cur, table.table_name, column_matches, True)
        if not all([i in table_column_names for i in table.data_columns]):
            raise Exception("Invalid columns %s are given for table %s!"%(str(table.data_columns), table.table_name))
        if not all([i in table.data_columns for i in table_primary_column_names]):
            raise Exception("The primary keys %s are required for the table %s!"%(" ".join(table_primary_column_names), table.table_name))
        insert_statement = "INSERT INTO %s (%s)"%(table.table_name, ", ".join(table.data_columns))
        return insert_statement
    
    def _prepare_data_statement(self, cur: sqlite3.Cursor, table: DBTable, data: pd.core.frame.DataFrame) -> str:
        """
        Prepare the data statement for the given data.

        Args:
            cur (sqlite3.Cursor): The Cursor object of the database.
            table (DBTable): The DBTable object of the table.
            data (pd.core.frame.DataFrame): The input data frame.

        Raises:
            Exception: The exception is raised in case no data are available.

        Returns:
            str: The data part of the sql statement for the given data.
        """
        values = data.values
        if (len(values) == 0):
            raise Exception("There should be data available!")
        reduced_data = self._reduce_data(cur, table, data)
        values = reduced_data.values
        if (len(values) == 0):
            return None
        data_statement = "VALUES\n" + ",\n".join(["  (%s)"%(k) for k in [", ".join(["'%s'"%(str(j)) for j in i]) for i in values]]) + ";"
        return data_statement

    def _reduce_data(self, cur: sqlite3.Cursor, table: DBTable, data: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
        """
        Remove all data, that exist in the data base.

        Args:
            cur (sqlite3.Cursor): The Cursor object of the database.
            table (DBTable): The DBTable object of the table.
            data (pd.core.frame.DataFrame): The input data frame.

        Returns:
            pd.core.frame.DataFrame: The reduced data frame.

        """
        if len(table.primary_key_list) == 0:
            raise Exception("No primary key exists for table %s!"%(table.table_name))
        select_statement = "SELECT " + ", ".join(table.primary_key_list)
        result = cur.execute("""%s\nFROM %s"""%(select_statement, table.table_name)).fetchall()
        pd_result = pd.core.frame.DataFrame(result, columns = table.primary_key_list)
        merged = data.merge(pd_result, on=table.primary_key_list, how="left", indicator=True)
        result_indices = merged[merged["_merge"] == "left_only"].index
        reduced_data = data.loc[result_indices]
        return reduced_data

    def _get_column_raw_data(self, cur: sqlite3.Cursor, table_name: str) -> list[str]:
        """
        Obtain the raw column data.

        Args:
            cur (sqlite3.Cursor): The Cursor object of the database.
            table_name (str): The input table name.
        
        Returns:
            list[str] : The raw column data of the table.
        
        Raises:
            Exception: The exception is raised in case the table data could not be found.
        """
        statement = cur.execute("""SELECT sql FROM sqlite_master WHERE type='table' AND name='%s';"""%(table_name)).fetchall()[0][0]
        pattern = r"CREATE TABLE %s\((.*)\)"%(table_name)
        matches = re.findall(pattern, statement)
        if (len(matches) != 1):
            raise Exception("Invalid data columns found!")
        split_pattern = r",\s+(?![^()]*\))"
        column_matches: list[str] = re.split(split_pattern, matches[0])
        return column_matches

    def _get_table_column_names(self, cur: sqlite3.Cursor, table_name: str, column_matches: list[str] = None, only_primary_columns: bool = False) -> pd.core.indexes.base.Index:
        """
        Obtain the column names of the given table.

        Args:
            cur (sqlite3.Cursor): The Cursor object of the database.
            table_name (str): The input table name.
            column_matches (list[str], optional): The raw column data of the table. Defaults to None (in this case the raw data will be retrieved).
            only_primary_columns (bool, optional): Return only primary columns. Defaults to False.
        
        Returns:
            pd.core.indexes.base.Index : column names of the table.
        
        Raises:
            Exception: The exception is raised in case the table data could not be found.
        """
        if column_matches == None:
            column_matches = self._get_column_raw_data(cur, table_name)
        column_names = []
        primary_key_matcher = re.compile(r"%s \(([\w\s,]+)\)"%(self.PRIMARY_KEY))
        for column in column_matches:
            match = primary_key_matcher.search(column)
            if match:
                column_names += match.group(1).split(", ")
            else:
                column_elements = column.split(" ")
                if len(column_elements) >= 2 and ((self.PRIMARY_KEY in column) or (not only_primary_columns)) and column_elements[0] not in column_names:
                    column_names.append(column_elements[0])
        return pd.core.indexes.base.Index(column_names)
    
    def _select_data_unfiltered(self, cur: sqlite3.Cursor, table_name: str, select_columns: list[str] = [], order_by: dict[str, str] = {}) -> pd.core.frame.DataFrame:
        """
        Internal function for selecting data.

        Args:
            cur (sqlite3.Cursor): The Cursor object of the database.
            table_name (str): The input table name.
            select_columns (list[str], optional): The columns of the table to select. Defaults to [].
            order_by (dict[str, str], optional): The order by of the columns. Defaults to {}.

        Returns:
            pd.core.frame.DataFrame: The resulting data.
        """
        select_statement = "SELECT " + ("* " if len(select_columns) == 0 else ", ".join(select_columns)) + "FROM " + table_name + (" ORDER BY " + ", ".join(key + " " + value for key, value in order_by.items()) if len(order_by) != 0 else "")
        result = cur.execute(select_statement).fetchall()
        
        cols = self._get_table_column_names(cur, table_name)
        p_cols = self._get_table_column_names(cur, table_name, None, True)
        np_cols = cols if len(p_cols) == 0 else cols[:-len(p_cols)]
        columns = pd.core.indexes.base.Index(select_columns) if len(select_columns) != 0 else np_cols
        pd_result = pd.core.frame.DataFrame(result, columns = columns)
        return pd_result
    
    class ConnectorContextManager:
        """
        The ConnectorContextManager is used to handle the cursor and connection to the database in a with clause.
        """
        def __init__(self, db_fullpath):
            """
            Initialize the ConnectorContextManager.

            Args:
                db_fullpath (str): The full path to the database.
            """
            self.db_fullpath = db_fullpath
            self.conn = None
            self.cur = None

        def __enter__(self):
            """
            The enter function is used to start the connection in the with statement and return the as-value.

            Returns:
                ConnectorContextManager: The as-return value.
            """
            if (self.conn == None):
                self.conn = sqlite3.connect(self.db_fullpath)
                self.cur = None
            return self
        
        def __exit__(self, exc_type, exc_value, traceback) -> bool:
            """
            The exit function is used at the end of the with statement.

            Args:
                exc_type (Type[BaseException], optional): The exception type, if any, None, if no exception ocurred. Defaults to None.
                exc_value (BaseException, optional): The exception value, if any, None, if no exception ocurred. Defaults to None.
                traceback (TracebackType, optional): The stacktrace of the exception, if any, None, if no exception ocurred. Defaults to None.
            
            Returns:
                bool: True, if the connection has been established, False otherwise.
            
            Raises:
                Exception: The exception is raised in case something went wrong before calling __exit__.
            """
            result = False
            if (self.conn != None):
                self.conn.close()
                self.conn = None
                self.cur = None
                result = True
            if exc_type != None or exc_value != None:
                raise exc_value
            return result

        def get_cursor(self) -> sqlite3.Cursor:
            """
            Obtain the cursor of the connection.

            Raises:
                Exception: The exception is raised in case no cursor could be obtained.

            Returns:
                sqlite3.Cursor: The cursor object of the connection.
            """
            if self.cur == None:
                if self.conn == None:
                    raise Exception("No connection found!")
                else:
                    self.cur = self.conn.cursor()
                    if self.cur == None:
                        raise Exception("No cursor could be obtained!")
            return self.cur

        def commit(self):
            """
            Commit the changes to the database.

            Raises:
                Exception: The exception is raised in case no connection has been established.
            """
            if self.conn == None:
                raise Exception("No connection found!")
            self.conn.commit()
