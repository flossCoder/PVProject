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

library(DBI)
library(RSQLite)
library(R6)

#' Definition of a Sqlite database connector.
SqliteConnector <- R6Class(
  classname = "SqliteConnector",
  private = list(
    wd = "",
    dbName = "",
    con = NULL,
    #' Test, if the table exists.
    #' 
    #' @param tableName The name of the given table.
    #' 
    #' @return true, if the given table exists, false otherwise.
    tableExists = function(tableName) {
      return(tableName %in%  dbListTables(private$con))
    },
    #' Get the meta data of the given table.
    #' 
    #' @param tableName The name if the given table.
    #' 
    #' @return The meta data of the given table.
    getTableMetaData = function(tableName) {
      return(dbGetQuery(private$con, paste("PRAGMA table_info('", tableName, "')", sep="")))
    },
    castColumnData = function(columnData, metaData) {
      if (!("name" %in% names(metaData))) {
        stop("'name' is not in input 'metaData'")
      }
      if (!("type" %in% names(metaData))) {
        stop("'type' is not in input 'metaData'")
      }
      if (sum(metaData["name"] == names(columnData)[1]) != 1) {
        stop(paste("The name of the column ", names(columnData)[1], "is not in the name column of the input metaData", sep=""))
      }
      return(
        switch(metaData$type[metaData["name"] == names(columnData)[1]],
          "DATE" = {
            result <- data.frame(as.POSIXct(as.matrix(columnData), format="%Y-%m-%d %H:%M"))
            colnames(result) <- names(columnData)
            result
          },
          "TEXT" = columnData,
          "REAL" = {
            result <- data.frame(as.numeric(as.matrix(columnData)))
            colnames(result) <- names(columnData)
            result
          },
          "INT" = {
            result <- data.frame(as.numeric(as.matrix(columnData)))
            colnames(result) <- names(columnData)
            result
          }
        )
      )
    },
    castDataFrame = function(data, metaData) {
      return(
          do.call(cbind, lapply(names(data), function(colname) {
               single_col_df <- data[, colname, drop = FALSE]
               return(private$castColumnData(single_col_df, metaData))
             }
           )
         )
      )
    }
  ),
  public = list(
    #' Initialize the connector.
    #'
    #' @param wd The working directory, where the db shall be saved.
    #' @param dbName The name of the database file.
    initialize = function(wd, dbName) {
      private$wd <- wd
      private$dbName <- dbName
      self$connect()
    },
    #' Connect the database.
    connect = function() {
      private$con <- dbConnect(RSQLite::SQLite(), dbname = file.path(private$wd, private$dbName))
    },
    #' Disconnect the database.
    disconnect = function() {
      dbDisconnect(private$con)
      private$con <- NULL
    },
    #' Get the data of the table.
    #' 
    #' @param tableName The name of the table.
    #' 
    #' @return The data of the table, or NULL, if the table does not exist.
    getData = function(tableName) {
      if (private$tableExists(tableName)) {
        metaData <- private$getTableMetaData(tableName)
        return(private$castDataFrame(dbReadTable(private$con, tableName), metaData))
      }
      return(NULL)
    }
  )
)
