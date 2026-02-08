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

library(this.path)
source(file.path(this.dir(), "dbConnector.R"))
source(file.path(this.dir(), "metadata.R"))
source(file.path(this.dir(), "fieldComparator.R"))

Main <- R6Class(
  classname = "Main",
  public = list(
    con = NULL,
    mainRaw = NULL,
    trackerRaw = NULL,
    trackerMeta = NULL,
    fieldComparator = NULL,
    #' Initialize the connector.
    #'
    #' @param wd The working directory, where the db shall be saved.
    #' @param dbName The name of the database file.
    initialize = function(wd, dbName) {
      self$con <- SqliteConnector$new(wd, dbName)
      self$mainRaw <- self$con$getData("main_raw")
      self$trackerRaw <- self$con$getData("tracker_raw")
      self$trackerMeta <- self$con$getData("tracker_meta")
      self$con$disconnect()
      self$fieldComparator <- FieldComparator$new(self$trackerMeta, self$trackerRaw)
    }
  )
)
