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

library(R6)

#' The class FieldComparator is responsible for evaluating the solar data of the different solar panels.
FieldComparator <- R6Class(
  classname = "FieldComparator",
  private = list(
    fieldData = NULL
  ),
  public = list(
    #' Initialize the field comparator.
    #' 
    #' @param trackerMetaData The meta data of all fields.
    #' @param trackerRaw The raw data of all fields.
    initialize = function(trackerMetaData, data) {
      private$fieldData <- buildFieldData(trackerMetaData, data)
      if (is.null(private$fieldData)) {
        stop("No field data generated!")
      }
    },
    getData = function() {
      return(private$fieldData)
    },
    #' Get the resulting data.
    #' 
    #' @return The aggregated result.
    getAggregatedData = function() {
      type <- "Solarproduktion_per_square_meter"
      time <- private$fieldData[[1]]$data$timestamp
      days <- unique(as.Date(time))
      result <- as.data.frame(days)
      colnames(result) <- "time"
      sumData <- c(1:length(days)) * 0
      for (elem in private$fieldData) {
        if (!any(type == names(elem$data))) {
          stop(paste("Column", type, "not found for", elem$metadata$getTrackerName()))
        }
        if (any(elem$timestamp != time)) {
          stop(paste("Timestamp does not fit for", elem$metadata$getTrackerName()))
        }
        current <- c(1:length(days)) * 0
        for (i in 1:length(days)) {
          currentIndex <- as.Date(time) == days[i]
          current[i] <- sum(elem$data[type][currentIndex,])
          sumData[i] <- sumData[i] + current[i]
        }
        name <- paste(elem$metadata$getTrackerName(), type, sep="_")
        name <- gsub("\\. ", "_", name)
        name <- gsub(" ", "_", name)
        name <- gsub("\\.", "_", name)
        result[name] <- current
      }
      for (i in 1:length(days)) {
        if (sumData[i] != 0) {
          result[i,c(2:dim(result)[2])] <- result[i,c(2:dim(result)[2])] / sumData[i]
        }
      }
      return(result)
    }
  )
)

#' The class FieldData serves as a data container for a solar field.
FieldData <- R6Class(
  classname = "FieldData",
  public = list(
    metadata = NULL,
    data = NULL,
    #' Initialize the field data.
    #' 
    #' @param trackerMetaData The meta data of the field.
    #' @param trackerRaw The raw data of all fields.
    initialize = function(trackerMetaData, trackerRaw) {
      self$metadata <- Metadata$new(trackerMetaData)
      self$data <- self$metadata$calculateResult(trackerRaw)
      if (is.null(self$data)) {
        stop("No result calculated!")
      }
    }
  )
)

#' Builder for the field data.
#' 
#' @param trackerMetaData The meta data of all fields.
#' @param trackerRaw The raw data of all fields.
#' 
#' @return A list containing a FieldData object for each tracker.
buildFieldData <- function(trackerMetaData, trackerRaw) {
  result <- c()
  for (i in seq_len(nrow(trackerMetaData))) {
    result <- append(result, FieldData$new(trackerMetaData[i,], trackerRaw))
  }
  return(result)
}
