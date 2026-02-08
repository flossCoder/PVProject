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
library(suncalc)

#' The class metadata defines the properties of the different solar fields.
Metadata <- R6Class(
  classname = "Metadata",
  private = list(
    trackerName = "",
    direction = 0,
    suncalcDirection = 0,
    suncalcDirectionRad = 0,
    inclinationAngle = 0,
    inclinationAngleRad = 0,
    latitude = 0,
    longitude = 0,
    solarPanelWidth = 0,
    solarPanelHeight = 0,
    solarPanelEnergyConversionEfficiency = 0,
    ratedPower = 0,
    solarPanelNumber = 0,
    solarPanelArea = 0,
    #' Calculate the solar panel area.
    calculateSolarPanelArea = function() {
      private$solarPanelArea <- private$solarPanelWidth * private$solarPanelHeight * private$solarPanelNumber
    },
    #' Convert the direction into the suncalc format, measuring from south to the direction west (positive) and east (negative).
    #' Therefor the suncalcDirection is in [-180 °, 180 °].
    calculateSuncalcDirection = function() {
      private$suncalcDirection <- private$direction - 180
      if ((private$suncalcDirection < -180) | (private$suncalcDirection > 180)) {
        private$suncalcDirection <- (private$suncalcDirection + 180) %% 360 - 180
      }
      private$suncalcDirectionRad <- private$suncalcDirection * pi / 180
    },
    #' Convert the inclination angle into rad.
    calculateInclinationAngle = function() {
      private$inclinationAngleRad <- private$inclinationAngle * pi / 180
    },
    #' Calculate the influence of the sun position on the solar field.
    #' 
    #' @param currentTime The timestamp to be processed.
    #' 
    #' @return The correction factor for the solar field.
    calculateSunFactor = function(currentTime) {
      pos <- getSunlightPosition(
        date = as.POSIXct(currentTime, tz = "CET"),
        lat = private$latitude,
        lon = private$longitude
      )
      #sunElevation <- pos$altitude * 180 / pi
      #sunAzimuthe <- pos$azimuth * 180 / pi + 180
      cosTheta <- sin(pos$altitude) * sin(private$inclinationAngleRad) + cos(pos$altitude) * cos(private$inclinationAngleRad) * cos(pos$azimuth - private$suncalcDirectionRad)
      if (length(cosTheta) > 1) {
        cosTheta[cosTheta < 0] <- 0
        return(cosTheta)
      }
      if (cosTheta < 0) {
        return(0)
      }
      return(cosTheta)
    },
    #' Calculate the influence of the sun position on the solar field per panel number.
    #' 
    #' @param currentTime The timestamp to be processed.
    #' 
    #' @return The correction factor for the solar field per panel number.
    calculateSunFactorPerPanelNumber = function(currentTime) {
      return(private$calculateSunFactor(currentTime) / private$solarPanelNumber)
    },
    #' Calculate the influence of the sun position on the solar field per square meter and number of panels.
    #' 
    #' @param currentTime The timestamp to be processed.
    #' 
    #' @return The correction factor for the solar field per square meter and number of panels.
    calculateSunFactorPerPanelNumberSquareMeter = function(currentTime) {
      return(private$calculateSunFactorPerPanelNumber(currentTime) / private$solarPanelAreas)
    }
  ),
  public = list(
    #' Initialize the meta data.
    #' 
    #' @param trackerMetaData The input tracker meta data.
    initialize = function(trackerMetaData) {
      if (dim(trackerMetaData)[1] != 1) {
        stop("Invalid number of rows for meta data input")
      }
      trackerNames <- list("tracker_name", "direction", "inclination_angle", "latitude", "longitude", "solar_panel_width", "solar_panel_height", "solar_panel_energy_conversion_efficiency", "solar_panel_number")
      if (length(trackerNames) != length(names(trackerMetaData))) {
        stop("Invalid meta data input")
      }
      for (name in names(trackerMetaData)) {
        if (!any(name == trackerNames)) {
          stop("Invalid meta data input")
        }
      }
      private$trackerName = trackerMetaData[1, "tracker_name"]
      private$direction = trackerMetaData[1, "direction"]
      private$inclinationAngle = trackerMetaData[1, "inclination_angle"]
      private$latitude = trackerMetaData[1, "latitude"]
      private$longitude = trackerMetaData[1, "longitude"]
      private$solarPanelWidth = trackerMetaData[1, "solar_panel_width"] / 1000
      private$solarPanelHeight = trackerMetaData[1, "solar_panel_height"] / 1000
      private$solarPanelEnergyConversionEfficiency = trackerMetaData[1, "solar_panel_energy_conversion_efficiency"]
      private$ratedPower = trackerMetaData[1, "rated_power"]
      private$solarPanelNumber = trackerMetaData[1, "solar_panel_number"]
      private$calculateSolarPanelArea()
      private$calculateSuncalcDirection()
      private$calculateInclinationAngle()
    },
    #' Get the sun factor for the given time vector.
    #' 
    #' @param currentTime The time vector for calculating the sun factor.
    #' 
    #' @return The sun factor vector for the current time vector.
    getSunFactor = function(currentTime) {
      return(private$calculateSunFactor(currentTime))
    },
    #' Get the name of the tracker.
    #' 
    #' @return The name of the tracker.
    getTrackerName = function() {
      return(private$trackerName)
    },
    #' Calculate the result for the input data.
    #' 
    #' @param data The input data.
    #' 
    #' @returns The result of the input data.
    calculateResult = function(data) {
      if (!any(data$tracker_name == private$trackerName)) {
        stop("The meta data instance is not in the data!")
      }
      this_data <- data[data$tracker_name == private$trackerName, ]
      result <- this_data#[1:(length(this_data) - 1)]
      result[paste(names(this_data)[length(this_data)], "per_panel", sep="_")] <- this_data[,3] / private$solarPanelNumber
      result[paste(names(this_data)[length(this_data)], "per_square_meter", sep="_")] <- this_data[,3] / (private$solarPanelNumber * private$solarPanelArea)
      sun_factor <- private$calculateSunFactor(this_data$timestamp)
      result[paste(names(this_data)[length(this_data)], "per_panel_sun_factor", sep="_")] <- this_data[,3] / private$solarPanelNumber * sun_factor
      result[paste(names(this_data)[length(this_data)], "per_square_meter_sun_factor", sep="_")] <- this_data[,3] / (private$solarPanelNumber * private$solarPanelArea) * sun_factor
      result["sun_factor"] <- sun_factor
      return(result)
    }
  )
)
