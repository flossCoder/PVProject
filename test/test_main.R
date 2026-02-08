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

library(testthat)
library(here)

setup <- function() {
  here::i_am("test/test_main.R") 
  source(here("src", "main.R"))
  testDataPath <- sub("/test", "", here("test_resources", "test_data"))
  main <- Main$new(testDataPath, "pvdb_test.db")
  return(main)
}

expectedTrackerMeta <- read.table(text = "
1_1 59 53 52.37352 7.1011 1755 1038 19.9 10
1_2 40 52 52.37352 7.1011 1755 1038 19.9 10
1_3 61 54 52.37352 7.1011 1755 1038 19.9 10
", header = FALSE)
expectedTrackerMeta[, 1] <- as.character(expectedTrackerMeta[, 1])
colnames(expectedTrackerMeta) <- c("tracker_name", "direction", "inclination_angle", "latitude", "longitude", "solar_panel_width", "solar_panel_height", "solar_panel_energy_conversion_efficiency", "solar_panel_number")

expectedMainRaw_input <- "
2023-03-02 16:00 6 7
2023-03-02 16:15 6 7
2023-03-02 16:30 6 7
2023-03-02 16:45 6 7
2023-03-02 17:00 6 7
2023-03-02 17:15 6 7
2023-04-02 16:00 6 7
2023-04-02 16:15 6 7
2023-04-02 16:30 6 7
2023-04-02 16:45 6 7
2023-04-02 17:00 6 7
2023-04-02 17:15 6 7
"

expectedMainRaw_raw <- read.table(text = trimws(expectedMainRaw_input), header = FALSE)

expectedMainRaw <- data.frame(
  timestamp = as.POSIXct(paste(expectedMainRaw_raw$V1, expectedMainRaw_raw$V2), format="%Y-%m-%d %H:%M"),
  Production = as.numeric(expectedMainRaw_raw$V3),
  Consumption = as.numeric(expectedMainRaw_raw$V4)
)


expectedMainRaw_input <- "
2023-03-02 16:00 6 7
2023-03-02 16:15 6 7
2023-03-02 16:30 6 7
2023-03-02 16:45 6 7
2023-03-02 17:00 6 7
2023-03-02 17:15 6 7
2023-04-02 16:00 6 7
2023-04-02 16:15 6 7
2023-04-02 16:30 6 7
2023-04-02 16:45 6 7
2023-04-02 17:00 6 7
2023-04-02 17:15 6 7
"

expectedMainRaw_raw <- read.table(text = trimws(expectedMainRaw_input), header = FALSE)

expectedMainRaw <- data.frame(
  timestamp = as.POSIXct(paste(expectedMainRaw_raw$V1, expectedMainRaw_raw$V2), format="%Y-%m-%d %H:%M"),
  Production = as.numeric(expectedMainRaw_raw$V3),
  Consumption = as.numeric(expectedMainRaw_raw$V4)
)

expectedTrackerRaw_input <- "
2023-03-02 16:00 1_1 1
2023-03-02 16:15 1_1 1
2023-03-02 16:30 1_1 1
2023-03-02 16:45 1_1 1
2023-03-02 17:00 1_1 1
2023-03-02 17:15 1_1 1
2023-04-02 16:00 1_1 1
2023-04-02 16:15 1_1 1
2023-04-02 16:30 1_1 1
2023-04-02 16:45 1_1 1
2023-04-02 17:00 1_1 1
2023-04-02 17:15 1_1 1
2023-03-02 16:00 1_2 2
2023-03-02 16:15 1_2 2
2023-03-02 16:30 1_2 2
2023-03-02 16:45 1_2 2
2023-03-02 17:00 1_2 2
2023-03-02 17:15 1_2 2
2023-04-02 16:00 1_2 2
2023-04-02 16:15 1_2 2
2023-04-02 16:30 1_2 2
2023-04-02 16:45 1_2 2
2023-04-02 17:00 1_2 2
2023-04-02 17:15 1_2 2
2023-03-02 16:00 1_3 3
2023-03-02 16:15 1_3 3
2023-03-02 16:30 1_3 3
2023-03-02 16:45 1_3 3
2023-03-02 17:00 1_3 3
2023-03-02 17:15 1_3 3
2023-04-02 16:00 1_3 3
2023-04-02 16:15 1_3 3
2023-04-02 16:30 1_3 3
2023-04-02 16:45 1_3 3
2023-04-02 17:00 1_3 3
2023-04-02 17:15 1_3 3
"

expectedTrackerRaw_raw <- read.table(text = trimws(expectedTrackerRaw_input), header = FALSE)

expectedTrackerRaw <- data.frame(
  timestamp = as.POSIXct(paste(expectedTrackerRaw_raw$V1, expectedTrackerRaw_raw$V2), format="%Y-%m-%d %H:%M"),
  tracker_name = as.character(expectedTrackerRaw_raw$V3),
  Solarproduktion = as.numeric(expectedTrackerRaw_raw$V4)
)

expectedAggregatedData_raw <- read.table(text = "
2023-03-02 0.1666667 0.3333333 0.5
2023-04-02 0.1666667 0.3333333 0.5
", header = FALSE)

expectedAggregatedData <- data.frame(
  as.Date(paste(expectedAggregatedData_raw$V1), format="%Y-%m-%d"),
  as.numeric(expectedAggregatedData_raw$V2),
  as.numeric(expectedAggregatedData_raw$V3),
  as.numeric(expectedAggregatedData_raw$V4)
)

colnames(expectedAggregatedData) <- c("time", "1_1_Solarproduktion_per_square_meter", "1_2_Solarproduktion_per_square_meter", "1_3_Solarproduktion_per_square_meter")

test_that("Data import works", {
  main <- setup()
  expect_equal(main$trackerMeta, expectedTrackerMeta)
  expect_equal(main$mainRaw, expectedMainRaw)
  expect_equal(main$trackerRaw, expectedTrackerRaw)
  expect_equal(main$fieldComparator$getAggregatedData(), expectedAggregatedData, tolerance = 1e-6)
})
