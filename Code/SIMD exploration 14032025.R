#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# RStudio Workbench is strictly for use by Public Health Scotland staff and     
# authorised users only, and is governed by the Acceptable Usage Policy https://github.com/Public-Health-Scotland/R-Resources/blob/master/posit_workbench_acceptable_use_policy.md.
#
# This is a shared resource and is hosted on a pay-as-you-go cloud computing
# platform.  Your usage will incur direct financial cost to Public Health
# Scotland.  As such, please ensure
#
#   1. that this session is appropriately sized with the minimum number of CPUs
#      and memory required for the size and scale of your analysis;
#   2. the code you write in this script is optimal and only writes out the
#      data required, nothing more.
#   3. you close this session when not in use; idle sessions still cost PHS
#      money!
#
# For further guidance, please see https://github.com/Public-Health-Scotland/R-Resources/blob/master/posit_workbench_best_practice_with_r.md.
#
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readr)
library(readxl)
library(powerjoin)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

#######################
#Open master HFSS and SIMD files#
#######################
SIMD2023 <- fread("/PHI_conf/PHSci-HFSS/Kantar analysis/Kantar original data/panel_household_master_202301_v3.csv")
SIMD2024 <- fread("/PHI_conf/PHSci-HFSS/Kantar analysis/Kantar original data/panel_household_master_202401_v3.csv")
HFSSFINAL22_23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL22_23.parquet")

###################################
# Link SIMD files to master dataset
###################################

# Add the 2022 and 2023 SIMD datasets (rbind)
# There will be duplicates because many panel members will be in both the 2022 and 2023 panels
# SIMD 2023 relates to 2022 and SIMD 2024 relates to the 2023 panel

SIMD22_23 <- rbind(SIMD2023, SIMD2024) # add 2023 SIMD dataframe to the bottom of the 2022 SIMD dataframe (4,598 rows)

unique(SIMD22_23$panel_id) #2478 unique panel_IDs in linked SIMD file

SIMD22_23_nodups <- SIMD22_23 [!duplicated(SIMD22_23$panel_id), ] # remove duplicate rows for panel members who are in both the 2022 and 2023 panels

#Link joined SIMD file to HFSS masterfile
HFSSFINAL_SIMD22_23 <- HFSSFINAL22_23 %>%
  left_join(SIMD22_23_nodups %>% select(panel_id, SIMD2020_Quintile), by="panel_id")


#Save masterfile with SIMD variable
write_parquet(HFSSFINAL_SIMD22_23, "HFSSFINAL_SIMD22_23.parquet")


###################################
#Check linkage is all ok
###################################

#10 panel members without SIMD - these should already have been removed from the dataset as they are English postcodes
#EX2, IP31, LN11, YO14, GL17, TS4, CA28, PL26, TS14, LA7

#There are no rows missing an SIMD code








