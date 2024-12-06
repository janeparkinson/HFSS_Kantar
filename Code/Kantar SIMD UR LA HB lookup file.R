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

#Create lookup for Kantar
#Full postcode, SIMD, urban/rural, LA, HB, other?
#Written by Elaine Tod 06/12/24


#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readr)
library(readxl)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

#Open files to link (both based on postcodes 2023-1)
#"postcode_to_SIMD" This postcode look-up is based on the postcodes from the 2023-1 NRS Scottish Postcode Index. https://www.gov.scot/publications/scottish-index-of-multiple-deprivation-2020v2-postcode-look-up/
# "geog_lookup" This is the 2023-1 postcode directory https://webarchive.nrscotland.gov.uk/20241128123056/https://www.nrscotland.gov.uk/statistics-and-data/geography/our-products/scottish-postcode-directory/2023-1
postcode_to_SIMD <- fread("Postcode to SIMD.csv")
geog_lookup <- fread("smallUser postcodes NRS 2023-2.csv")

#Which postcodes are not in both files?
#41,283 postcodes are in the postcode to SIMD file but not in the small user file.
# A spot check of postcodes shows that these relate to postcodes which have been deleted and are no longer in use
result <- anti_join(postcode_to_SIMD, geog_lookup_subset, by = "Postcode")

#Review geog_lookupfile and delete unnecessary columns
geog_lookup_subset = subset(geog_lookup, select = c(1, 11, 16, 23, 25, 53, 54 ))


#Left join geog_lookup subset (with current postcodes) to SIMD file, removing deleted postcodes
#Join SIMD file with geography lookup file
Kantar_geoglookup <- geog_lookup_subset %>%
  left_join(postcode_to_SIMD, by = c("Postcode"))

#Reorder columns and remove DZ column (column 8, duplicate)
Kantar_geoglookup <- Kantar_geoglookup[  ,c(1,9,10:12,4,5,6,7,2,3)]

#Save geography lookup file
write.csv(Kantar_geoglookup, "Kantar_geoglookup.csv")


