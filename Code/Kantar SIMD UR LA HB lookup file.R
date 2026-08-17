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
postcode_to_SIMD_SG <- fread("Postcode to SIMD.csv")
geog_lookup_NRS <- fread("smallUser postcodes NRS 2023-2.csv")

#Which postcodes are not in both files?
#41,283 postcodes are in the postcode to SIMD file but not in the small user file.
# A spot check of postcodes shows that these relate to postcodes which have been deleted and are no longer in use,
# Some postcodes are split postcodes and have an A, B or C suffix to allow them to align with other geographical boundaries. The A suffix indicates where the biggest proportion of the population
# lies and this is the datazone used in the SIMD postcode lookup.

postcodesnotinbothfiles <- anti_join(postcode_to_SIMD_SG, geog_lookup_NRS, by = "Postcode")

#38,883 deleted postcodes in NRS file
# xxx with split postcodes (select postcode with A suffix then remove A suffix from postcode)

#Drop 38,883 rows with deleted postcodes
geog_lookup_NRS_No_del_pc<-subset(geog_lookup_NRS, DateOfDeletion=="")

#Isolate all rows with a postcode that has a suffix of B or C and remove these. Keep those with suffix A
#Check all Bs and Cs have been removed then isolate all with a suffix of A and remove the A. This should match the datazone info in the SG file

#Step 1: Split outward and inward parts of postcode
geog_lookup_NRS_No_del_pc_suff <- geog_lookup_NRS_No_del_pc %>% separate(Postcode, into = c("Outward", "Inward"), sep = " ")

#Step 2: Split inward postcode so suffix A, B or C is in a separate column
geog_lookup_NRS_No_del_pc_suff_rem <- geog_lookup_NRS_No_del_pc_suff %>% separate(Inward, into = c("Inward", "Suffix"), sep = 3)

#Step 3: Delete rows where Suffix has a B or C
geog_lookup_NRS_cleaned <- subset(geog_lookup_NRS_No_del_pc_suff_rem, Suffix == "A" | Suffix =="")

#Step 4: Concatenate Outward and inward parts of the postcode
geog_lookup_NRS_pc_concat <- geog_lookup_NRS_cleaned %>%
  mutate(Postcode = paste(Outward, Inward, sep = " "))

#Move postcode variable to the start of the dataframe
geog_lookup_NRS_pc_concat <- geog_lookup_NRS_pc_concat[, c("Postcode", setdiff(names(geog_lookup_NRS_cleaned), "Postcode"))]

#Step 5: Delete suffix variable
geog_lookup_NRS_pc_concat <- subset(geog_lookup_NRS_pc_concat, select = c(-Outward, -Inward, -Suffix))

#Review geog_lookupfile and delete unnecessary columns
geog_lookup_subset = subset(geog_lookup_NRS_pc_concat, select = c(1, 11, 16, 23, 25, 53, 54 ))


#Left join geog_lookup subset (with current postcodes) to SIMD file, removing deleted postcodes
#Join SIMD file with geography lookup file
Kantar_geoglookup <- geog_lookup_subset %>%
  left_join(postcode_to_SIMD_SG, by = c("Postcode"))

#Reorder columns and remove DZ column (column 8, duplicate)
Kantar_geoglookup <- Kantar_geoglookup[  ,c(1,9,10:12,4,5,6,7,2,3)]

#Save geography lookup file
write.csv(Kantar_geoglookup, "Kantar_geoglookup.csv")


