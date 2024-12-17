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

#Link nutritional data to master file
#Elaine Tod 05/12/2024


#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library (generics)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

#Open Master file for 2022 and 2023 combined
HFSSFINAL22_23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL22_23.parquet")
Nutrition2022 <- fread ("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/2022 Purchase Data/nutrition_data_202301.csv")
Nutrition2023 <- fread ("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/2023 Purchase Data/nutrition_data_202401.csv")

#Check for duplicate rows in both nutrition files
common_rows <- intersect(Nutrition2022, Nutrition2023)

#Append 2022 and 2023 nutritional files together
Nutrition2022_2023 = rbind(Nutrition2022, Nutrition2023)


#rename Purchase Number and Week No variables in nutrition file to match master file
colnames(Nutrition2022_2023) [colnames(Nutrition2022_2023) %in% c("Purchase Number", "Week No")] <- c("purchnum", "weeknum")

#recode purchnum as numeric
HFSSFINAL22_23PC$purchnum <- as.numeric(HFSSFINAL22_23PC$purchnum)




#Link combined nutrition file to master file using purchase number and week number variables
HFSSFINAL22_23NUT <- HFSSFINAL22_23 %>%
  left_join(Nutrition2022_2023, by = c("purchnum", "weeknum"))

#There are now 5,126,820 rows when there should still be 4,947,930. Explore why.
excessrows <-  HFSSFINAL22_23NUT %>% group_by('purchnum', 'weeknum') %>% filter(n()>1) 


#Write parquet file
#386 purchases/products have no associated nutritional information in the nutrition files
write_parquet(HFSSFINAL22_23PCNUT, "HFSSFINAL22_23NUT.parquet")

#Exploration of HFSS/NPM scoring anomaly around croissants
Croissants <- HFSSFINAL22_23PCNUT %>% 
  filter(HFSSFINAL22_23PCNUT$purchnum == '3140021' | purchnum == '3958225')





