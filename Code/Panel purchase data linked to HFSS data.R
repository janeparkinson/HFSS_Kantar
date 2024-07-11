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


#Link HFSS score data to joined panel/purchase file
#NPM score varies with time (due to reformulation?) 
#Written by Elaine Tod 8th July 2024

#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readr)
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")


# !!!2022 DATA!!!!!!

#Read in time, purchase/panel data and NPM2024P2 csv files
time2022 <- read_csv("2022 Purchase Data/time2022.csv")
joined_data_2022 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/joined_data2022.parquet")
NPM_2024P2 <- read_csv("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/NPM_2024P2.csv")

# joining the time csv and the panel/purchase data using purchase date (one to many - time to purchase_panel2022)
# This stage add the period for NPM variable to the panel/purchase data - necessary to join the HFSS data file
Time_purchpanel <- time2022 %>%
  merge(y=joined_data_2022, by.y="purchdate", by.x="Date" )

# joining HFSS and Time_purchpanel file
# need to join datasets on product code and date of purchase variables in order to assign correct NPM code to period product was purchased (reformulation?)

#Recode prodcode in panpurchase datset as double.
Time_purchpanel$prodcode <- as.numeric(Time_purchpanel$prodcode)

#Join panel and purchase data with NPM and HFSS status
HFSSPP2022 <- dplyr::left_join(Time_purchpanel, NPM_2024P2, by=c("prodcode" = "PRODUCT", "Period for NPM" = "period"))


# !!!2023 DATA!!!!!!
# Repeat for 2023 data then append using rbind command








