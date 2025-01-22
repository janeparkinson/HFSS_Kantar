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

####################################################
#Link panel_purchase_HFSS data to promotion descriptors + nutrition info#
#Written by Elaine Tod 10th October 2024
####################################################

#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readr)
library(readxl)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

#Read in HFSSFINAL22_23file, nutrition_data_202401, price promotion codes (Excel readme file)
#Open Master file for 2022 and 2023 combined
HFSSFINAL22_23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL22_23.parquet")
promo_desc <- fread("Promotions codes and descriptors.csv")

#Rename Code(s) as codes
promo_desc <- promo_desc %>%
 rename(codes = "Code(s)")

#Separate codes into cells where the "or" operator has been used
promo_desc_separated <- promo_desc %>%
  separate_longer_delim(`codes`, delim = ",")

#Separate codes into cells where "/" is the operator
promo_desc_separated <- promo_desc_separated %>%
  separate_longer_delim(`codes`, delim = "/")

#How many codes does each promotion description have?
promo_desc_count <- promo_desc_separated %>%
  count(Promotion)

#subset so that only promotion descriptors with one code remain
promo_desc_single <- subset.data.frame(promo_desc_count, n ==1, select=c('Promotion', "n")) 

#join the single subset file to promo_desc_separated
promo_desc_singlewithNA <- full_join(promo_desc_single, promo_desc_separated, by = "Promotion")

#subset so that only promotion descriptors with one code remain
promo_desc_single_final <- subset.data.frame(promo_desc_singlewithNA, n ==1, select=c('Promotion', 'codes')) 

#subsey so that only promotion descriptors with multiple codes remain
promo_desc_multiple_final <- promo_desc_singlewithNA[is.na(promo_desc_singlewithNA$"n"),]


#Separate letter and numbers in code
promo_desc_multiple_final <- promo_desc_multiple_final %>%
  mutate(number = as.numeric(str_extract_all(codes, "\\d+")), # Extract just the numeric part of the code (using regex \\d+), and format this as a number
         letter = substr(codes, 1, 1)) %>% # Extract just the letter from the code (always the first character)
  group_by(Promotion, letter)

# Fill down with missing numbers from codes
  promo_desc_multiple_final <- promo_desc_multiple_final %>%
    group_by(Promotion, letter) %>%
    complete(number= full_seq(number, period = 1), fill = list(Value = 0)) %>% # Fill in missing numbers between the range, for each promotion and letter
    ungroup() %>% # Remember to ungroup, or it can cause problems later on
    fill(codes, .direction = "down") %>% # The newly created rows have missing codes. Fill these downwards, so that we know how long the final code has to be
    mutate(number = if_else(str_length(codes) == 4, sprintf("%03d", number), sprintf("%04d", number)), # If the original code has 4 characters, format the number with three digits. Otherwise, format it with 4 digits.
           combined_code = paste0(letter, number)) %>% # Combine the letter and formatted number in one cell
    select(Promotion, combined_code) %>% # Select only the relevant columns
    rename(codes = combined_code)
  

#Remember to append single code rows from "promo_desc_separated
  promo_desc_final <- rbindlist(list(promo_desc_multiple_final, promo_desc_single_final))






