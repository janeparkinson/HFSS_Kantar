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

#Read in HFSSFINAL22_23file,, price promotion codes (Excel readme file)
#Open Master file for 2022 and 2023 combined
HFSSFINAL22_23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL22_23.parquet")
promo_desc <- fread("Promotions codes and descriptors_383_384_rem.csv") # these two lines have promotional codes which encapsulate all of the other codes so causes duplication and linkage issues

#Rename Code(s) as codes
promo_desc <- promo_desc %>%
 rename(codes = "Code(s)")

#Separate codes into cells where the "or" operator has been used
promo_desc_separated <- promo_desc %>%
  separate_longer_delim(`codes`, delim = ",")

#Add in a column with a marker for those descriptors that have a "/" operator. This distinguishes those descriptors with multiple rows but no "/"
#This avoids meal deals, for example, having codes added that don't apply.
promo_desc_separated <- promo_desc_separated %>%
  mutate(marker = ifelse(grepl("/", codes), 1, 0))

#Separate codes into cells where "/" is the operator
promo_desc_separated <- promo_desc_separated %>%
  separate_longer_delim(`codes`, delim = "/")


#subset so that only promotion descriptors with one code, or have discrete codes which are not part of a range remain (e.g. meal deals £5.99+)
promo_desc_no_range <- subset.data.frame(promo_desc_separated, marker ==0, select=c('Promotion', 'codes')) 

promo_desc_range <- subset.data.frame(promo_desc_separated, marker ==1, select=c('Promotion', 'codes', 'marker'))


#Separate letter and numbers in code
promo_desc_range_final <- promo_desc_range %>%
  mutate(number = as.numeric(str_extract_all(codes, "\\d+")), # Extract just the numeric part of the code (using regex \\d+), and format this as a number
         letter = substr(codes, 1, 1)) %>% # Extract just the letter from the code (always the first character)
  group_by(Promotion, letter)

# Fill down with missing numbers from codes
  promo_desc_range_final <- promo_desc_range_final %>%
    group_by(Promotion, letter) %>%
    complete(number= full_seq(number, period = 1), fill = list(Value = 0)) %>% # Fill in missing numbers between the range, for each promotion and letter
    ungroup() %>% # Remember to ungroup, or it can cause problems later on
    fill(codes, .direction = "down") %>% # The newly created rows have missing codes. Fill these downwards, so that we know how long the final code has to be
    mutate(number = if_else(str_length(codes) == 4, sprintf("%03d", number), sprintf("%04d", number)), # If the original code has 4 characters, format the number with three digits. Otherwise, format it with 4 digits.
           combined_code = paste0(letter, number)) %>% # Combine the letter and formatted number in one cell
    select(Promotion, combined_code) %>% # Select only the relevant columns
    rename(codes = combined_code)
  

#Remember to append single code rows from "promo_desc_separated
  promo_desc_final <- rbindlist(list(promo_desc_range_final, promo_desc_no_range))

#Link to master file
#ISSUE PROMOTION CODES DO NOT ALL APPEAR TO BE UNIQUE FOR A SPECIFIC PROMOTION TYPE!
  
  HFSSFINAL22_23 <- HFSSFINAL22_23 %>%
    left_join(promo_desc_final, by=c("promcode" = "codes"))

#Check for NAs
  #There are 1,326 purchases with promotion code "aaaa". This does not correspond with a promotion descriptor in the Kantar files
  subset_na_promotion <- HFSSFINAL22_23[is.na(HFSSFINAL22_23$Promotion), ]
  
  #The promcode aaaa means "TPR £10.00+".
  #Change these NAs in the promcode description
  HFSSFINAL_22_23 <- HFSSFINAL_22_23%>%
    mutate(Promotion = ifelse(promcode== "aaaa", "TPR £10.00+", Promotion))
  

      
  
  
  

  write_parquet(HFSSFINAL22_23, "HFSSFINAL22_23.parquet")
  
  ####################################################
  #Code two promotion category variables for analysis
  
  #V1 (Bundle offers, extra free, free additional item, loyalty card, meal deal, multi-buy, no prom, TPR)
  #V2 (Permitted, Restricted, No promotion)
  
  #V1
  
  
  #Create promotion regulation category variable    
  HFSSFINAL22_23 <- HFSSFINAL_22_23 %>%
    mutate(promocode_regs = case_when(
      str_detect(promcode, "^b\\d{2}$") & as.numeric(str_extract(promcode, "\\d+")) >= 1 & as.numeric(str_extract(promcode, "\\d+")) <= 37 ~ "Multi-buy",
      str_detect(promcode, "^b\\d{3}$") & as.numeric(str_extract(promcode, "\\d+")) >= 40 & as.numeric(str_extract(promcode, "\\d+")) <= 182 ~ "Multi-buy",
      str_detect(promcode, "^c\\d{3}$") & as.numeric(str_extract(promcode, "\\d+")) >= 1 & as.numeric(str_extract(promcode, "\\d+")) <= 170 ~ "Multi-buy",
      promcode %in% c("c300", "c999") ~ "Multi-buy",
      
      str_detect(Promotion, "^TPR") ~ "TPR",
      
      promcode %in% c("b038", "b188", "b190", "b185") ~ "Mealdeal-dinein",
      
      str_detect(promcode, "^a\\d{3}$") & as.numeric(str_extract(promcode, "\\d+")) >= 1 & as.numeric(str_extract(promcode, "\\d+")) <= 999 ~ "Other promotions",
      str_detect(promcode, "^d\\d{3}$") & as.numeric(str_extract(promcode, "\\d+")) >= 1 & as.numeric(str_extract(promcode, "\\d+")) <= 999 ~ "Other promotions",
      
      promcode %in% c("a000", "d000", "0000") | is.na(promcode) ~ "No promotion",
      
      TRUE ~ "Other"
    ))
  
  
  
  
  
  
  
