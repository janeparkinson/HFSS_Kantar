#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# RStudio Workbench is strictly for use by Public Health Scotland staff and     
# authorised users only, and is governed by an <Acceptable Usage Policy>.
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
# For further guidance, please see <insert link>.
#
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

#This code calculates the outcome measures for WP2 - promotions
#Remember to apply grossing up factor for reporting figures
#Written by Elaine Tod
#10th October 2025



#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readr)
library(readxl)
library(data.table) # For 'fread' function to read in CSVs efficiently
library(writexl)
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")


#open dataset
HFSSFINAL_SIMD22_23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL_SIMD22_23.parquet")


#RQ1 HAVE THERE BEEN CHANGES IN HOW TARGETED HFSS PRODUCTS ARE PROMOTED BY TARGETED RETAILERS USING PERMITTED OR NEW PRICE PROMOTIONS IN SCOTLAND?

#OM1: NUMBER OF PURCHASES (EACH PURCHASE MAY INCLUDE A QUANTITY GREATER THAN 1) BOUGHT ON PERMITTED PROMOTION TYPE



#no purchases in Scotland 22-23 (a purchase may include different quantities of a product purchased at the same time)
#HFSSFINAL_SIMD22_23 %>%
#  group_by(promocode_regs) %>%
# summarise(row_count = n())



#No of HFSS packs bought in Scotland 22-23 (pcksbought multiplied by grossupfactor)

#Prevent scientific notation
options(scipen = 999)

HFSS_purchases_by_promotion <- HFSSFINAL_SIMD22_23 %>%
  mutate(pcksbought = 
           as.numeric(pcksbought),
         grossupfact =
           as.numeric(grossupfact),
                     adjusted_value_pcksbought = pcksbought * grossupfact) %>%
                     group_by(promocode_regs, HFSS_STATUS) %>%
                     summarise(total_value = sum(adjusted_value_pcksbought, na.rm = TRUE))





#Convert grossupfactor to a numeric variable from a character variable
HFSSFINAL_SIMD22_23 <- HFSSFINAL_SIMD22_23 %>%
  mutate(grossupfact_num = as.numeric(grossupfact))


Permprom_purchases <- HFSSFINAL_SIMD22_23 %>%
  mutate(grossupfact_num = as.numeric(grossupfact)) %>%
  group_by(promocode_regs) %>%
  summarise(count = n()) %>%
  mutate(Purchases_Scotland = count * grossupfact_num)

library(dplyr)

Permprom_purchases <- HFSSFINAL_SIMD22_23 %>%
  mutate(grossupfact_num = as.numeric(grossupfact),
  group_by(promocode_regs) %>%
  summarise(sum() = n(),
    Purchases_Scotland = count * HFSSFINAL_SIMD22_23$grossupfact_num ), na.rm = TRUE)




