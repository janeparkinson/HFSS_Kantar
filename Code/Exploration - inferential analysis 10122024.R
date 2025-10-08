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
library(data.table) # For 'fread' function to read in CSVs efficiently
library(writexl)
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")



#Inferential analysis - exploration
#open datasets
HFSSFINAL_SIMD22_23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL_SIMD22_23.parquet")

#How may panellists in a 4 week period?
#Around 1800-1900
results_panellists <- HFSSFINAL_SIMD22_23 %>% group_by(period) %>% summarize(count = n_distinct(panel_id))

#How many purchases per period?
results_purchases <- HFSSFINAL_SIMD22_23 %>% group_by(period) %>% summarize(count = n_distinct(purchnum))

#Mean number of purchases per household, per period?
#purchnum = ID for a single purchased item
results_purchaseperHHCOUNT <- HFSSFINAL_SIMD22_23 %>% group_by(period, panel_id) %>% summarize(count = n_distinct(purchnum))
results_purchasesperHHMEAN <- results_purchaseperHHCOUNT %>% group_by(period) %>% 
  summarize(
    mean = mean(count),
    SD= sd(count))
  

#Summary stats per period (number of purchases)
summary(results_purchaseperHHCOUNT$count)


#QA - which categories are coded as HFSS exempt in the KWP?
#This is to check that no in scope HFSS products are incorrectly coded

unique_valuesEXEMPT <- HFSSFINAL_SIMD22_23%>%
  filter(HFSS_STATUS == "HFSS_EXEMPT") %>%
  pull(`VF_TITLE.x`) %>%
  unique()

print(unique_valuesEXEMPT)


#HFSS PURCHASE DESCRIPTIVE STATISTICS

# HFSS IN SCOPE
#Count number of HFSS (IN SCOPE) purchases in 2022 and 2023 (1,004,310)
Totalpurchases_HFSSinscope<- HFSSFINAL_SIMD22_23 %>%
  filter(HFSSFINAL_SIMD22_23$HFSS_STATUS == "HFSS_INREGCATS")

#Count of HFSS in scope purchases per 4 week period
Totalpurchases_HFSSinscopeCOUNT <- Totalpurchases_HFSSinscope %>%
  group_by(period) %>%
  summarise(count = n())


#HFSS EXEMPT
#Count number of HFSS (EXEMPT) purchases in 2022 and 2023  (949,555)
Totalpurchases_HFSSexempt<- HFSSFINAL_SIMD22_23 %>%
  filter(HFSSFINAL_SIMD22_23$HFSS_STATUS == "HFSS_EXEMPT")

#Count of HFSS exempt purchases per 4 week period
Totalpurchases_HFSSexemptCOUNT <- Totalpurchases_HFSSexempt %>%
  group_by(period) %>%
  summarise(count = n())

#NON-HFSS PURCHASES
#Count number of non HFSS (cats in regs and out of regs) purchases in 2022 and 2023 (2,988,008)
Totalpurchases_NONHFSS<- HFSSFINAL_SIMD22_23 %>%
  filter(HFSSFINAL_SIMD22_23$HFSS_STATUS %in% c("NOT_HFSS_NOREGS", "NOT_HFSS_INREGCATS"))

#Count of non HFSS purchases per 4 week period
Totalpurchases_NON_HFSS_COUNT <- Totalpurchases_NONHFSS %>%
  group_by(period) %>%
  summarise(count = n())





results_HFSSpurchaseperHHCOUNT <- HFSSFINAL_SIMD22_23 %>% group_by(period, panel_id) %>% summarize(count = n_distinct(purchnum))                                              