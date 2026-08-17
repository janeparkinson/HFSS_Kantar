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

# Create a 'periods' object which includes all periods
# The below pseudocode assumes that the first 20 rows of the NPM dataset has a complete list of periods, which it might not (and it might require more rows than 20)

#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readr)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

#Files needed
HFSSFINAL22 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL22.parquet")
HFSSFINAL23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL23.parquet")
HFSSFINAL22 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL22.parquet")
HFSSFINAL22_23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL22_23.parquet")
NPM_2024P2 <- fread("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/NPM_2024P2.csv")


# Create a 'periods' object which includes all periods
# The below pseudocode assumes that the first 65 rows of the NPM dataset has a complete list of periods, which it might not (and it might require more rows than 40)
periods <- NPM_2024P2  %>%
  select(period) %>%
  head(65) # Only select first 65 rows


# Create a 'products' object, which includes all product numbers
products <- NPM_2024P2 %>%
  select(PRODUCT) %>%
  distinct(.) # Distinct removes duplicate products

#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readr)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

#Files needed
NPM_2024P2 <- fread("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/NPM_2024P2.csv")


# Create a 'periods' object which includes all periods
# The below pseudocode assumes that the first 65 rows of the NPM dataset has a complete list of periods, which it might not (and it might require more rows than 40)
#periods <- NPM_2024P2  %>%
  select(period) %>%
  head(65) # Only select first 65 rows

periods <- NPM_2024P2[match(unique(NPM_2024P2$period), NPM_2024P2$period),]


# Create a 'products' object, which includes all product numbers
products <- NPM_2024P2 %>%
  select(PRODUCT) %>%
  distinct(.) # Distinct removes duplicate products


# Create a dataframe like the first bullet point below
# This assumes that there are 65 periods and 672,639 unique product codes)
all_periods <- data.frame(period=rep(periods$period, 672639),
                          (product=rep(products$PRODUCT,each=65)))


#rename column 2
colnames(all_periods) [colnames(all_periods) %in% c("X.product...rep.products.PRODUCT..each...65..")] <- c("PRODUCT")


#Part 2: Use merge (join) to map the NPM data to each period and product
all_periods_NPM <- dplyr::left_join(all_periods, NPM_2024P2, by=c('period', 'PRODUCT'))


#Part 3: Use fill function to fill down the previous or next NPM score when one is missing
all_periods_NPM <- all_periods_NPM %>%
  fill(NPM, HFSS, .direction = "downup")

all_periods_NPM <- subset(all_periods_NPM, select = -c(V1))  #Remove V1 variable

write_parquet(all_periods_NPM, "all_periods_NPM.parquet")


#12/09/24
#missing product data from NPM file for 60 products equating to 3,081 purchases - NPM and HFSS need to be calculated manually?
#Open missing prodcode file
missing_prodcodes<- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/missing_prodcodes.parquet")

#Open nutrition information for 2023
nutrition_data_202401 <- fread("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/2023 Purchase Data/nutrition_data_202401.csv")

#spot check of which products aren't in NPM file
rst_products %>% filter(PRODUCT == '900036')# old potatoes

#When filtering on purchase numbers for products which are missing from the NPM file, I found that these purchases were also missing from the nutrition data file
#Panel purchase data - get purchase number relating to product 900036
HFSSFINAL23 <- HFSSFINAL23 %>%
  filter(prodcode == "900036" )# old potatoes

#nutrition file. HFSS cannot therefore be imputed.
nutrition_data_202401 <- nutrition_data_202401 %>%
  filter('Purchase Number' == "14445962" )# old potatoes


