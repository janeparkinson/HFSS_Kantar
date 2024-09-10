

####################################################
#Link HFSS score data to joined panel/purchase file#
#NPM score varies with time (due to reformulation?)#
#Written by Elaine Tod 8th July 2024
####################################################


#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readr)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

###########################################
# 2022 HFSS master data file
###########################################

#Read in time, purchase/panel data and NPM2024P2 csv files
#The time file is necessary to add the NPM period which is used to add the HFSS and NPM categories from the NPM_2024P2 file
time2022 <- fread("2022 Purchase Data/time2022.csv")
joined_data_2022 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/joined_data2022.parquet")
NPM_2024P2 <- fread("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/NPM_2024P2.csv")

# joining the time csv and the panel/purchase data using purchase date
# This stage adds the period for NPM variable to the panel/purchase data - necessary to join the HFSS data file at the next stage
# 'Date' from time2022 variable is the primary key

# First rename 'Period for NPM' as R thinks the 'for' part of the name is a function
rename(time2022, Period_for_NPM = `Period for NPM`)
    
# LINKAGE 
# Merge purchasepanel data with time period file
Time_purchpanel22 <- joined_data_2022 %>%
  merge(y=time2022, by.y="Date", by.x="purchdate" )


# need to join datasets on product code and date of purchase variables in order to assign correct NPM code to period product was purchased (reformulation?)
#Recode prodcode in panpurchase dataset as double.
Time_purchpanel22$prodcode <- as.numeric(Time_purchpanel22$prodcode)


#Write intermediate file
write_parquet(Time_purchpanel22, "Time_purchpanel22.parquet")

#START HERE ON 10/9/24 
Time_purchpanel22 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/Time_purchpanel22.parquet")


#Join panel and purchase data with NPM and HFSS data (NPM_2024P2)
#Left join retaining all of the Time_purchpanel22 file as this has all the purchases for 2022
#NPM_2024P2 file has all NPM data for more products than there are purchases i.e. extra products that aren't matched onto panel file will drop off
PP_NPM22 <- dplyr::left_join(Time_purchpanel22, NPM_2024P2, by=c("prodcode" = "PRODUCT", "Period for NPM" = "period"))


#Add columns with names of products, markets, submarkets and extended (from Catrona's code 'Linking NPM scores.R'- adapted on 07/08/24)
# Product text data (has product descriptor but NOT producer details which is in rfnnnn files
#read in market files
product_area <- read_delim("2022 Purchase Data/product attributes/area.txt", col_names = c("area_code", "Area"))
product_market <- read_delim("2022 Purchase Data/product attributes/market.txt", col_names = c("market_code", "Market"))
product_sector <- read_delim("2022 Purchase Data/product attributes/mktsect.txt", col_names = c("sector_code", "Sector"))
product_submarket <- read_delim("2022 Purchase Data/product attributes/submark.txt", col_names = c("submarket_code", "Submarket"))
product_extended <- read_delim("2022 Purchase Data/product attributes/extended.txt", col_names = c("extended_code", "Extended"))

#Merge with descriptor info look ups for area, market, submarket, sector and extended with respective code variables
PP_NPM_MKTS22 <- PP_NPM22 %>%
  left_join(product_area, by=c("area" = "area_code")) %>%
  left_join(product_market, by=c("market" = "market_code")) %>%
  left_join(product_sector, by=c("mktsector" = "sector_code"))%>%
  left_join(product_submarket, by=c("submkt" = "submarket_code"))%>%
  left_join(product_extended, by=c("extended" = "extended_code"))

  

####################################
#Add store, product and producer info
####################################
#Read in stores file and master file
#PP_NPM_MKTS22- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/PP_NPM_MKTS22.parquet")
stores05 <- fread("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/2022 Purchase Data/stores05.csv")

#Reclassify storecode in stores05 file as character
stores05$"Store Code" <- as.character(stores05$"Store Code")

#Join store codes to master file
PP_NPM_MKTS_STORE22 <- PP_NPM_MKTS22 %>%
  left_join(stores05, by=c("shopcode" = "Store Code"))

##################################################
#Add product codes##
##################################################

# Product data - open data
rst_products <- fread("2022 Purchase Data/rst_products.csv")

#join product descriptors 
HFSSFINAL22 <- PP_NPM_MKTS_STORE22 %>%
  left_join(rst_products, by=c("prodcode" = "PRODUCT"))

###################################################
#ADD UOM data for volume conversion################
###################################################

#open uom file for 2022
rst_uom22 <- fread("2022 Purchase Data/rst_uom.csv")
HFSSFINAL22$VF <- as.numeric(HFSSFINAL22$VF)

#VF codes are repeated for some categories across different RF groupings but the uom information is the same, regardless
#I have collapsed the data retaining the first occurrence of the VF data. This is to allow for a one to many merge

rst_uom22_first <- rst_uom22[match(unique(rst_uom22$VF), rst_uom22$VF),]


#join uom data to 2022
HFSSFINAL22 <- HFSSFINAL22 %>%
  left_join(rst_uom22_first, by=c("VF"))

  
######################################
#Tidy file and remove extra variables#
######################################

#null1-9, Week Number, Day Number, ...1
HFSSFINAL22 <- subset(HFSSFINAL22, select = -c(null1, null2,null3, null4, null5, null6, null7, null8, null9, V1, `Day Number`, `Week Number`,  V6, V7, VF_TITLE.y))

#Reorder variables
HFSSFINAL22 <- HFSSFINAL22[  ,c(1:27,28,36,29,37,30,38,31,39,32,40,33,34,35,41:54)]


#Write HFSS22 file for merging with HFSS23 from code below.
#2,528,239 obs of 54 variables
write_parquet(HFSSFINAL22, "HFSSFINAL22.parquet")

  
############################################
###########################################
# 2023 master data file
############################################

#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readr)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

###########################################
# 2023 HFSS master data file
###########################################

#Read in time, purchase/panel data and NPM2024P2 csv files
#The time file is necessary to add the NPM period which is used to add the HFSS and NPM categories from the NPM_2024P2 file
time2023 <- fread("2023 Purchase Data/time2023.csv")
joined_data_2023 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/joined_data2023.parquet")
NPM_2024P2 <- fread("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/NPM_2024P2.csv")

# joining the time csv and the panel/purchase data using purchase date
# This stage adds the period for NPM variable to the panel/purchase data - necessary to join the HFSS data file at the next stage
# 'Date' from time2023 variable is the primary key

# First rename 'Period for NPM' as R thinks the 'for' part of the name is a function
rename(time2023, Period_for_NPM = `Period for NPM`)

# LINKAGE 
# Merge purchasepanel data with time period file
Time_purchpanel23 <- joined_data_2023 %>%
  merge(y=time2023, by.y="Date", by.x="purchdate" )


# need to join datasets on product code and date of purchase variables in order to assign correct NPM code to period product was purchased (reformulation?)
#Recode prodcode in panpurchase dataset as numeric.
Time_purchpanel23$prodcode <- as.numeric(Time_purchpanel23$prodcode)


#Write intermediate file
write_parquet(Time_purchpanel23, "Time_purchpanel23.parquet")

#START HERE FOR HFSS CODE 10/9/24
#Time_purchpanel23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/Time_purchpanel23.parquet")


#Join panel and purchase data with NPM and HFSS data (NPM_2024P2)
#Left join retaining all of the Time_purchpanel23 file as this has all the purchases for 2023
#NPM_2024P2 file has all NPM data for more products than there are purchases i.e. extra products that aren't matched onto panel file will drop off
PP_NPM23 <- dplyr::left_join(Time_purchpanel23, NPM_2024P2, by=c("prodcode" = "PRODUCT", "Period for NPM" = "period"))


#Add columns with names of products, markets, submarkets and extended (from Catrona's code 'Linking NPM scores.R'- adapted on 07/08/24)
# Product text data (has product descriptor but NOT producer details which is in rfnnnn files
#read in market files
product_area <- read_delim("2023 Purchase Data/product attributes/area.txt", col_names = c("area_code", "Area")) 
product_market <- read_delim("2023 Purchase Data/product attributes/market.txt", col_names = c("market_code", "Market")) 
product_sector <- read_delim("2023 Purchase Data/product attributes/mktsect.txt", col_names = c("sector_code", "Sector"))
product_submarket <- read_delim("2023 Purchase Data/product attributes/submark.txt", col_names = c("submarket_code", "Submarket"))
product_extended <- read_delim("2023 Purchase Data/product attributes/extended.txt", col_names = c("extended_code", "Extended"))

#Merge with descriptor info look ups for area, market, submarket, sector and extended with respective code variables
PP_NPM_MKTS23 <- PP_NPM23 %>%
  left_join(product_area, by=c("area" = "area_code")) %>%
  left_join(product_market, by=c("market" = "market_code")) %>%
  left_join(product_sector, by=c("mktsector" = "sector_code"))%>%
  left_join(product_submarket, by=c("submkt" = "submarket_code"))%>%
  left_join(product_extended, by=c("extended" = "extended_code"))

####################################
#Add store info
####################################
#Read in stores file and master file
#PP_NPM_MKTS23- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/PP_NPM_MKTS23.parquet")
stores05 <- fread("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/2023 Purchase Data/stores05.csv")

#Reclassify storecode in stores05 file as character
stores05$"Store Code" <- as.character(stores05$"Store Code")

#Join store codes to master file
PP_NPM_MKTS_STORE23 <- PP_NPM_MKTS23 %>%
  left_join(stores05, by=c("shopcode" = "Store Code"))

##################################################
#Add product codes##
##################################################

# Product data - open data
rst_products <- fread("2023 Purchase Data/rst_products.csv")

#join product descriptors 
HFSSFINAL23 <- PP_NPM_MKTS_STORE23 %>%
  left_join(rst_products, by=c("prodcode" = "PRODUCT"))


###################################################
#ADD UOM data for volume conversion################
###################################################

#open uom file for 2023
rst_uom23 <- fread("2023 Purchase Data/rst_uom.csv")
rst_uom23$SVF <- as.numeric(rst_uom23$VF)

#VF codes are repeated for some categories across different RF groupings but the uom information is the same, regardless
#I have collapsed the data retaining the first occurrence of the VF data. This is to allow for a one to many merge

rst_uom23_first <- rst_uom23[match(unique(rst_uom23$VF), rst_uom23$VF),]


#join uom data to 2023
HFSSFINAL23 <- HFSSFINAL23 %>%
  left_join(rst_uom23_first, by=c("VF"))



######################################
#Tidy file and remove extra variables#
######################################

#null1-9, Week Number, Day Number, ...1
HFSSFINAL23 <- subset(HFSSFINAL23, select = -c(null1, null2,null3, null4, null5, null6, null7, null8, null9, V1, `Day Number`, `Week Number`,  V6, V7, VF_TITLE.y, SVF))

#Reorder variables
HFSSFINAL23 <- HFSSFINAL23[  ,c(1:27,28,36,29,37,30,38,31,39,32,40,33,34,35,41:54)]


#Write HFSS23 file for merging with HFSS23 from code below.
#2,421,692 obs of 54 variables
write_parquet(HFSSFINAL23, "HFSSFINAL23.parquet")


#######################################
#Append HFSSFINAL23 onto HFSS22 final##
#######################################

#If not already open use code to open HFSSFINAL22 and 23 files
HFSSFINAL22 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL22.parquet")
HFSSFINAL23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL23.parquet")

#Append HFSSFINAL22 and HFSSFINAL23
HFSSFINAL22_23 = rbind(HFSSFINAL22, HFSSFINAL23)

#rename category variable descriptors
 
colnames(HFSSFINAL22_23) [colnames(HFSSFINAL22_23) %in% c("Area", "Market","Sector","Submarket","Extended", "VF_TITLE.x")] <- c("area_desc", "market_desc", "mktsector_desc", "submkt_desc",  "extended_desc", "VF_TITLE" )
  
#Write joined datafile
  write_parquet(HFSSFINAL22_23, "HFSSFINAL22_23.parquet")
