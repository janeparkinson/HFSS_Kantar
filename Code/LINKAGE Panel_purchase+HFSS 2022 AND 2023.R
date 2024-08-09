

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
# 2022 time2022+joined_data2022+NPM_2024P2#
###########################################

#Read in time, purchase/panel data and NPM2024P2 csv files
#The time file is necessary to add the NPM period which is used to add the HFSS and NPM categories from the NPM_2024P2 file
time2022 <- read_csv("2022 Purchase Data/time2022.csv")
joined_data_2022 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/joined_data2022.parquet")
NPM_2024P2 <- read_csv("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/NPM_2024P2.csv")


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

#Join panel and purchase data with NPM and HFSS data (NPM_2024P2)
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
###START HERE ON FRIDAY 09/08/24
#Read in stores file and master file
PP_NPM_MKTS22- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/PP_NPM_MKTS22.parquet")
stores05 <-read.csv("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/2022 Purchase Data/stores05.csv")

#Reclassify storecode in stores05 file as character
stores05$Store.code <- as.character(stores05$Store.Code)

#Join store codes to master file
PP_NPM_MKTS22_STORE22 <- PP_NPM_MKTS22 %>%
  left_join(store05, by=c("Shopcode" = "Store Code"))


#Add product codes abd uom
# Product data - open data
rst_products <- fread("2022 Purchase Data/rst_products.csv")
rst_uom <- read.csv("2022 Purchase Data/rst_uom.csv")

#Merge product and uom data with master file
rst_uom$VF <- as.character(rst_uom$VF)
rst_products$VF <- as.character(rst_products$VF)
 


HFSSPRODUCTS22 <- PP_NPM_MKTS22 %>%
  right_join(rst_products, by=c("validfield" = "VF")) %>%
  right_join(rst_uom, by=c("validfield" = "VF"))

#Save 2022 file
write_parquet(PP_NPM_MKTS22, "PP_NPM_MKTS22.parquet")




  

##########################################
# 2023 time2023+joined_data2023+NPM_2024P2#
###########################################

#Read in time, purchase/panel data and NPM2024P2 csv files
#The time file is necessary to add the NPM period which is used to add the HFSS and NPM categories from the NPM_2024P2 file
time2023 <- read_csv("2023 Purchase Data/time2023.csv")
joined_data_2023 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/joined_data2023.parquet")
NPM_2024P2 <- read_csv("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/NPM_2024P2.csv")


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
#Recode prodcode in panpurchase dataset as double.
Time_purchpanel23$prodcode <- as.numeric(Time_purchpanel23$prodcode)

#Join panel and purchase data with NPM and HFSS data (NPM_2024P2)
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

#Save 2023 file
write_parquet(PP_NPM_MKTS23, "PP_NPM_MKTS23.parquet")









