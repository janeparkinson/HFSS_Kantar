

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
library(readxl)
library(powerjoin)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

###########################################
# 2022 HFSS master data file
###########################################

#Read in time, purchase/panel data and NPM2024P2 csv files
time2022 <- fread("2022 Purchase Data/time2022.csv")
joined_data_2022 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/joined_data2022.parquet")
all_periodsproductsNPM <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/all_periods_NPM.parquet") # 'complete' NPM file with imputed NPM anf HR values for products with missing periods


# joining the time csv and the panel/purchase data using purchase date (The time file is necessary to add the NPM period which is used to add the HFSS and NPM categories from the NPM_2024P2 file)
# This stage adds the period for NPM variable to the panel/purchase data - necessary to join the HFSS data file (joined_data_2022)
time2022 <- rename(time2022, `period` = `Period for NPM`)

Time_purchpanel22 <- joined_data_2022 %>%
  merge(y=time2022, by.y="Date", by.x="purchdate" )


#Recode prodcode in panpurchase dataset as double.
Time_purchpanel22$prodcode <- as.numeric(Time_purchpanel22$prodcode)

#Join panel and purchase data with NPM and HFSS data (all_periodsproductsNPM)
#Left join retaining all of the Time_purchpanel22 file as this has all the purchases for 2022
#NPM_2024P2 file has all NPM data for more products than there are purchases i.e. extra products that aren't matched onto panel file will drop off
PP_NPM22 <- dplyr::left_join(Time_purchpanel22, all_periodsproductsNPM, by=c("period", "prodcode" = "PRODUCT"))

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

#Remove panellists with English postcodes
HFSSFINAL22 <- subset(HFSSFINAL22, !(starting_postcode %in% c("YO14", "LN11", "TS4", "IP31", "EX2", "GL17") ))

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
  left_join(rst_uom22_first)

  
######################################
#Tidy file and remove extra variables#
######################################

#null1-9, Week Number, Day Number, ...1
HFSSFINAL22 <- subset(HFSSFINAL22, select = -c(null1, null2,null3, null4, null5, null6, null7, null8, null9, `Day Number`, `Week Number`, V6,V7))


HFSSFINAL22 <- HFSSFINAL22[  ,c(1,26,2:25,27,52,53,54,28,36,29,37,30,38,31,39,32,40,33,34,35,41:51)]


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
time2023 <- fread("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/2023 Purchase Data/time2023.csv")
joined_data_2023 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/joined_data2023.parquet")
Kantar_regcats <- read_xlsx("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/PHS HFSS Itemisation Breakdown amended.xlsx")
NPM_2024P2 <- fread("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/NPM_2024P2.csv")
all_periodsproductsNPM <-read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/all_periodsproductsNPM.parquet")


# joining the time csv and the panel/purchase data using purchase date
# This stage adds the period for NPM variable to the panel/purchase data - necessary to join the HFSS data file at the next stage
# 'Date' from time2023 variable is the primary key

# First rename 'Period for NPM' as R thinks the 'for' part of the name is a function
time2023 <- rename(time2023, `period` = `Period for NPM`)

# LINKAGE 
# Merge purchasepanel data with time period file
Time_purchpanel23 <- joined_data_2023 %>%
  merge(y=time2023, by.y="Date", by.x="purchdate" )  


# need to join datasets on product code and date of purchase variables in order to assign correct NPM code to period product was purchased (reformulation?)
#Recode prodcode in panpurchase dataset as numeric.
Time_purchpanel23$prodcode <- as.numeric(Time_purchpanel23$prodcode)


#Join panel and purchase data with NPM and HFSS data (NPM_2024P2)
#Left join retaining all of the Time_purchpanel23 file as this has all the purchases for 2023
#NPM_2024P2 file has all NPM data for more products than there are purchases i.e. extra products that aren't matched onto panel file will drop off
PP_NPM23 <- dplyr::left_join(Time_purchpanel23, all_periodsproductsNPM, by=c("period", "prodcode" = "PRODUCT" ))


#Add columns with names of products, markets, submarkets and extended (from Catriona's code 'Linking NPM scores.R'- adapted on 07/08/24)
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
rst_uom23$VF <- as.numeric(rst_uom23$VF)

#VF codes are repeated for some categories across different RF groupings but the uom information is the same, regardless
#I have collapsed the data retaining the first occurrence of the VF data. This is to allow for a one to many merge
rst_uom23_first <- rst_uom23[match(unique(rst_uom23$VF), rst_uom23$VF),]


#join uom data to 2023
HFSSFINAL23 <- HFSSFINAL23 %>%
  left_join(rst_uom23_first)



######################################
#Tidy file and remove extra variables#
######################################

#null1-9, Week Number, Day Number, ...1
HFSSFINAL23 <- subset(HFSSFINAL23, select = -c(null1, null2,null3, null4, null5, null6, null7, null8, null9, `Day Number`, `Week Number`, V1, V6, V7))
HFSSFINAL23 <- HFSSFINAL23[  ,c(1,26,2:25,27,52,53,54,28,36,29,37,30,38,31,39,32,40,33,34,35,41:51)]#Reorder variables

#Drop  panellists who live in England#Outward postcodes (YO14 - North Yorkshire, LN11 - Lincolnshire, TS4 - Teeside, IP31 - Suffolk, EX2- Devon, GL17 - Gloucestershire)
HFSSFINAL23 <- subset(HFSSFINAL23, !(starting_postcode %in% c("YO14", "LN11", "TS4", "IP31", "EX2", "GL17") ))

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
 colnames(HFSSFINAL22_23) [colnames(HFSSFINAL22_23) %in% c("Area", "Market","Sector","Submarket","Extended")] <- c("area_desc", "market_desc", "mktsector_desc", "submkt_desc",  "extended_desc")
 colnames(Kantar_regcats) [colnames(Kantar_regcats) %in% c("RST 4 Market", "RST 4 Sub Market", "RST 4 Extended")] <- c("market_desc", "submkt_desc", "extended_desc")


#LINK HFSS REG CATEGORIES FROM KANTAR WITH MAIN FILE
 #Step 1: Create market_sub_extend variable i.e. collapsed HFSS dataset so every extended category is only listed once
 #This step shows that there are 1,777 distinct extended categories across submarkets and markets
 market_sub_extend <- HFSSFINAL22_23 %>%
  distinct(market_desc, submkt_desc, extended_desc)
 
# Step 2: Join Kantar regs file with extended categories from collapsed purchase file based on 'market and submarket'so that the full extended list is joined with the HFSS categories
#Split Kantar reg file into rows to be linked on 2 IDs and rows to be linked on 3 IDs
 
 #Remove rows with NA for extended category (Kantar_noNA)- three linkage IDs
 Kantar_regcats <- subset(Kantar_regcats, select = -c(Amendment))
 Kantar_noNA <- Kantar_regcats[complete.cases(Kantar_regcats), ]
 colnames(Kantar_noNA) [colnames(Kantar_noNA) %in% c("RST 4 Market", "RST 4 Sub Market", "RST 4 Extended")] <- c("market_desc", "submkt_desc", "extended_desc")
 
 #Create Kantar file all NA for extended = two linkage variables
 Kantar_allNA <- Kantar_regcats %>%
   filter(is.na(extended_desc))
 
 #Do two step left-joins
 HFSS_COMPLETEA <- Kantar_noNA %>%
      left_join(market_sub_extend, by = c("market_desc", "submkt_desc", "extended_desc"))
 
 #Rows that can be joined with two IDs
 HFSS_COMPLETEB <- Kantar_allNA %>%
   left_join(market_sub_extend, by = c("market_desc", "submkt_desc"))
 HFSS_COMPLETEB <- HFSS_COMPLETEB[, -which(names(HFSS_COMPLETEB) == "extended_desc.x")]
 colnames(HFSS_COMPLETEB) [colnames(HFSS_COMPLETEB) %in% c("extended_desc.y")] <- c("extended_desc")
 
 
 #Append HSSCOMPLETEA and HFSSCOMPLETEB
 HFSS_COMPLETE = rbind(HFSS_COMPLETEA, HFSS_COMPLETEB)

#LINK reviseed Kantar regs file to HFSS data file
  HFSSFINAL22_23 <- HFSSFINAL22_23 %>%
   left_join(HFSS_COMPLETE, by=c("market_desc", "submkt_desc", "extended_desc"))
  
# Added 29/22/2024
#Should there be a stage in here where I then remove products which are excluded from each reg category because their HFSS score is 0.
#Code below not working yet 29/11/24
  HFSSFINAL22_23PC <- HFSSFINAL22_23PC %>%
    mutate(`HFSS Category` = na_if(HFSS, 0))
  
  
 HFSSFINAL23 <- PP_NPM_MKTS_STORE23 %>%
   left_join(rst_products, by=c("prodcode" = "PRODUCT"))
 

#Write joined datafile
  write_parquet(HFSSFINAL22_23, "HFSSFINAL22_23.parquet")

  
  

  
 

