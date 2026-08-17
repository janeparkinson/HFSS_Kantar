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

#Linkage 3: Linkage 2 + NPM files
#Written by Elaine Tod 8th July 2024





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
# STAGE 1: 2022 HFSS master data file
###########################################

#Read in time, purchase/panel data and NPM2024P2 csv files
combined_rfs <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/combined_rfs.parquet")
time2022 <- fread("2022 Purchase Data/time2022.csv")
joined_data_2022 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/joined_data2022.parquet")
all_periodsproductsNPM <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/all_periods_NPM.parquet") # 'complete' NPM file with imputed NPM and HR values for products with missing periods


# joining the time csv and the panel/purchase data using purchase date (The time file is necessary to add the NPM period which is used to add the HFSS and NPM categories from the NPM_2024P2 file)
# This stage adds the period for NPM variable to the panel/purchase data - necessary to join the HFSS data file (joined_data_2022)
time2022 <- rename(time2022, `period` = `Period for NPM`)

Time_purchpanel22 <- joined_data_2022 %>%
  merge(y=time2022, by.y="Date", by.x="purchdate" )

# Reorder columns
Time_purchpanel22 <- Time_purchpanel22 %>%
  select("Week Number", "Day Number", "period", everything())

#Recode prodcode in panpurchase dataset as double.
Time_purchpanel22$prodcode <- as.numeric(Time_purchpanel22$prodcode)

#Join panel and purchase data with NPM and HFSS data (all_periodsproductsNPM)
#Left join retaining all of the Time_purchpanel22 file as this has all the purchases for 2022
#NPM_2024P2 file has all NPM data for more products than there are purchases i.e. extra products that aren't matched onto panel file will drop off
PP_NPM22 <- dplyr::left_join(Time_purchpanel22, all_periodsproductsNPM, by=c("period", "prodcode" = "PRODUCT"))

# Some products are missing from the NPM file
#53 product numbers are missing from the NPM file - [1] 306584 900035 900034 900036 870428 306284 900516 306900 900334 900517 306413 307106 306316 306574 307411 306385 306818 306665 306755 306841 307064
#[22] 306662 306739 306895 306408 306741 679973 306745 515408 307048 306294 306327 306242 306231 306224 306315 306406 147718 307122 258544 306429 306612
#[43] 306358 306261 259374 306906 306268 307143 306803 306260 306342 306276 307857

#unique_values <- subset_na_PP_NPM22 %>%
#  filter(is.na(NPM)) %>%
#  pull(prodcode) %>%
#  unique()

#Add columns with names of products, markets, submarkets and extended (from Catriona's code 'Linking NPM scores.R'- adapted on 07/08/24)
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

#Check if rst_product files  in 2022 and 2023 are the same
#They are the same
rst_products_2022 <- read_csv("2022 Purchase Data/rst_products.csv")
rst_products_2023 <- read_csv("2023 Purchase Data/rst_products.csv")

rst_products_differences <- are_equal <- isTRUE(all.equal(rst_products_2022, rst_products_2023, check.attributes = FALSE)) # both the 2022 and 2023 files are the same

#join product descriptors 
HFSSFINAL22a <- PP_NPM_MKTS_STORE22 %>%
  left_join(rst_products_2022, by=c("prodcode" = "PRODUCT"))

#Remove panellists with English postcodes
HFSSFINAL22b <- subset(HFSSFINAL22a, !(starting_postcode %in% c("YO14", "LN11", "TS4", "IP31", "EX2", "GL17") ))

######################################
#Tidy file and remove extra variables#
######################################

#null1-9, Week Number, Day Number, ...1
HFSSFINAL22c <- subset(HFSSFINAL22b, select = -c(`...6`, `...7`))



#Write HFSS22 file for merging with HFSS23 from code below.
#2,526,238 obs of 52 variables
write_parquet(HFSSFINAL22c, "HFSSFINAL22.parquet")


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

# LINKAGE 
# Merge purchasepanel data with time period file
Time_purchpanel23 <- joined_data_2023 %>%
  merge(y=time2023, by.y="Date", by.x="purchdate" )  

# Reorder columns
Time_purchpanel23 <- Time_purchpanel23 %>%
  select("Week Number", "Day Number", "Period for NPM", everything()) 

#Rename period variable
Time_purchpanel23 <- Time_purchpanel23 %>%
  rename( period = `Period for NPM`)


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
HFSSFINAL23a <- PP_NPM_MKTS_STORE23 %>%
  left_join(rst_products, by=c("prodcode" = "PRODUCT"))


######################################
#Tidy file and remove extra variables#
######################################

#null1-9, Week Number, Day Number, ...1
HFSSFINAL23b <- subset(HFSSFINAL23a, select = -c(`V1`, `V6`, `V7`))


#Drop  panellists who live in England#Outward postcodes (YO14 - North Yorkshire, LN11 - Lincolnshire, TS4 - Teeside, IP31 - Suffolk, EX2- Devon, GL17 - Gloucestershire)
HFSSFINAL23c <- subset(HFSSFINAL23b, !(starting_postcode %in% c("YO14", "LN11", "TS4", "IP31", "EX2", "GL17") ))

#Write HFSS23 file for merging with HFSS23 from code below.
#2,421,692 obs of 52 variables
write_parquet(HFSSFINAL23c, "HFSSFINAL23.parquet")


#######################################
#Append HFSSFINAL23 onto HFSS22 final##
#######################################
#If not already open use code to open HFSSFINAL22 and 23 files
HFSSFINAL22 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL22.parquet")
HFSSFINAL23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL23.parquet")
combined_rfs <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/combined_rfs.parquet")

#Append HFSSFINAL22 and HFSSFINAL23
HFSSFINAL22_23 = rbind(HFSSFINAL22, HFSSFINAL23)

#rename category variable descriptors
#colnames(HFSSFINAL22_23) [colnames(HFSSFINAL22_23) %in% c("Area", "Market","Sector","Submarket","Extended")] <- c("area_desc", "market_desc", "mktsector_desc", "submkt_desc",  "extended_desc")
#colnames(Kantar_regcats) [colnames(Kantar_regcats) %in% c("RST 4 Market", "RST 4 Sub Market", "RST 4 Extended")] <- c("market_desc", "submkt_desc", "extended_desc")


###################################################
#ADD UOM data for volume conversion################
###################################################

#Join uom files to master file using RF and VF as linkage variables -this will make it easier to join this file to the combined master file in the last stage
#Check if rst_product files  in 2022 and 2023 are the same
#They are the same
#rst_uom_2022 <- read_csv("2022 Purchase Data/rst_uom.csv")
#rst_uom_2023 <- read_csv("2023 Purchase Data/rst_uom.csv")

#rst_uom_differences <- are_equal <- isTRUE(all.equal(rst_uom_2022, rst_uom_2023, check.attributes = FALSE)) # both the 2022 and 2023 files are the same



###################################################
#Link uom and rf files to master file
##################################################

#Link combined_rfs to HFSS
HFSSFINAL22_23 <- HFSSFINAL22_23 %>%
  left_join(combined_rfs, by=c("prodcode" = "Product"))

#open uom file for 2023 (same as for 2022)
rst_uom23 <- fread("2023 Purchase Data/rst_uom.csv")
HFSSFINAL22_23$VF <- as.numeric(HFSSFINAL22_23$VF)
rst_uom23$VF <- as.numeric(rst_uom23$VF)


#Link rst_uom_2023 to the combined_rfs file
#Link combined_rfs to HFSS (use rst_uom23 - both years are the same)
HFSSFINAL22_23 <- HFSSFINAL22_23 %>%
  left_join(rst_uom23, by=c("RF_Title" = "RF_TITLE", "VF"))

####################################################
#LINK HFSS REG CATEGORIES FROM KANTAR WITH MAIN FILE
####################################################

#Kantar regcats - where all of a submarket is included in the regs then the extended category is not listed
#Extended is only listed where certain extended categories are included within a submarket

#colnames(Kantar_regcats) [colnames(Kantar_regcats) %in% c("RST 4 Market", "RST 4 Sub Market", "RST 4 Extended")] <- c("market_desc", "submkt_desc", "extended_desc")

#Some categories need to be linked on 2 IDs and some on 3IDs
#HFSSFINAL22_23 <- rename(HFSSFINAL22_23,`RST 4 Extended` = `RST 4 Extended.x`)

# Join Kantar regcats to HFSSFINAL22_23 
HFSSFINAL22_23a <- HFSSFINAL22_23 %>%
  left_join(Kantar_regcats %>% filter(!is.na(Kantar_regcats$`RST 4 Extended`)), by = c("RST 4 Market", "RST 4 Sub Market", "RST 4 Extended")) %>% # Join where ID3 is NOT NA in df2
  left_join(Kantar_regcats %>% filter(is.na(Kantar_regcats$`RST 4 Extended`)), by = c("RST 4 Market", "RST 4 Sub Market")) %>%  # Join where ID3 is NA in df2%
  mutate(HFSS_reg_cat = coalesce(`HFSS Category.x`, `HFSS Category.y`)) 

#Remove columns that aren't needed
HFSSFINAL22_23 <- HFSSFINAL22_23 %>% select(- `HFSS Category.y`, -`RST 4 Extended.y`, - Amendment.y)

#Reorder and prune columns
# HFSSFINAL22_23 <- HFSSFINAL22_23 %>% select(1,2,19,20,3:16,27,17,18,21:26,28:33,36:40,48, 49,52,34,35,69,64,50,46,47,66:68,41:45,53:65)


#Code an HFSS status column for HFSS categories
#1 HFSS_INREGCATS: Products with an NPM score of 4 or 1 + AND with an HFSS regulation category applied
#2 HFSS_NOREGS: Products with an NPM score of 4 or 1 + BUT not covered or excluded from any of the 13 regulation categories
#3 NOT_HFSS_INREGCATS: Products with an NPM score of less than 4 or 1 (drinks) AND technically covered under one of the 13 regulation categories
#4 NOT_HFSS_NOREGS: Products with an NPM score of less than 4 or 1 (drinks) BUT not covered or excluded from the 13 regulation categories

#1 HFSS_INREGCATS: Products with an NPM score of 4 or 1 + AND with an HFSS regulation category applied

#HFSSFINAL22_23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL22_23.parquet")


HFSSFINAL22_23B <- HFSSFINAL22_23 %>%
  mutate(HFSS_STATUS = case_when(
    NPM >= 1 & `Sector` == 'Take Home Soft Drinks' & !is.na(`HFSS_reg_cat`) ~ 'HFSS_INREGCATS',# soft drinks in reg category
    NPM >= 1 & `Sector` == 'Chilled Drinks' & !is.na(`HFSS_reg_cat`) ~ 'HFSS_INREGCATS',# soft drinks in reg category
    NPM >= 1 & `Sector` == 'Hot Beverages' & !is.na(`HFSS_reg_cat`) ~ 'HFSS_INREGCATS',# soft drinks in reg category
    NPM >= 4 & `Sector` != 'Take Home Soft Drinks' & !is.na(`HFSS_reg_cat`) ~ "HFSS_INREGCATS",# cat 1 foods in reg cats
    NPM >= 4 & `Sector` != 'Chilled Drinks' & !is.na(`HFSS_reg_cat`) ~ "HFSS_INREGCATS",# cat 1 foods in reg cats
    NPM >= 4 & `Sector` != 'Hot Beverages' & !is.na(`HFSS_reg_cat`) ~ "HFSS_INREGCATS",# cat 1 foods in reg cats
    NPM >= 1 & `Sector` == 'Take Home Soft Drinks' & is.na(`HFSS_reg_cat`)  ~ "HFSS_EXEMPT", # cat 2 HFSS soft drinks with high NPM score but exempt under the regs e.g. fruit juice without added sugar
    NPM >= 1 & `Sector` == 'Chilled Drinks' & is.na(`HFSS_reg_cat`)  ~ "HFSS_EXEMPT", # cat 2 HFSS soft drinks with high NPM score but exempt under the regs e.g. fruit juice without added sugar
    NPM >= 1 & `Sector` == 'Hot Beverages' & is.na(`HFSS_reg_cat`)  ~ "HFSS_EXEMPT", # cat 2 HFSS soft drinks with high NPM score but exempt under the regs e.g. fruit juice without added sugar
    NPM >= 4 & `Sector` != 'Take Home Soft Drinks' & is.na(`HFSS_reg_cat`)  ~ "HFSS_EXEMPT", # cat 2 HFSS food but exempt under the regs e.g. pies and pastries
    NPM >= 4 & `Sector` != 'Chilled Drinks' & is.na(`HFSS_reg_cat`)  ~ "HFSS_EXEMPT", # cat 2 HFSS food but exempt under the regs e.g. pies and pastries
    NPM >= 4 & `Sector` != 'Hot Beverages' & is.na(`HFSS_reg_cat`)  ~ "HFSS_EXEMPT", # cat 2 HFSS food but exempt under the regs e.g. pies and pastries
    NPM   <1 & `Sector` == 'Take Home Soft Drinks'& !is.na(`HFSS_reg_cat`) ~ "NOT_HFSS_INREGCATS", # cat 3 NON HFSS soft drinks with low NPM score and exempt under the regs
    NPM   <1 & `Sector` == 'Chilled Drinks' & !is.na(`HFSS_reg_cat`) ~ "NOT_HFSS_INREGCATS", # cat 3 NON HFSS soft drinks with low NPM score and exempt under the regs
    NPM   <1 & `Sector` == 'Hot Beverages'  & !is.na(`HFSS_reg_cat`) ~ "NOT_HFSS_INREGCATS", # cat 3 NON HFSS soft drinks with low NPM score and exempt under the regs
    NPM   <4 & `Sector` != 'Take Home Soft Drinks' & !is.na(`HFSS_reg_cat`) ~ "NOT_HFSS_INREGCATS", # cat 3 non HFSS foods exempt from regs
    NPM   <4 & `Sector` != 'Chilled Drinks' & !is.na(`HFSS_reg_cat`) ~ "NOT_HFSS_INREGCATS", # cat 3 non HFSS foods exempt from regs
    NPM   <4 & `Sector` != 'Hot Beverages' & !is.na(`HFSS_reg_cat`) ~ "NOT_HFSS_INREGCATS", # cat 3 non HFSS foods exempt from regs
    NPM   <1 & `Sector` == 'Take Home Soft Drinks' & is.na(`HFSS_reg_cat`)  ~ "NOT_HFSS_NOREGS", # cat 4 non HFSS drinks not in a reg category
    NPM   <1 & `Sector` == 'Chilled Drinks' & is.na(`HFSS_reg_cat`)  ~ "NOT_HFSS_NOREGS", # cat 4 non HFSS drinks not in a reg category
    NPM   <1 & `Sector` == 'Hot Beverages' & is.na(`HFSS_reg_cat`)  ~ "NOT_HFSS_NOREGS", # cat 4 non HFSS drinks not in a reg category
    NPM   <4 & `Sector` != 'Take Home Soft Drinks' & is.na(`HFSS_reg_cat`)  ~ "NOT_HFSS_NOREGS", #cat 4 non HFSS foods not in a reg cat
    NPM   <4 & `Sector` != 'Chilled Drinks'& is.na(`HFSS_reg_cat`)  ~ "NOT_HFSS_NOREGS", #cat 4 non HFSS foods not in a reg cat
    NPM   <4 & `Sector` != 'Hot Beverages' & is.na(`HFSS_reg_cat`)  ~ "NOT_HFSS_NOREGS", #cat 4 non HFSS foods not in a reg cat
  ))




#Drop erroneous variables
HFSSFINAL22_23 <- HFSSFINAL22_23 %>% select(-'HFSS Category.x', -'Amendment.x')

write_parquet(HFSSFINAL22_23, "HFSSFINAL22_23.parquet")












