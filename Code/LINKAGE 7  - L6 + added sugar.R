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

#Soft drinks with added sugar investigation
#Written by Elaine 09/01/25

#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readr)
library(readxl)
library(data.table) # For 'fread' function to read in CSVs efficiently
library(writexl)
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")


#open datasets
Kantar_sugar <- fread("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/Low Sugar Calorie Fat Attribute Information - Kantar.csv")
HFSSFINAL_SIMD22_23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL_SIMD22_23.parquet")
NPMHFSSfile <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/all_periodsproductsNPM.parquet")

#Rename Product description variable from Kantar sugar file to "Product desc drinks"
Kantar_sugar <- Kantar_sugar %>% rename("Prod desc drinks" = "Product Desc")

#Join Kantar drink sugar status file to master dataset
HFSSFINAL_SIMD22_23 <- HFSSFINAL_SIMD22_23 %>%
  left_join(Kantar_sugar, by=c("prodcode"))

#Tidy dataset
rm(HFSSFINAL_SIMD22_23$`227 - Low Sugar/Calorie/Fat.y`)
HFSSFINAL_SIMD22_23 <- subset(HFSSFINAL_SIMD22_23, select = -(`227 - Low Sugar/Calorie/Fat.y`))
HFSSFINAL_SIMD22_23 <- subset(HFSSFINAL_SIMD22_23, select = -(`Prod desc drinks.y`))
HFSSFINAL_SIMD22_23 <- HFSSFINAL_SIMD22_23 %>% rename(`Prod desc drinks` = `Prod desc drinks.x`)

write_parquet(HFSSFINAL_SIMD22_23, "HFSSFINAL_SIMD22_23.parquet")

# Create a subset of unique products purchased in 2022 and 2023
unique_products_slice <- HFSSFINAL_SIMD22_23 %>%
  group_by(prodcode) %>%
  slice(1) %>%
  ungroup()

View(unique_products_slice)

#Create a subset of unique drinks products purchased in 2022 and 2023
unique_products_slice_drinks <- subset(unique_products_slice,`227 - Low Sugar/Calorie/Fat.x` !="NA")
#unique_products_slice_drinks %>% filter(!is.na(unique_products_slice$`227 - Low Sugar/Calorie/Fat.x`))

#Create a subset of unique food products purchasedin 2022 and 2023
#unique_products_slice_food <- dplyr::filter(unique_products_slice, is.na(`227 - Low Sugar/Calorie/Fat`)) 
sugar_free_drinks <- subset(unique_products_slice_drinks, `227 - Low Sugar/Calorie/Fat.x` =="Sugar Free")
no_added_sugar_drinks <- subset(unique_products_slice_drinks, `227 - Low Sugar/Calorie/Fat.x` =="No Added Sugar")
low_calorie_drinks <- subset(unique_products_slice_drinks, `227 - Low Sugar/Calorie/Fat.x` =="Low Calorie")
diet_drinks <- subset(unique_products_slice_drinks, `227 - Low Sugar/Calorie/Fat.x` =="Diet")
standard_drinks <- subset(unique_products_slice_drinks, `227 - Low Sugar/Calorie/Fat.x` =="Standard")
regular_drinks <- subset(unique_products_slice_drinks, `227 - Low Sugar/Calorie/Fat.x` =="Regular")

##################
#Data exploration
variable_list <- data.frame(Variables = colnames(HFSSFINAL22_23_SIMD22_23))


#Explore Diet drink category (810 products)
unique(diet_drinks$`RST 4 Market`)

diet_drinks_not_HFSS <- subset(diet_drinks, HFSS ==0)

patterns <- c("SF", "S/F", "NAS", "ZERO", "R/S", "L/S", "FREE", "DT", "DIET", "LIGHT", "ZR", "XTRA", "LGT") #patterns to search
combined_patterns <- paste(patterns, collapse = "|")
diet_drinks_nomarker <- diet_drinks_not_HFSS[!grepl(combined_patterns, diet_drinks_not_HFSS$`Prod desc drinks`, ignore.case=TRUE),]


#Explore No added sugar drink category (469 products)
unique(HFSSFINAL22_23_noaddedsugardrinks$`RST 4 Market`)

HFSSFINAL22_23_noaddedsugardrinks %>%
  group_by(`Prod desc drinks`) %>%
  summarise(count = n_distinct(`Prod desc drinks`)) %>%
  print(n=500)


unique(subset(HFSSFINAL22_23_dietdrinks$`227 - Low Sugar/Calorie/Fat` =="Diet"))


#How many unique products are in each drinks category?


#Complete drinks file (all possible products in database)
# Diet                                   3005
# Low Calorie                            2783
# Low Sugar                                62
# No Added Sugar                         1635
# Non Barcoded Products                     3
# Regular                               16797
# Standard                               3688
# Sugar Free                              513

unique_counts <- Kantar_sugar %>%
  group_by(`227 - Low Sugar/Calorie/Fat`) %>%
  summarise(unique_values = n_distinct(prodcode))


  

# Print the result
print(unique_counts)

#4,832 unique pre-prepared soft drink products  sold in 22/23 (will be less than above)

# Diet                                    811
# Low Calorie                             531
# Low Sugar                                 4
# No Added Sugar                          469
# Regular                                2391
# Standard                                507
# Sugar Free                              119

unique_counts_purchased <- HFSSFINAL_SIMD22_23 %>%
  group_by(`227 - Low Sugar/Calorie/Fat`) %>%
  summarise(unique_values = n_distinct(prodcode))

# Print the result
print(unique_counts_purchased)




#Explore markets in each of the 6 drinks categories
#Cross-check NPM scores for each category

#What markets are in the low calorie drinks catgegory?
unique_lowcaldrinks <- subset(unique_products_slice, unique_products_slice$"227 - Low Sugar/Calorie/Fat" == "Low Calorie")
unique(unique_lowcaldrinks$`RST 4 Market`)




#What markets are in the regular/standard drinks category?
HFSSFINAL22_23_regulardrinks <- subset(unique_products_slice, unique_products_slice$"227 - Low Sugar/Calorie/Fat" == "Regular")
unique(HFSSFINAL22_23_regulardrinks$`RST 4 Market`)

#Regular drink markets
#Regular has 17 markets
#[1] "Bottled Other Flavours"    "Bottled Colas"             "Ambient One Shot Drinks"   "Canned Other Flavours"     "Ambnt Fruit/Yght Juc+Drnk"
#[6] "Canned Colas"              "Bottled Lemonade"          "Chilled Fruit Juice+Drink" "Ambient Flavoured Milk"    "Breakfast Cereals"        
#[11] "Chilled One Shot Drinks"   "Chilled Flavoured Milk"    "Total Fruit Squash"        "Bottled Shandies"          "Canned Lemonade"          
#[16] "Canned Shandies"           "Tonic Water"   


#What RST 4 submarkets are in regular drinks category
unique_submarkets_reg <- HFSSFINAL22_23_regulardrinks %>%
  filter(`RST 4 Market` == 'Bottled Other Flavours') %>%
  select(`RST 4 Sub Market`) %>%
  distinct()

# Print the unique values
print(unique_submarkets_reg)

#What RST 4 submarkets are in regular drinks category
unique_submarkets_reg <- HFSSFINAL22_23_regulardrinks %>%
  filter(`RST 4 Market` == 'Canned Other Flavours') %>%
  select(`RST 4 Sub Market`) %>%
  distinct()

# Print the unique values
print(unique_submarkets_reg)

#What RST 4 submarkets are in regular drinks category - chilled fruit juice + drink
unique_submarkets_reg <- HFSSFINAL22_23_regulardrinks %>%
  filter(`RST 4 Market` == 'Chilled Fruit Juice+Drink') %>%
  select(`RST 4 Sub Market`) %>%
  distinct()

# Print the unique values
print(unique_submarkets_reg)

#Chilled pure juice - likely to be 100% natural sugar? 413 products
#Chilled Juice Drinks - likely to be a mix of added and natural? 99 products
#Ambient Juice Drinks - likely to be a mix of added and natural? 1 product
unique_counts_chilled_fruit_juice_drink <- HFSSFINAL_SIMD22_23 %>%
  filter(`Shop Aisle` == `Take Home Soft Drinks`) %>%
  group_by(`RST 4 Sub Market`)%>%
  summarise(unique_values = n_distinct(prodcode)) 
  


#How many distinct products in each market for 'regular' drinks are HFSS?
# Ambient Flavoured Milk      128
# Ambient One Shot Drinks     303
# Ambnt Fruit/Yght Juc+Drnk   281
# Bottled Colas                45
# Bottled Lemonade             29
# Bottled Other Flavours      310
# Bottled Shandies              5
# Breakfast Cereals            24
# Canned Colas                 28
# Canned Lemonade              10
# Canned Other Flavours       371
# Canned Shandies               4
# Chilled Flavoured Milk      190
# Chilled Fruit Juice+Drink   443
# Chilled One Shot Drinks     112
# Tonic Water                   2
# Total Fruit Squash          103
HFSSFINAL22_23_regulardrinks %>%
  group_by(market_desc, HFSS) %>%
  summarise(count = n_distinct(`Prod desc drinks`)) %>%
  print(n=29)


#How many of these distinct products are HFSS ie. NPM of 1+
#Then I need to look at these to decide if we can discount any as having added sugar
HFSSFINAL22_23_regulardrinks %>%
  group_by(market_desc, HFSS) %>%
  summarise(count = n_distinct(`Prod desc drinks`))


#Standard drink markets
#Standard has 7 markets
#[1] "Soda Water"             "Mineral Water"          "Tonic Water"            "Ginger Ale"             "Bitter Lemon"          
#[6] "Mixers"                 "Bottled Other Flavours"

unique(standarddrinks$`RST 4 Market`)






#ignore for now

#What RST 4 submarkets are in regular drinks category
unique_submarkets_reg <- HFSSFINAL22_23_regulardrinks %>%
  filter(`RST 4 Market` == 'Bottled Colas') %>%
  select(`RST 4 Sub Market`) %>%
  distinct()

# Print the unique values
print(unique_submarkets_reg)

#What RST 4 submarkets are in regular drinks category
unique_submarkets_reg <- HFSSFINAL22_23_regulardrinks %>%
  filter(`RST 4 Market` == 'Ambient One Shot Drinks') %>%
  select(`RST 4 Sub Market`) %>%
  distinct()
