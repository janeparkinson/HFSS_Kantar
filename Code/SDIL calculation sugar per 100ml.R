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

##################################
#SDIL calculation sugar per 100ml#
#Written by Elaine Tod           #
#28th October 2025               #
##################################

#######################################################################################################################################
#The aim of this code is to calculate the sugar content per 100ml of a drinks products i.e. whether it is covered by the SDIL
#Drinks covered by the SDIL (5+g of sugar per 100mL AND pre-packaged - THIS WILL LOWER TO 4.5G FROM 1ST JANUARY 2028) AND have an NPM score of 1+ are in scope for the HFSS price promotion restrictions
#Includes flavoured milk products as these are due to fall under the SDIL FROM 1ST JNAUARY 2028
#Includes powders, syrups, pods, cordials and ready to drink products
#######################################################################################################################################


#####################################################################
#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readr)
library(readxl)
library(data.table) # For 'fread' function to read in CSVs efficiently
library(stringr)
library(writexl)
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")
#####################################################################

#Inferential analysis - exploration
#open datasets
HFSSFINAL_SIMD22_23_new <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL_SIMD22_23_new.parquet")

#######################################################
#IDENTIFYING SDIL ELIGIBLE DRINKS

#Summary of stages below:
#1. Create dataframe with minimum and maximum sugar content of HFSS in scope purvhases (sugar measured in kg per purchase)
#2. Create dataframe which only includes regulation category "prepared soft drinks"
  #2a. "Prepared Soft Drinks" regulation category includes 14 categories of drinks from the validation field variable. These are listed below:
  #2a.   1 COFFEE-INSTANT                
        #2 DRINKING CHOC+COCOA           
        #3 FRUIT JUICE & FRUIT DRINKS    
        #4 FRUIT SQUASHES                
        #5 MILKSHAKE MIXES  **EXEMPT**             
        #6 SLIM AIDS+OPTIMAL HEALTH      
        #7 SOFT DRINKS-COLA BOTTLES      
        #8 SOFT DRINKS-COLA CANNED       
        #9 SOFT DRINKS-CRBNTD FLVRS BTTLS
        #10 SOFT DRINKS-CRBNTD FLVRS CNND 
        #11 SOFT DRINKS-FRT JCE/DRNKS     
        #12 SOFT DRINKS-MIXERS            
        #13 SOFT DRINKS-MLKSHKS (RDY MADE) **EXEMPT**
        #14 SOFT DRINKS-SHANDIES  
#We need to be able to isolate drinks with 5g+ of sugar per 100ml under the current definition of the SDIL
#We need to remnove Bottled milkshakes, Flavoured milks, Sweetened yoghurt drinks, Ready-to-drink coffee drinks which are exempt under the current definition of the SDIL (this will chaneg in October 2028)
#

# Create dataframe with minimum and maximum sugar content in HFSS 'in scope' products
# sugar (measured in kg) per purchase (may be one or more of the same item in a single purchase)
#SDIL liable drinks have 5+g sugar per 100ml (to change to 4.5g from 1st Jan 2028)
#range 0-5.22kg per purchase (5.22g relates to a purchase of 15 x 600g tubs of Quality Street)
sugar_range <- HFSSFINAL_SIMD22_23_new %>%
  filter(HFSS_STATUS == "HFSS_INREGCATS") %>%
  summarise(min_sugar = min(`Sugar KG`, na.rm = TRUE),
            max_sugar = max(`Sugar KG`, na.rm = TRUE),
            range_sugar = max(`Sugar KG` , na.rm = TRUE) - min(`Sugar KG`, na.rm = TRUE))

print(sugar_range)

#Subset master dataframe to look at drinks included in Kantar's HFSS category
subset_KWP <- HFSSFINAL_SIMD22_23_new %>%
  filter(REG_CAT13 == "Prepared Soft Drinks") %>%
  select(panel_id, purchnum, period, pcksbought,barcode, `Pack Type`, VF_TITLE.x, NPM, HFSS, REG_CAT13, HFSS_STATUS, `Energy KJ`, `Sugar KG`, `Sodium KG`, `Fat KG`, `Saturates KG`, `Fibre KG`, `Protein KG`, PRODUCT_DESC, `Prod desc drinks`, Area, Market, Sector, Submarket, Extended, `227 - Low Sugar/Calorie/Fat.x`)

#Extract single pack size from product description
#PRODUCT_DESC is complete but product_desc_drinks is not
subset_KWP <- subset_KWP %>%
  mutate(
    pack_size = str_extract(PRODUCT_DESC, "\\d+(\\.\\d+)?\\s*(M|ml|ML|l|LT|G|GM)$"),
    multipack_multiplier = as.numeric(str_extract(PRODUCT_DESC, "\\d+(?=[xX])")),
    multipack_multiplier = if_else(is.na(multipack_multiplier), 1, multipack_multiplier),  # default to 1 if missing
  )


subset_KWP <- subset_KWP %>%
  mutate(
    # Extract numeric part from pack_size
    pack_size_num = as.numeric(str_extract(pack_size, "\\d+(\\.\\d+)?")),
    
    # Extract unit type from pack_size
    pack_unit = str_extract(pack_size, "(?i)(ML|M|LT|L|G|GM)"),
    
    # Convert to millilitres
    # doesn't include powders/cordials/syrups that need made up with water or milk etc.
    pack_sizeML = case_when(
      pack_unit %in% c("ml", "ML", "M") ~ pack_size_num,
      pack_unit %in% c("l", "L", "lt", "LT") ~ pack_size_num * 1000,
      TRUE ~ NA_real_
    )
  )

#SUGAR CONTENT IS GIVEN IN KG PER PURCHASE IN THE KANTAR DATASET. NEED TO WORK OUT SUGAR CONTENT IN GRAMS PER 100ML OF DRINKS PRODUCT 

#PART 1
#HOW MUCH SUGAR CONTENT IN ONE PACK INTENDED FOR SALE 'AS IS' (MAY ME SINGLE ITEM OR SOLD AS MULTIPACKS OF DRINKS) E.G. 6 PACK OF COCA-COLA

#Multiply single item/unit size e.g. one can of coke = 330ml by number of items in one PACK e.g 6 pack of coke to give total volume of one multi-pack product e.g. 6 x 330ml = 1980ml per multipack
subset_KWP <- subset_KWP %>%
  mutate(
    pack_sizeML = pack_sizeML * multipack_multiplier
  )

#PART 2
#Multiply TOTAL PACK VOLUME (ML) by the number of packs purchased to get the total amount in ML for purchase e.g. 2 multipacks @1,000 ML = 2000ML
subset_KWP <- subset_KWP %>%
  mutate(
    totalpurchaseML = pack_sizeML * as.numeric(pcksbought)
  )

#PART 3
# Create variable converting sugar per total purchase in KG (as provided by Kantar) to sugar per total purchase in grams
subset_KWP <- subset_KWP %>%
  mutate(
    Sugar_GM_totalpurchase = `Sugar KG` * 1000
  )

#PART 4
# Calculate sugar (g) for 1 ml of product
subset_KWP <- subset_KWP %>%
  mutate(
    SugarGM_per1ml = Sugar_GM_totalpurchase/totalpurchaseML
  )

#PART 5
#Calculate sugar (g) per 100ml of drink
subset_KWP <- subset_KWP %>%
  mutate(
    SugarGM_per100ml = SugarGM_per1ml * 100
  ) 
#IDENTIFYING DRINKS WITH ADDED SUGAR (THROUGH PROCESS OF ELIMINATION)
#Derive variable for drinks with added sugar i.e. remove drinks which are marked NAS, SF or S/F For option 2 from study protocol i.e. exclude drinks likely to be natural sugar ONLY marked as no added sugar
 subset_KWP <- subset_KWP %>%
   mutate(
    drinks_added_sugar = case_when(
      !str_detect(
        PRODUCT_DESC,
        regex("\\b(?:NAS|SF|S/Fm|red\\.sgr)\\b", ignore_case = TRUE)
      ) ~ "added_sugar",
      TRUE ~ "NAS"
    )
  )

#DRINKS SUBJECT TO HFSS REGULATIONS
  
#Part one  
#Derive a variable for drinks which are SDIL (have added sugar AND 5+g OF TOTAL SUGAR) + NPM >=1
#This code selects products with added sugar, and with more than 5mg per 100ml TOTAL SUGAR and an NPM of 1+
#MAKE MILKSHAKES 'EXEMPT' IN HFSS_drinks variable

subset_KWP <- subset_KWP %>%
  mutate(
    HFSS_drinks = case_when(
      drinks_added_sugar == "added_sugar" &
        SugarGM_per100ml >= 5 &
        NPM >= 1 &
        !VF_TITLE.x %in% c("MILKSHAKE MIXES",
                           "SOFT DRINKS-MLKSHKS (RDY MADE)") ~ "HFSS_drinks",
      TRUE ~ NA_character_
    )
  )


#Left-join SDIL drinks variable to master dataframe.
#This allows drinks subject to the current SDIL to be identified
HFSSFINAL_SIMD22_23_new <- HFSSFINAL_SIMD22_23_new %>%
  left_join(
    subset_KWP %>%
      select(panel_id, purchnum, period, HFSS_drinks),
    by = c("panel_id", "purchnum", "period")
  )






###########################
#SG energy drinks analysis# - IGNORE FOR NOW
###########################

#Derive energy drink variable


#VERSION 1 - BASED ON ENERGY BEING ON THE PRODUCYT DESCRIPTION
subset_KWP <- subset_KWP %>%
  mutate(
    # Match ENERGY / ENRGY / ENRG as complete tokens (case-insensitive)
    energy_drink = case_when(
      str_detect(PRODUCT_DESC, regex("\\bEN(?:ERGY|RGY|RG)\\b", ignore_case = TRUE)) ~ "energy_drink",
      TRUE ~ "std_drink"
    )
  ) 

#OR VERSION 2 - BASED ON ENERGY OR SPORTS BEING IN THE EXTENDED DESCRIPTOR COLUMN (MORE EXTENSIVE)

#Derive variable that identifies drinks that are listed as sport or energy drinks in the Extended column
#"Canned Oth Regular Sport+Energ" "Ambient One Sho Sports Drinks"  "Bottled Ot Regular Sport+Energ" "Ambnt Fru Ambient J Sports Dri"
subset_KWP <- subset_KWP %>%
  mutate(
    sport_energydrink = case_when(
      str_detect(Extended, regex("\\b(Sport|Energ)\\w*", ignore_case = TRUE)) ~ "sport_energydrink",
      TRUE ~ "std_drink"
      )
  )


#QUESTION 1: What proportion of sweetened beverage (SSB) purchases are energy drinks?

#TOTAL SALES VOLUME(L) ENERGY DRINKS AS A PROPORTION OF ALL SSB DRINKS IN 2022 and 2023

#Denominator - total volume of ssb sold in 2022 and 2023
total_SSB_salesvol_litres <- subset_KWP %>%
  filter(drinks_added_sugar == "added_sugar") %>%
  summarise(total_SSB_litres = sum(totalpurchaseML, na.rm = TRUE) /1000) %>%
  pull(total_SSB_litres)

#Numerator - total volume of energy drinks sold in 2022 and 2023

#VERSION 1
total_energyvol_litres <- subset_KWP %>%
  filter(energy_drink == "energy_drink") %>%
  summarise(total_energy_litres = sum(totalpurchaseML, na.rm = TRUE) /1000) %>%
  pull(total_energy_litres)


#VERSION 2
#Total sales volume(L) of energy drinks purchased in 2022 and 2023 (raw panel)?
total_sports_energyvol_litres <- subset_KWP %>%
  filter(sport_energydrink == "sport_energydrink") %>%
  summarise(total_energysports_litres = sum(totalpurchaseML, na.rm = TRUE) /1000) %>%
  pull(total_energysports_litres)


#energy drinks as a percentage of total SSB drinks sales volume (L)


#VERSION 1
#6.8% of SSB volume sales were classified as energy drinks in 2022 and 2023
percentage_energySSBv1 <- (total_energyvol_litres/total_SSB_salesvol_litres) *100


#VERSION 2
#11.5% of SSB volume sales were classified as energy or sports drinks in 2022 and 2023
percentage_energySSBV2 <- (total_sports_energyvol_litres/total_SSB_salesvol_litres) *100


#Q2
#TOTAL SALES VOLUME(L) ENERGY DRINKS AS A PROPORTION OF ALL SOFT DRINKS 
#What proportion of TOTAL SOFT DRINK VOLUME SALES are energy drinks

#energy drinks as a percentage of total soft drink sales volume (L)

#Total sales volume of soft drinks 2022 and 2023 combined (L)
total_volsoftdrinks_litres <- subset_KWP %>%
  summarise(total_volsoftdrinks = sum(totalpurchaseML, na.rm = TRUE)/1000) %>%
  pull(total_volsoftdrinks)

#energy drinks as a proportion of ALL soft drinks sales (sales volume L)


#VERSION 1
#5.2% of TOTAL soft drinks sales were energy drinks in 2022 and 2023
percentage_totalsoftdrinks_energyV1 <- (total_energyvol_litres/total_volsoftdrinks_litres) *100


#VERSION 2
#8.7% of TOTAL soft drinks sales were energy drinks in 2022 and 2023
percentage_totalsoftdrinks_energyV2 <- (total_sports_energyvol_litres/total_volsoftdrinks_litres) *100








