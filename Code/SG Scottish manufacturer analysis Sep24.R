####################################################
#SG ANALYSIS#
#What proportion of total HFSS sales in 2023 (by price and volume) were made by Scottish manufacturers  #
#Written by Elaine Tod 3rd September 2024
####################################################

#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(tidyr)
library(dplyr)
library(readr)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")


###########################################################################
#PART 1:CREATE SG DATASET 2023 (PURCHASES WITH MANUFACTURER AND HFSS INFO)#
###########################################################################

HFSSFINAL23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL23.parquet")#Open linked data file for 2023

files <- list.files(path = "2023 Purchase Data/product master", full.names = TRUE)#Create combined RFfile

dataframes <- lapply(files, function(f){
  fread(f, skip = 1, header = TRUE) %>%
    select(any_of(c("Product", "Product Desc", "Manufacturer", "Brand", "RST 4 Market", "RST 4 Sub Market", "RST 4 Extended", "Low Sugar/Calorie/Fat")))
}
)

rf_combined_SG <- do.call(bind_rows, dataframes)

write_parquet(rf_combined_SG, "rf_combined_SG.parquet")


rf_combined_SG <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/rf_combined_SG.parquet")#Link manufacturer data to HFSSFINAL22_23 data file using a version of Catriona's 'rf_markets_combined.parquet file - saved separately.


#QA - remove 47 products with missing manufacturer info
rf_combined_SG <- rf_combined_SG[-which(rf_combined_SG$Manufacturer == ""), ]

#Join panel and purchase data with NPM and HFSS data (NPM_2024P2)
SG_dataset <- dplyr::left_join(HFSSFINAL23, rf_combined_SG, by=c("prodcode" = "Product"))


#Change amtspent to numeric
SG_dataset$amtspent <- as.numeric(SG_dataset$amtspent)
SG_dataset <- SG_dataset %>% mutate_at(c('amtspent'), as.numeric)


#QA CHECK
#2,421,692 observations of 54 variables

#Drop non-HFSS products
SG_dataset <- SG_dataset[SG_dataset$HFSS != 0, ]

#Write data file for SG analysis
write_parquet(SG_dataset, "SG_dataset.parquet")


########################################
#PART 2: SYNTAX TO CREATE TABLE METRICS#
#######################################


#Open SG data file
SG_dataset <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/SG_dataset.parquet")

#TEMP CODE QA
#SG_dataset <- SG_dataset %>%
  #filter(Manufacturer == "108 Foods Sarl" )


#SUMMARY STATISTICS
SGTableA = SG_dataset%>%        
  group_by(Manufacturer) %>% 
  summarise(Spend= sum(amtspent [`HFSS` == 1]))#Create table showing total amount spent on HFSS products in 2023,  by manufacturer

summary(SGTableA)

SGTableA_clean = SGTableA %>% drop_na(Spend) # remove one blank manufacturer

summary(SGTableA_clean)
            
SGTableA_clean = SGTableA_clean %>% 
  mutate(percent = (Spend/sum(Spend)*100))#Calculate percentage of total spend by manufacturer SGTableA %>%

SGTableA_clean <- SGTableA_clean %>% #Calculate cumulative percentage of total spend by manufacturer
  arrange(-Spend) %>% 
  mutate(cumperc=cumsum(percent))

#Write CSV file for charts in Excel
write_csv(SGTableA_clean, "SGTableA_clean.csv")
