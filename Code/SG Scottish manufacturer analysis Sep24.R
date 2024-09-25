####################################################
#SG ANALYSIS#
#What proportion of total HFSS sales in 2023 (by price and volume) were made by Scottish manufacturers  #
#Written by Elaine Tod 3rd September 2024
####################################################

#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(tidyr)
library(janitor)
library(dplyr)
library(readr)
library(data.table) # For 'fread' function to read in CSVs efficiently                                        # Install xlsx R package                                                # Load xlsx R package to RStudio
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")


###########################################################################
#PART 1:CREATE SG DATASET 2023 (PURCHASES WITH MANUFACTURER AND HFSS INFO)#
###########################################################################

HFSSFINAL23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL23.parquet")#Open linked data file for 2023
prodnames <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/npm_hfss_prod_names.parquet")

#Kantar_regcats <- fread("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/PHS HFSS Itemisation Breakdown amended.csv")
#IGNORE - KEPT IN CASE NEEDED THEN WILL DELETE
#files <- list.files(path = "2023 Purchase Data/product master", full.names = TRUE)#Create combined RFfile
#dataframes <- lapply(files, function(f){
  #fread(f, skip = 1, header = TRUE) %>%
    #select(any_of(c("Product", "Product Desc", "Manufacturer", "Brand", "RST 4 Market", "RST 4 Sub Market", "RST 4 Extended", "Low Sugar/Calorie/Fat")))
#}
#)

#rf_combined_SG <- do.call(bind_rows, dataframes)

  #write_parquet(rf_combined_SG, "rf_combined_SG.parquet")

#rf_combined_SG <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/rf_combined_SG.parquet")#Link manufacturer data to HFSSFINAL22_23 data file using a version of Catriona's 'rf_markets_combined.parquet file - saved separately.


#QA - remove 47 products with missing manufacturer info
#rf_combined_SG <- rf_combined_SG[-which(rf_combined_SG$Manufacturer == ""), ]


#18/09/24 - START HERE

#Get Catriona's file with HFSS categories
prodnames <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/npm_hfss_prod_names.parquet")

#Merge master purchase dataset for 2023 with HFSS regulation category file
SG_dataset <- dplyr::left_join(HFSSFINAL23,prodnames, by=c("prodcode" = "PRODUCT"))
             
#Tidy up file
SG_dataset <- subset(SG_dataset, select = -c(28, 57:60, 63:65, 68))#Delete duplicate variables
SG_dataset <- SG_dataset[  ,c(1:28,53,29:44,59,45:52,54:58)] #re-order variables

#rename category variable descriptors
colnames(SG_dataset) [colnames(SG_dataset) %in% c("Area", "Market","Sector","Submarket","Extended")] <- c("area_desc", "market_desc", "mktsector_desc", "submkt_desc",  "extended_desc")

SG_dataset <- SG_dataset %>% mutate_at(c('amtspent'), as.numeric)#Change amtspent to numeric


#QA CHECK
#2,421,692 observations of 59 variables

#Remove products which are not included in the regulation categories and which are not HFSS = 1
SG_dataset <- SG_dataset %>% drop_na(hfss_category)#Drop non-HFSS category products (left with 765,267 purchases for 2023)
SG_dataset <- SG_dataset[SG_dataset$HFSS.x != 0, ]#Drop products with an HFSS score of 0 within the regulation categories (left with 474,844 HFSS purchases falling into the reg categories for 2023)






#Write data file for SG analysis
write_parquet(SG_dataset, "SG_dataset.parquet")


########################################
#PART 2: SYNTAX TO CREATE TABLE METRICS#
#######################################

#SUMMARY STATISTICS


SGTableA <- SG_dataset%>%        
  group_by(Manufacturer) %>% 
  summarise(Spend= sum(amtspent))

#Link SG file with manufacturer addresses to HFSS/reg product file
Manufacturer_addresses <- fread("Manufacturers_registered_UK_office_address.csv")
SGTableA <- dplyr::left_join(SGTableA,Manufacturer_addresses, by=c("Manufacturer"))


SGTableA = SGTableA %>% 
  mutate(percent = (Spend/sum(Spend)*100))#Calculate percentage of total spend by manufacturer SGTableA %>%
SGTableA[,'percent']=round(SGTableA[,'percent'],2)

SGTableA <- SGTableA %>% #Calculate cumulative percentage of total spend by manufacturer
  arrange(-Spend) %>% 
  mutate(cumperc=cumsum(percent))
SGTableA[,'cumperc']=round(SGTableA[,'cumperc'],2)

summary(SGTableA)


#Write CSV file for charts in Excel
write_csv(SGTableA, "SGTableA.csv")



#Part 2 run above but with volume of sales as the measure of HFSS purchases - MAY NOT NEED TO DO THIS - 
#Open SG data file
SG_dataset <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/SG_dataset.parquet")

