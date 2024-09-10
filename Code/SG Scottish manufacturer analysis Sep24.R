####################################################
#SG ANALYSIS#
#What proportion of total HFSS sales in 2023 (by price and volume) were made by Scottish manufacturers  #
#Written by Elaine Tod 3rd September 2024
####################################################

#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readr)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")



#PART 1:LOOK AT THE DISTRIBUTION OF TOTAL HFSS SALES BY MANUFACTURER IN 2023

#Open linked data file for 2023
HFSSFINAL23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL23.parquet")

#Link manufacturer data to HFSSFINAL22_23 data file uing Catriona's 'rf_markets_combined.parquet file
rf_markets_combined <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/rf_markets_combined.parquet")

#Join panel and purchase data with NPM and HFSS data (NPM_2024P2)
SG_dataset <- dplyr::left_join(HFSSFINAL23, rf_markets_combined, by=c("prodcode" = "Product"))


#TEMPORARY CODE - Change amtspent to numeric
SG_dataset$amtspent <- as.numeric(SG_dataset$amtspent)

#QA CHECK
#2,421,692 observations of 54 variables

#Write data file for SG analysis
write_parquet(SG_dataset, "SG_dataset.parquet")


################################
#SYNTAX TO CREATE TABLE METRICS#
################################

#Open SG data file
SG_dataset <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/SG_dataset.parquet")

#Create table showing total amount spent by manufacturer for all HFSS products 
SGTableA <- SG_dataset %>%
  group_by(Manufacturer) %>%
  summarise (Total_spend = sum(amtspent[`HFSS` == 1])) %>%
  view

#1,967 valid manufacturers
#1 which is NA - remove



#Create variable to show percentage of total purchases (monetary pounds) for each of the 1,967 manufacturers in 2023
  mutate(percentage = Total_spend / sum(Total_spend) * 100) %>%
  view

#Create cumulative percentage variable to help with cut off of manufacturers (Y%)


  
  


 


  write_csv(TableA, filename="SGTableA.csv")
                                         
summarydf <- phenology %>% group_by......(as above)

write.csv(summarydf, filename="yourfilenamehere.csv")          
                                         





prob = c(0.25, 0.75))
group_by(SG_dataset, Manufacturer) %>% mutate(percent = amtspent/sum(amtspent))
  
  
SG_dataset %>%
  group_by(Manufacturer) %>%
  mutate(Perc_total_HFSS_sales = amtspent / sum(amtspent) * 100)
  
  



#Create bar chart showing:
# x axis manufacturers
# y axis total monetary sales
# restrict to HFSS = 1

SG_dataset %>%
  filter(HFSS == 0)
ggplot(SG_dataset, aes(x=Manufacturer, y=amtspent))
  
  
  ggplot(SG_datast,aes(Manufacturer,amtspent))+
  {if(Switch)geom_hline(yintercept=15)}+
  geom_point() 

