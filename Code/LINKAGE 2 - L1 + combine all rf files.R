

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



#STAGE 1: clean and combine rf files for linkage to master file later on
#Pivot rf title to a new column so that every rows has an indicator of which rf category it belongs to
#Catriona's code to add function - RF_title across all 291 rf files

#Part 1: Create function to add columns with rf title and RF number
#Read multiple RF files sequentially
RF_title_func <- function(f) {
  title <- read_csv(f, n_max = 1, col_select = 6) %>% 
    rf_num <- read_csv(f, n_max = 1, col_select = 5) %>%                  
    names(.) # Turn the column name into a variable

}


read_csv(f, skip = 1) %>%
  mutate(RF_Title = title,
         Product = as.numeric(Product)) %>%
  select(any_of(c("rf_num", "RF_Title", "Product", "Product Desc", "Branded/Private Label", "Holding Company", "Manufacturer", "Range/Trading Company", "Brand", "Packaging", "Pack Type", "Shop Aisle",  "RST 4 Trading Area", "RST 4 Market", "RST 4 Sub Market", "RST 4 Extended"))) 


#Part 2: Listing the rf files and running the RF_title_func
#See notes on these above
#R might warn you that there's a parsing error, but I think that's just because of the way we've extracted the title. The code still seems to work ok.

rf_files_test <- list.files("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/2023 Purchase Data/product master/", full.names = TRUE)

all_rfs_test <- lapply(rf_files_test, RF_title_func)



#Create function to add the RF number as a column
RF_func <- function(f) {
  RF <- read_csv(f, n_max = 1, col_select = 5) %>% # Only read in the RF number
    names(.) # Turn the column name into a variable
  
  
read_csv(f, skip = 1) %>%
    mutate(RF_Title = title,
           Product = as.numeric(Product)) %>%
    select(any_of(c("RF", "RF_Title", "Product", "Product Desc", "Branded/Private Label", "Holding Company", "Manufacturer", "Range/Trading Company", "Brand", "Packaging", "Pack Type", "Shop Aisle",  "RST 4 Trading Area", "RST 4 Market", "RST 4 Sub Market", "RST 4 Extended"))) 
  
}
  
rf_files_test <- list.files("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/2023 Purchase Data/product master/", full.names = TRUE)

all_rfs_test <- lapply(rf_files_test, RF_func)
  


#Part 3: Combining the rf files into one dataframe
#The all_rfs object is a list of different dataframes. To combine these together, we can use the code below.
#If the dataframes contain different columns (e.g. Because you've used any_of() above), it will keep all columns and add NA for dataframes without those columns.

combined_rfs <- do.call(bind_rows, all_rfs_test)

write_parquet(combined_rfs, "combined_rfs.parquet")

combined_rfs <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/combined_rfs.parquet")

