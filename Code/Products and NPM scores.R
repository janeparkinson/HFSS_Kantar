########################################################################################################################
##### Create a lookup file of HFSS category, product name and whether product is HFSS or not (according to NPM score)
##### Catriona Fraser
##### August 2024
########################################################################################################################

## Set up

library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readxl)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

## Read in data

# Joined 2022 and 2023 data

joined_data_2022 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/joined_data2022.parquet")
joined_data_2023 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/joined_data2023.parquet")

# NPM data
npm <- fread("NPM_2024P2.csv")

# Market, submarket, etc. codes and names data
product_area <- read_delim("2023 Purchase Data/product attributes/area.txt", col_names = c("area_code", "Area"))
product_market <- read_delim("2023 Purchase Data/product attributes/market.txt", col_names = c("market_code", "Market"))
product_sector <- read_delim("2023 Purchase Data/product attributes/mktsect.txt", col_names = c("sector_code", "Sector"))
product_submarket <- read_delim("2023 Purchase Data/product attributes/submark.txt", col_names = c("submarket_code", "Submarket"))
product_extended <- read_delim("2023 Purchase Data/product attributes/extended.txt", col_names = c("extended_code", "Extended"))

## Join 2022 and 2033 data together

# Join 2022 and 2023 data, select only relevant columns and make prodcode numeric

joined_data_22_23 <- rbind(joined_data_2022, joined_data_2023) %>%
  select(prodcode, area_code = area, market_code = market, sector_code = mktsector, submarket_code = submkt, extended_code = extended) %>%
  mutate(prodcode = as.numeric(prodcode))

# Check duplicate products

joined_data_duplicates <- joined_data_22_23 %>%
  group_by_all() %>%
  filter(n() > 1) %>%
  ungroup() %>%
  arrange(prodcode)

# Remove duplicates

joined_data_no_dups <- joined_data_22_23 %>%
  distinct()

# 82,658 non-duplicated products

# How many products in NPM file? Get the latest time period for which there is an NPM score

npm_latest <- NPM_2024P2 %>%
  group_by(PRODUCT) %>%
  slice(which.max(period))

# Over 670,000 products

## Merge with NPM file

npm_markets <- NPM_2024P2 %>%
  merge(joined_data_no_dups, by.x = "PRODUCT", by.y = "prodcode")

# Merge with market, submarket, etc. lookups
npm_markets_names <- npm_markets %>%
  merge(product_area, by = "area_code", all.x = TRUE) %>%
  merge(product_market, by = "market_code", all.x = TRUE) %>%
  merge(product_sector, by = "sector_code", all.x = TRUE) %>%
  merge(product_submarket, by = "submarket_code", all.x = TRUE) %>%
  merge(product_extended, by = "extended_code", all.x = TRUE)

# Only show latest NPM score

npm_markets_latest <- npm_markets_names %>%
  group_by(PRODUCT) %>%
  slice(which.max(period))

# Save file

write_parquet(npm_markets_latest, "npm_markets_latest.parquet")

# Read in file

npm_markets_latest <- read_parquet("npm_markets_latest.parquet")