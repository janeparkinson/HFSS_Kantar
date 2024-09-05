## Set up

library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readxl)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

## Read in data

# Read in rf files
# Use select(any_of()) to include 'Low sugar/calorie/fat' column if the dataframe has it (to check for added sugar drinks), but not if it doesn't

# files <- list.files(path = "2023 Purchase Data/product master", full.names = TRUE)
# 
# dataframes <- lapply(files, function(f){
#   fread(f, skip = 1, header = TRUE) %>%
#     select(any_of(c("Product", "Product Desc", "Manufacturer", "Brand", "RST 4 Market", "RST 4 Sub Market", "RST 4 Extended", "Low Sugar/Calorie/Fat")))
# }
#                      )
# 
# rf_combined <- do.call(bind_rows, dataframes)
# 
# # Write rf_combined to a file
# 
# write_parquet(rf_combined, "rf_markets_combined.parquet")

# Read in rf_combined file

rf_combined <- read_parquet("rf_markets_combined.parquet")

# Read in NPM data
npm <- fread("NPM_2024P2.csv")

# HFSS category data
# There is a mistake in Danish pastries and fruit/malt loaves extended being swapped.
# Has been fixed in amended sheet.
hfss <- read_xlsx("PHS HFSS Itemisation Breakdown amended.xlsx", range = "A2:D642")

# rst_products, for longer product names
rst_products_2023 <- fread("2023 Purchase Data/rst_products.csv")
rst_products_2022 <- fread("2022 Purchase Data/rst_products.csv")
# Check equal
all.equal(rst_products_2022, rst_products_2023)
# They are equal, so use 2023 file below

## Process NPM data

# Get latest NPM scores
npm_latest <- npm %>%
  group_by(PRODUCT) %>%
  slice(which.max(period))

# Match NPM products to markets from rf files

npm_markets <- npm_latest %>%
  merge(rf_combined, by.x = "PRODUCT", by.y = "Product", all.x = TRUE) %>%
  rename(market = "RST 4 Market",
         sub_market = "RST 4 Sub Market",
         extended = "RST 4 Extended",
         low_sugar = "Low Sugar/Calorie/Fat")

## Process HFSS data

# Rename columns in HFSS data and concatenate market, submarket and extended
# Only some markets/submarkets have extended specified, so only include extended in concatenation if it's relevant

hfss_tidy <- hfss %>%
  rename(category = "HFSS Category",
         market = "RST 4 Market",
         sub_market = "RST 4 Sub Market",
         extended = "RST 4 Extended") %>%
  mutate(concat = if_else(is.na(extended), paste(market, sub_market), paste(market, sub_market, extended)))

# Create a df where extended isn't NA

hfss_extended <- hfss_tidy %>%
  filter(!is.na(extended))

# Use this to set how to concatenate in the npm_markets file

npm_markets <- npm_markets %>%
  mutate(concat = if_else(market %in% hfss_extended$market &
                            sub_market %in% hfss_extended$sub_market &
                            paste(market, sub_market, extended) %in% hfss_tidy$concat,
                          paste(market, sub_market, extended),
                          paste(market, sub_market)))

# Match on HFSS categories
npm_markets_hfss <- npm_markets %>%
  merge(hfss_tidy %>%
          select(hfss_category = category, concat),
        by = "concat",
        all.y = TRUE)

# Check if any combinations of markets/submarkets/extended are missing
npm_markets_hfss %>%
  filter(is.na(PRODUCT)) %>%
  View()

# Investigate missing ones:
# Frozen Ready Meals Italian Frozen Ready Me Italian Kievs
npm_markets %>%
  filter(market == "Frozen Ready Meals",
         sub_market == "Italian") %>%
  group_by(extended) %>%
  tally()

# Check rf and NPM files separately

rf_combined %>%
  rename(market = "RST 4 Market",
         sub_market = "RST 4 Sub Market",
         extended = "RST 4 Extended") %>%
  filter(market == "Frozen Ready Meals" &
           sub_market == "Italian"
         #,
         #extended == "Frozen Ready Breaded-Kievs/E"
         ) %>%
  group_by(extended) %>%
  tally()

# Not in rf files

# Process drinks:
# Remove non-alcoholic beer, since SG has confirmed that most of these are exempt
# Also remove any drinks that don't have added sugar

# First, investigate added sugar categories for drinks
npm_markets_hfss %>%
  filter(hfss_category == "Prepared Soft Drinks") %>%
  group_by(low_sugar) %>%
  tally()

npm_markets_hfss %>%
  filter(hfss_category == "Prepared Soft Drinks") %>%
  arrange(Manufacturer, Brand) %>%
  View()

# Keep only 'Regular' and 'Standard'
# Also filter out non-alcoholic beer
npm_markets_hfss_filtered <- npm_markets_hfss %>%
  filter(hfss_category != "Prepared Soft Drinks" | low_sugar %in% c("Regular", "Standard"),
         market != "Non Alcoholic Beer")

# Match on full names of products from rst_products file
npm_hfss_prod_names <- npm_markets_hfss_filtered %>%
  merge(rst_products_2023%>%
          select(PRODUCT, PRODUCT_LONG_DESC),
        by = "PRODUCT",
        all.x = TRUE)

# How many HFSS vs not HFSS?
table(npm_hfss_prod_names$HFSS)
# 59% HFSS

# How many don't have a long description?
npm_hfss_prod_names %>%
  filter(PRODUCT_LONG_DESC == "") %>%
  View()
# 89,044 don't have long descriptions, around 37%

## Investigate wet/smoked fish

npm_markets %>%
  filter(market == "Wet/Smoked Fish") %>%
  group_by(sub_market) %>%
  tally()