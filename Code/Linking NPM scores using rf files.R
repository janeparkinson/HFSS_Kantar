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
hfss_2023 <- read_xlsx("2023 Purchase Data/HFSS.xlsx")
hfss_2022 <- read_xlsx("2022 Purchase Data/HFSS.xlsx")
# Check equal
all.equal(hfss_2023, hfss_2022)

# rst_products, for longer product names
rst_products_2023 <- fread("2023 Purchase Data/rst_products.csv")
rst_products_2022 <- fread("2022 Purchase Data/rst_products.csv")
# Check equal
all.equal(rst_products_2022, rst_products_2023)

## Process NPM data

# Get latest NPM scores
npm_latest <- npm %>%
  group_by(PRODUCT) %>%
  slice(which.max(period))

# Match NPM products to markets from rf files

npm_markets <- npm_latest %>%
  merge(rf_combined, by.x = "PRODUCT", by.y = "Product", all.x = TRUE)

## Process HFSS data

# Tidy HFSS data

hfss_tidy <- hfss_2023 %>%
  rename(category = "HFSS Category",
         market = Filters,
         sub_market = "...3",
         extended = "...4") %>%
  fill(category, .direction = "down") %>%
  filter(!is.na(category)) # Remove first blank row

# Only show submarkets that are in the HFSS categories

# First create vector of submarkets in HFSS categories
hfss_sub_markets <- hfss_tidy %>%
  select(sub_market) %>%
  filter(str_detect(sub_market, "RST 4", negate = TRUE))

# Then use this to filter the NPM markets df
npm_hfss_sub_market_lookup <- npm_markets %>%
  rename(market = "RST 4 Market", # First rename columns, otherwise filtering doesn't seem to work
         sub_market = "RST 4 Sub Market",
         extended = "RST 4 Extended") %>%
  filter(sub_market %in% hfss_sub_markets$sub_market) %>%
  select(market, sub_market, extended) %>%
  unique(.) %>%
  arrange(market, sub_market, extended)

# Match markets in lookup to submarkets in HFSS file?
# Then check if it's easy to match these manually

sub_market_to_market <- hfss_tidy %>%
  select(sub_market) %>%
  merge(npm_hfss_sub_market_lookup, by = "sub_market") %>%
  arrange(market) %>%
  select(market, everything()) # Move market column to the start of the dataframe

# Save file as csv
#write_csv(sub_market_to_market, "hfss_sub_market_to_market.csv")

# Soft drinks are only subject to regulations if they have added sugar
# So, need to match on the 'Low Sugar/Calorie/Fat' column from the rf files and exclude those that are 'sugar free' and 'no added sugar'?
# In files rf0084. rf0205, 
# For the audit, do we need to know whether products are in the categories and HFSS vs not in the categories and HFSS?

# Alcohol substitutes are exempt according to the English legislation (https://www.legislation.gov.uk/uksi/2018/41/regulation/9/made), but non-alcoholic beer is in the Kantar categories

# Read in hfss_cleaned, which has been done manually

hfss_cleaned <- read_xlsx("hfss_cleaned.xlsx", sheet = "Matched")

# Concatenate market, submarket and extended

hfss_cleaned <- hfss_cleaned %>%
  mutate(Extended = coalesce(Extended, ""), # Replace NAs in Extended column with blank
         concat = paste(Market, Submarket, Extended))

# Concatenate in npm_markets file
npm_markets_concat <- npm_markets %>%
  rename(Market = "RST 4 Market",
         Submarket = "RST 4 Sub Market",
         Extended = "RST 4 Extended") %>%
  mutate(concat = paste(Market, Submarket, Extended))

# Match on HFSS categories
npm_markets_hfss <- npm_markets_concat %>%
  merge(hfss_cleaned %>%
          select(hfss_category = Category, concat),
        by = "concat",
        all.y = TRUE)

# Check which combinations of markets/submarkets/extended are missing
npm_markets_hfss %>%
  filter(is.na(PRODUCT)) %>%
  View()

# None!

# Match on full names of products from rst_products file
npm_hfss_prod_names <- npm_markets_hfss %>%
  merge(rst_products_2023%>%
          select(PRODUCT, PRODUCT_LONG_DESC),
        by = "PRODUCT",
        all.x = TRUE)

# Remove duplicates and NA products
npm_hfss_prod_names_no_dups <- distinct(npm_hfss_prod_names) %>%
  filter(!is.na(PRODUCT))

# How many HFSS vs not HFSS?
table(npm_hfss_prod_names_no_dups$HFSS)
# 59% HFSS

# How many don't have a long description?
npm_hfss_prod_names_no_dups %>%
  filter(PRODUCT_LONG_DESC == "") %>%
  View()
# 91,124 don't have long descriptions, around 36%

## Investigate alcohol substitute drinks
npm_hfss_prod_names_no_dups %>%
  filter(Market == "Non Alcoholic Beer") %>%
  View()
# 192 products

npm_hfss_prod_names_no_dups %>%
  filter(Market == "Non Alcoholic Beer") %>%
  group_by(HFSS) %>%
  tally() %>%
  ungroup()
# Only 8 are HFSS. They are all fruit-flavoured ones (presumably higher in sugar).