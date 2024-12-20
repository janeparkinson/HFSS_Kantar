### Extracting 1,000 top-selling products to share with Stirling University
### Catriona Fraser, December 2024

## Set up

library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readxl)
library(data.table) # For 'fread' function to read in CSVs efficiently
library(lubridate)
setwd("//PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

## Read in data

# Read in 2022/23 data that Elaine has previously processed

HFSSFINAL22_23 <- read_parquet("HFSSFINAL22_23.parquet")

## Process data

# Only look at products bought in 2023
only_2023 <- HFSSFINAL22_23 %>%
  mutate(purchdate = dmy(purchdate)) %>%
  filter(year(purchdate) == 2023)

# Only look at products in HFSS categories
hfss_2023 <- only_2023 %>%
  rename(hfss_category = "HFSS Category") %>%
  filter(!is.na(hfss_category))

## Use most recent NPM scores

# Read in NPM scores
npm <- fread("NPM_2024P2.csv")

# Get latest NPM scores
npm_latest <- npm %>%
  group_by(PRODUCT) %>%
  slice(which.max(period))

# Remove current NPM scores in data and match on most recent ones
hfss_2023 <- hfss_2023 %>%
  select(-NPM, -HFSS) %>%
  merge(npm_latest %>%
          select(PRODUCT, NPM, HFSS),
        by.x = "prodcode",
        by.y = "PRODUCT",
        all.x = TRUE)

# Check any missing NPM scores
na_npm <- hfss_2023 %>%
  filter(is.na(NPM))

n_distinct(na_npm$prodcode)

# NPM missing for 35 products. Elaine has identified this as an issue previously

## Add in manufacturer and brand

# HFSSFINAL22_23 dataset doesn't have manufacturer included
# So, read in rf_combined file to match this on (created in the 'Linking NPM scores using rf files' script)
rf_combined <- read_parquet("rf_markets_combined.parquet")

# Match on prodcode to add manufacturer to only_2023 dataframe
only_2023 <- only_2023 %>%
  merge(rf_combined %>%
          select(prodcode = Product, Manufacturer, Brand),
        by = "prodcode",
        all.x = TRUE)

# Check for any missing manufacturers
only_2023 %>%
  filter(is.na(Manufacturer)) %>%
  distinct(prodcode, .keep_all = TRUE) %>%
  View()

# Check for any missing brands
only_2023 %>%
  filter(is.na(Brand)) %>%
  distinct(prodcode, .keep_all = TRUE) %>%
  View()

# Missing for 49 products
# All have missing NPM scores (apart from one, which isn't in a regulation category), so would be excluded anyway

## Find top-selling products

# Remove irrelevant shops and only keep products with an HFSS category
# Also create a column without a pack size
reg_cat_relevant_shops <- only_2023 %>%
  rename(shop_level_3 = "Level 3",
         hfss_category = "HFSS Category") %>%
  filter(!shop_level_3 %in% c("Amazon", "B&M Bargains", "Bakers", "Boots", "Budgens", "Cash & Carry", "Farm Foods", "Home Bargains",
                              "Iceland", "Market Stalls", "Milkman", "Other Bargain Store", "Other Chemist", "Other Drugstores", "Other Freezer Centres", "Other Multiples",
                              "Poundland", "Poundstretcher", "Savers", "Superdrug", "Total Butchers", "Wilkinson"),
         !is.na(hfss_category)) %>%
  mutate(prod_desc_no_size = str_replace(PRODUCT_LONG_DESC, '(\\s*\\([\\w\\s\\.]+\\))+$', ""))

# Some long product descriptions are missing, so if we just use prod_desc_no_size to find the top-selling products, the top ones are those with blank long descriptions
# So, check the top 2000 products by short product description. How many of them have missing long descriptions?
reg_cat_relevant_shops %>%
  group_by(prodcode) %>%
  mutate(tot = sum(as.numeric(pcksbought))) %>%
  arrange(desc(tot)) %>%
  ungroup() %>%
  distinct(prodcode, shop_level_3, .keep_all = TRUE) %>%
  top_n(2000) %>%
  filter(PRODUCT_LONG_DESC == "") %>%
  View()

# Get list to send to Elaine
# reg_cat_relevant_shops %>%
#   group_by(prodcode) %>%
#   mutate(tot = sum(as.numeric(pcksbought))) %>%
#   arrange(desc(tot)) %>%
#   ungroup() %>%
#   distinct(prodcode, shop_level_3, .keep_all = TRUE) %>%
#   top_n(2000) %>%
#   filter(PRODUCT_LONG_DESC == "") %>%
#   distinct(prodcode, PRODUCT_DESC, Manufacturer) %>%
#   View()

# Several, including for some of the very top sellers
# So, if products don't have a long description, make their product description without a pack size (prod_desc_no_size) their short description

reg_cat_relevant_shops <- reg_cat_relevant_shops %>%
  mutate(prod_desc_no_size = if_else(PRODUCT_LONG_DESC == "", PRODUCT_DESC, prod_desc_no_size))

# Combine Chocolate Confectionery and Sugar Confectionery into one confectionery category

reg_cat_relevant_shops <- reg_cat_relevant_shops %>%
  mutate(hfss_category = if_else(hfss_category %in% c("Chocolate Confectionery", "Sugar Confectionery"), "Confectionery", hfss_category))

# Find top products
top_prods <- reg_cat_relevant_shops %>%
  group_by(prod_desc_no_size) %>%
  mutate(tot = sum(as.numeric(pcksbought))) %>%
  arrange(desc(tot)) %>%
  ungroup() %>%
  distinct(prod_desc_no_size, .keep_all = TRUE) %>%
  top_n(1000)

## Check representativeness of categories

# Rank products in dataset and create flag for top 1000, 1500 and 2000
top_prods_ranking <- reg_cat_relevant_shops %>%
  group_by(prod_desc_no_size) %>%
  mutate(tot = sum(as.numeric(pcksbought))) %>%
  arrange(desc(tot)) %>%
  ungroup() %>%
  distinct(prod_desc_no_size, .keep_all = TRUE) %>%
  mutate(sales_rank = rank(-tot, ties.method = "average")) %>% # Note the '-tot' in rank, to make 1 the highest-selling product
  mutate(top_1000 = if_else(sales_rank <= 1006, 1, 0), # Cutoffs are just over exact numbers, since there are ties in ranks
         top_1500 = if_else(sales_rank <= 1507.5, 1, 0),
         top_2000 = if_else(sales_rank <= 2004.5, 1, 0))

# Find percentage of category coverage for the top 1000, 1500 and 2000 products
percent_cat_coverage <- top_prods_ranking %>%
  group_by(hfss_category) %>%
  summarise(tot_cat = n(), # Find total products in each category
         cat_count_top_1000 = length(prod_desc_no_size[top_1000 == 1]), # Count products that are in each category and are in top 1000
         cat_count_top_1500 = length(prod_desc_no_size[top_1500 == 1]), # Count products that are in each category and are in top 1500
         cat_count_top_2000 = length(prod_desc_no_size[top_2000 == 1])) %>% # Count products that are in each category and are in top 2000
  ungroup() %>%
  # Get percentage coverage of products for each category
  mutate(percent_top_1000 = round(cat_count_top_1000/tot_cat*100, 2),
         percent_top_1500 = round(cat_count_top_1500/tot_cat*100, 2),
         percent_top_2000 = round(cat_count_top_2000/tot_cat*100, 2))

# Create chart to show data
percent_cat_coverage %>%
  pivot_longer(cols = starts_with("percent_top"),
               names_to = "no_top_products",
               names_prefix = "percent_",
               values_to = "percent_coverage") %>%
  ggplot(aes(x = no_top_products, y = percent_coverage, fill = hfss_category)) +
  geom_bar(position = "dodge", stat = "identity")

# Get coverage relative to coverage for breakfast cereals for each number of top products
# This will show us which option has the most even coverage over categories
percent_cat_coverage %>%
  pivot_longer(cols = starts_with("percent_top"),
               names_to = "no_top_products",
               names_prefix = "percent_",
               values_to = "percent_coverage") %>%
  mutate(normalised_percent_coverage = (percent_coverage - percent_coverage[hfss_category == "Breakfast Cereals"])/percent_coverage[hfss_category == "Breakfast Cereals"]) %>%
  ggplot(aes(x = no_top_products, y = normalised_percent_coverage, fill = hfss_category)) +
  geom_bar(position = "dodge", stat = "identity")

# Top 2000 products does even things out the most

# For top 2000 products, see how they break down by manufacturer
# Only choose 'own brand' supermarket manufacturers
# I'll send this info to Stirling when I send them the data
top_prods_ranking %>%
  filter(top_2000 == 1) %>%
  group_by(Manufacturer) %>%
  tally() %>%
  filter(Manufacturer %in% c("Aldi Stores Ltd", "Asda Stores Ltd", "J Sainsburys", "Lidl UK GMBH", "Marks and Spencer", "Morrisons Ltd",
                             "Tesco Food Stores Ltd")) %>%
  View()

# Check whether there are any products in the top 2000 products that have duplicates once pack size has been removed
only_dups_2023 <- top_prods_ranking %>%
  filter(top_2000 == 1) %>%
  arrange(prod_desc_no_size) %>%
  group_by(prod_desc_no_size) %>%
  filter(n() > 1,
         prod_desc_no_size != "") %>%
  ungroup()

# No duplicates, so no need to remove products

# Extract only relevant columns to send to Stirling
# Then order by prodcode, so that ranking is not clear
cols_for_audit <- top_prods_ranking %>%
  filter(top_2000 == 1) %>%
  select(prodcode, area = area_desc, market = market_desc, sub_market = submkt_desc, extended = extended_desc, manufacturer = Manufacturer, brand = Brand, vf_title = VF_TITLE, reg_cat = hfss_category, HFSS, short_prod_desc = PRODUCT_DESC, long_prod_desc = prod_desc_no_size) %>%
  arrange(prodcode)
  
#####################################################
## QA check with data processed in a different way ##
#####################################################
  
# Get individual products
  
only_products <- hfss_2023 %>%
  distinct(prodcode, .keep_all = TRUE)

# Read in npm_hfss_prod_names file (processed in 'Linking NPM scores using rf files' script)
npm_hfss_prod_names <- read_parquet("npm_hfss_prod_names.parquet")

# Create column without pack size
npm_hfss_prod_names <- npm_hfss_prod_names %>%
  mutate(prod_desc_no_size = str_replace(PRODUCT_LONG_DESC, '(\\s*\\([\\w\\s\\.]+\\))+$', ""))

# What about if we only look at products bought in the last year?
# Read in HFSSFINAL23 data file created by Elaine (//PHI_conf/PHSci-HFSS/Kantar analysis/Elaine/HFSS Kantar/Code/LINKAGE Panel_purchase+HFSS 2022 AND 2023.R script)
purchases_2023 <- read_parquet("HFSSFINAL23.parquet")

# Only include products that were bought in last year, and get distinct products
hfss_bought_last_year <- purchases_2023 %>%
  select(prodcode, Area, shop_level_2 = "Level 2", shop_level_3 = "Level 3") %>% # Only keep prodcode (for matching) and area_desc and shop (which aren't in the hfss product df) from the purcahses_2023 file
  merge(npm_hfss_prod_names,
        by.x = "prodcode",
        by.y = "PRODUCT") %>%
  # Get distinct products
  distinct(prodcode, .keep_all = TRUE)

# Find products in only_products but not hfss_bought_last_year
only_in_only_products <- anti_join(only_products, hfss_bought_last_year, by = "prodcode")

# Missing products have missing NPM scores, which explains difference

# Find products in hfss_bought_last_year but not in only_products
only_in_bought_last_year <- anti_join(hfss_bought_last_year, only_products, by = "prodcode")

# Two products: 264036 and 273806
# Have been coded as HFSS category ready meals in my analysis but not Elaine's

## End of QA ##