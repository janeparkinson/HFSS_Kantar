## Set up

library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readxl)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("//PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

## Read in data

# Read in rf files
# Use select(any_of()) to include 'Low sugar/calorie/fat' column if the dataframe has it (to check for added sugar drinks), but not if it doesn't
# 
# files <- list.files(path = "2023 Purchase Data/product master", full.names = TRUE)
# 
# dataframes <- lapply(files, function(f){
#   fread(f, skip = 1, header = TRUE) %>%
#     select(any_of(c("Product", "Product Desc", "Manufacturer", "Brand", "RST 4 Trading Area", "RST 4 Market", "RST 4 Sub Market", "RST 4 Extended", "Low Sugar/Calorie/Fat")))
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

# Match on full names of products and VF_TITLE from rst_products file
# Use unfiltered product files
npm_hfss_prod_names <- npm_markets_hfss %>%
  merge(rst_products_2023%>%
          select(PRODUCT, VF_TITLE, PRODUCT_LONG_DESC),
        by = "PRODUCT",
        all.x = TRUE)

# Write to file
#write_parquet(npm_hfss_prod_names, "npm_hfss_prod_names.parquet")

# Read in npm_hfss_prod_names file
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
        #%>%
         # select(Manufacturer, Brand, hfss_category, PRODUCT, NPM, HFSS, hfss_period = period, prod_desc_no_size, market, sub_market, extended, VF_TITLE, `Product Desc`, PRODUCT_LONG_DESC, prod_desc_no_size, low_sugar),
        by.x = "prodcode",
        by.y = "PRODUCT") %>%
  # Get distinct products
  distinct(.)
  #distinct(hfss_category, NPM, HFSS, period, Manufacturer, Brand, Area, market, sub_market, extended, VF_TITLE, prodcode, `Product Desc`, PRODUCT_LONG_DESC, prod_desc_no_size, .keep_all = TRUE)

# Are there any duplicates that have different HFSS scores for products bought in 2023?
hfss_bought_last_year_diff_hfss <- hfss_bought_last_year %>%
  arrange(prod_desc_no_size) %>%
  filter(!is.na(prod_desc_no_size)) %>%
  filter(prod_desc_no_size == lag(prod_desc_no_size) &
           HFSS != lag(HFSS) |
           prod_desc_no_size == lead(prod_desc_no_size) &
           HFSS != lead(HFSS) )

# Yes, quite a lot. Save as a CSV file. This has decisions noted on it.
write_csv(hfss_bought_last_year_diff_hfss, "hfss_bought_last_year_diff_hfss.csv")

# Exclude duplicate products, as detailed in CSV above
prods_to_exclude <- c(2437,
                      193159,
                      109442,
                      277669,
                      180759,
                      276864,
                      56352,
                      80006,
                      186199,
                      190038,
                      94232,
                      196608,
                      252861,
                      123284,
                      507939,
                      111046,
                      199652,
                      203061,
                      100681,
                      90588,
                      120583,
                      549733,
                      59330,
                      155066,
                      319387,
                      999346,
                      382432,
                      212875,
                      269930,
                      295068,
                      43475,
                      11638,
                      457829,
                      264322,
                      81413,
                      702875,
                      702875,
                      100230,
                      727507,
                      2763,
                      477879,
                      135307
)

hfss_bought_last_year_filtered <- hfss_bought_last_year %>%
  filter(!prodcode %in% prods_to_exclude)

## Filter out irrelevant shops

# Investigate included shops

table(hfss_bought_last_year_filtered$shop_level_2)
table(hfss_bought_last_year_filtered$shop_level_3)

# Relevant shops not specifically included in shop names: Costcutter, day-to-day, Spar, Premier, N&S mini

# Remove irrelevant shops

bought_last_year_relevant_shops <- hfss_bought_last_year_filtered %>%
  filter(!shop_level_3 %in% c("Amazon", "B&M Bargains", "Bakers", "Boots", "Budgens", "Cash & Carry", "Farm Foods", "Home Bargains",
                              "Iceland", "Market Stalls", "Milkman", "Other Bargain Store", "Other Chemist", "Other Drugstores", "Other Freezer Centres", "Other Multiples",
                              "Poundland", "Poundstretcher", "Savers", "Superdrug", "Total Butchers", "Wilkinson"))

# Remove shop columns, and remove duplicates bought in different shops
# Also remove product_long_desc column to keep one pack size of products (if the HFSS designation and HFSS category is the same)
relevant_shops_no_shops <- bought_last_year_relevant_shops %>%
  select(-shop_level_2, -shop_level_3, -PRODUCT_LONG_DESC) %>%
  distinct(hfss_category, HFSS, prod_desc_no_size, .keep_all = TRUE)

# Get dataframe FOR 2023 PRODUCTS with only duplicates once pack size has been removed
only_dups_2023 <- relevant_shops_no_shops %>%
  arrange(prod_desc_no_size) %>%
  group_by(prod_desc_no_size) %>%
  filter(n() > 1,
         prod_desc_no_size != "") %>%
  ungroup()

# Some further duplicates here to exclude (not sure why these weren't picked up in previous check)
# 549841 - Jacob's crinkys. Established in previous CSV that these aren't HFSS
# 147951- Lucozade. Established in previous CSV that this is HFSS.

prods_to_exclude_2 <- c(549841, 147951)

relevant_shops_no_shops_filtered <- relevant_shops_no_shops %>%
  filter(!prodcode %in% prods_to_exclude_2)

# # Look at products in HFSS categories, that were bought in 2023
# table(bought_last_year_one_pack_size$hfss_category, bought_last_year_one_pack_size$HFSS)

# Only get count of category, and round to nearest 100
# bought_last_year_one_pack_size %>%
#   group_by(hfss_category) %>%
#   summarise(count = round(n(), -2)) %>%
#   ungroup() %>%
#   View()
# 
# # Get percentage HFSS
# tab <- table(bought_last_year_one_pack_size$hfss_category, bought_last_year_one_pack_size$HFSS)
# round(100 * tab / rowSums(tab), 0) %>% View()
# 
# # Get dataframe FOR 2023 PRODUCTS with only duplicates once pack size has been removed
# only_dups_2023 <- hfss_bought_last_year %>%
#   arrange(prod_desc_no_size) %>%
#   group_by(prod_desc_no_size) %>%
#   filter(n() > 1,
#          prod_desc_no_size != "") %>%
#   ungroup()
# 
# ## Further investigations
# 
# # How many HFSS vs not HFSS?
# table(npm_hfss_prod_names$HFSS)
# # 59% HFSS
# 
# # How many don't have a long description?
# npm_hfss_prod_names %>%
#   filter(PRODUCT_LONG_DESC == "") %>%
#   View()
# # 89,044 don't have long descriptions, around 37%

# Code Chocolate Confectionery and Sugar Confectionery into Confectionery
relevant_shops_no_shops_filtered <- relevant_shops_no_shops_filtered %>%
  mutate(hfss_category = if_else(hfss_category %in% c("Chocolate Confectionery", "Sugar Confectionery"), "Confectionery", hfss_category))

## Select relevant columns and save file as a CSV

cols_for_audit <- relevant_shops_no_shops_filtered %>%
  arrange(prod_desc_no_size) %>%
  select(prodcode, area = Area, market, sub_market, extended, manufacturer = Manufacturer, brand = Brand, vf_title = VF_TITLE, reg_cat = hfss_category, HFSS, short_prod_desc = "Product Desc", long_prod_desc = prod_desc_no_size)

# Save as CSV
write_csv(cols_for_audit, "hfss_for_audit.csv")

## Investigate brands/flavours

relevant_shops_no_shops %>%
  filter(Brand == "Mccoys Crisps") %>%
  View()

# McCoy's crisps is a brand on its own and all HFSS

relevant_shops_no_shops %>%
  filter(str_detect(Brand, "Walkers")) %>%
  View()
# Not all Walkers crisps are HFSS

relevant_shops_no_shops %>%
  filter(Brand == "Walkers Regular Crisps") %>%
  View()
# Most Walkers regular crisps are HFSS, except Christmas special ones (I wonder why)

# Ger percentages of HFSS per brand
tab_brand <- table(relevant_shops_no_shops$Brand, relevant_shops_no_shops$HFSS)
round(100 * tab_brand / rowSums(tab_brand), 0) %>%
  as.data.frame() %>%
  # Only select where HFSS = 1
  filter(Var2 == 1) %>%
  arrange(Freq) %>%
  select(-Var2, Brand = Var1) %>%
  View()