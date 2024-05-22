# Kantar analysis - Elaine - 15/05/202


library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Kantar data/FSS_PHSKantar Dataset20240405115021")

# read in fixed width file (purchase data)'
sample <- read_fwf("RT42D056_202301.txt", 
                   fwf_widths(c(6, 4, 6, 3, 8, 6, 1, 13, 17, 21, 8, 8, 11, 13, 11, 8, 4, 7, 36, 3, 6, 4, 4, 4, 4, 4, 4, 10), 
                              c("hhdnum", "null1", "prodcode", "shopcode", "null2", "weeknum", "daynum", "null3", 
                                "barcode", "null4", "pcksbought","amtspent", "volume", "null5", "grossupfact",
                                "null6", "promcode", "purchnum", "null7", "validfield", "null8", "area", "market", 
                                "mktsector", "submkt", "extended", "null9", "purchdate" )))

# read in a csv (2022 panel data)
panel_202301 <- read_csv("Kantar analysis/Kantar data/FSS_PHSKantar Dataset20240405115021/2022 Purchase Panel RST Delivery/2022 Purchase Panel RST Delivery/panel data/panel_household_master_202301.csv")


# joining the 2022 panel data and the purchase data
joined_data <- panel_202301 %>%
  merge(y=sample, by.y="hhdnum", by.x="panel_id" )

write_parquet(joined_data, "joined_data2022.parquet")
joined_data_2022 <- read_parquet("joined_data2022.parquet")






