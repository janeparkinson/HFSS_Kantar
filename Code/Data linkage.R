# Kantar analysis - Elaine - 15/05/202

# JOIN PANEL AND PURCHASE DATA FOR 2022
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

# read in fixed width file (purchase data)'
purchase2022 <- read_fwf("2022 Purchase Data/purchase record files/RT42D056_202301.txt", 
                   fwf_widths(c(6, 4, 6, 3, 8, 6, 1, 13, 17, 21, 8, 8, 11, 13, 11, 8, 4, 7, 36, 3, 6, 4, 4, 4, 4, 4, 4, 10), 
                              c("hhdnum", "null1", "prodcode", "shopcode", "null2", "weeknum", "daynum", "null3", 
                                "barcode", "null4", "pcksbought","amtspent", "volume", "null5", "grossupfact",
                                "null6", "promcode", "purchnum", "null7", "validfield", "null8", "area", "market", 
                                "mktsector", "submkt", "extended", "null9", "purchdate" )))

# read in a csv (2022 panel data)
panel_202301 <- read_csv("Panel data/panel_household_master_202301.csv")


# joining the 2022 panel data and the 2022 purchase data
joined_data <- panel_202301 %>%
  merge(y=purchase2022, by.y="hhdnum", by.x="panel_id" )

write_parquet(joined_data, "joined_data2022.parquet")
joined_data_2022 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/joined_data2022.parquet")

#INVESTIGATE DIFFERENCE IN THE NUMBER OF OBSERVATIONS IN THE PURCHASE DATA FILE AND THE JOINED DATA FILE
#compare household ID variables in 2022 purchase data and joined_data_2022 to identify observations missing from the linked dataset after merge
n_distinct(purchase2022 $hhdnum)
n_distinct(panel_202301 $panel_id)

#Which IDs are not in the panel_ID dataset
library(dplyr)
setdiff(purchase2022 $hhdnum, panel_202301 $panel_id)



# JOIN PANEL AND PURCHASE DATA FOR 2023
# read in fixed width file (purchase data)'
purchase2023 <- read_fwf("2023 Purchase Data/purchase record files/RT42D056_202401.txt", 
                         fwf_widths(c(6, 4, 6, 3, 8, 6, 1, 13, 17, 21, 8, 8, 11, 13, 11, 8, 4, 7, 36, 3, 6, 4, 4, 4, 4, 4, 4, 10), 
                                    c("hhdnum", "null1", "prodcode", "shopcode", "null2", "weeknum", "daynum", "null3", 
                                      "barcode", "null4", "pcksbought","amtspent", "volume", "null5", "grossupfact",
                                      "null6", "promcode", "purchnum", "null7", "validfield", "null8", "area", "market", 
                                      "mktsector", "submkt", "extended", "null9", "purchdate" )))

# read in a csv (2023 panel data)
panel_202401 <- read_csv("Panel data/panel_household_master_202401.csv")


# joining the 2023 panel data and the 2023 purchase data
joined_data <- panel_202401 %>%
  merge(y=purchase2023, by.y="hhdnum", by.x="panel_id" )

write_parquet(joined_data, "joined_data2023.parquet")
joined_data_2023 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/joined_data2023.parquet")

#INVESTIGATE DIFFERENCE IN THE NUMBER OF OBSERVATIONS IN THE PURCHASE DATA FILE AND THE JOINED DATA FILE
#compare household ID variables in 2023 purchase data and panel datasets to identify the IDs missing from the panel dataset 
n_distinct(purchase2023 $hhdnum)
n_distinct(panel_202401 $panel_id)

#Which IDs are not in the panel_ID dataset
library(dplyr)
setdiff(purchase2023 $hhdnum, panel_202401 $panel_id)





