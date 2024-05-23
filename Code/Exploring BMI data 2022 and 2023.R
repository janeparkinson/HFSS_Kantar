# Kantar analysis - Elaine - 15/05/2024

# Exploration of BMI data and possible outliers

#Read in packages
library(tidyverse) #lots of functions

#Read on 2023 panel data file
# read in a csv (2022 panel data)
panel_202301 <- read_csv("Panel data/panel_household_master_202301.csv")

#2023 panel data
summary (panel_202301$bmi)
boxplot (panel_202301$bmi)
qqnorm (panel_202301$bmi)
