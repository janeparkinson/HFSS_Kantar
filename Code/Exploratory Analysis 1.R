#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# RStudio Workbench is strictly for use by Public Health Scotland staff and     
# authorised users only, and is governed by the Acceptable Usage Policy https://github.com/Public-Health-Scotland/R-Resources/blob/master/posit_workbench_acceptable_use_policy.md.
#
# This is a shared resource and is hosted on a pay-as-you-go cloud computing
# platform.  Your usage will incur direct financial cost to Public Health
# Scotland.  As such, please ensure
#
#   1. that this session is appropriately sized with the minimum number of CPUs
#      and memory required for the size and scale of your analysis;
#   2. the code you write in this script is optimal and only writes out the
#      data required, nothing more.
#   3. you close this session when not in use; idle sessions still cost PHS
#      money!
#
# For further guidance, please see https://github.com/Public-Health-Scotland/R-Resources/blob/master/posit_workbench_best_practice_with_r.md.
#
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

#EXPLORATORY ANALYSIS OF KWP DATA
#Written by Elaine Tod
#29th October 2024

#Set libraries and working directories
library(tidyverse) #lots of functions
library(arrow) # for efficient file saving and reading
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(data.table) # For 'fread' function to read in CSVs efficiently
setwd("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/")

#Open Master file for 2022 and 2023 combined
HFSSFINAL22_23 <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL22_23.parquet")

#Average number of shopping trip per household between Jan 2022 and December 2023
#Range of between 1 and 712 shopping trips per household between WK1 2022 and WK52 2023
#Data are very positively skewed i.e. mean will ve larger than the median or mode as it takes into account all extremes
#Mean number of shopping trips from WK1 2022-WK52 2023 = 143
#Median number of shopping trips from WK1 2022-WK52 2023 = 112
Total_shopping_trips <- HFSSFINAL22_23%>%
  group_by(panel_id) %>%
  summarise(count2 = n_distinct(purchdate))

 summary(Total_shopping_trips) # Minimum 1, maximum 712, mean = 142, median = 112

 Total_shopping_trips_count_of_obs <- table(Total_shopping_trips$count2)
 
 
 
 

#What proportion of HFSS and non-HFSS products were bought on promotion in 2022/23?
# NEED TO LINK PROMOTION CODE DESCRIPTIONS - CURRENTLY ONLY CODE INFO
HFSS_bought_on_promotion <- HFSSFINAL22_23%>%
  group_by(promcode, `HFSS Category`) %>%
  count()

HFSS_bought_on_promotion <- HFSS_bought_on_promotion[order(HFSS_bought_on_promotion$"HFSS Category", -HFSS_bought_on_promotion$n), ]


HFSS_bought_on_promotion = HFSS_bought_on_promotion %>% #REMEMBER TO REMOVE NA FIRST (I.E. NON-HFSS REGULATED PRODUCTS)
  group_by(`HFSS Category`)%>%
  mutate(percent = (n/sum(n)*100))


#Collapse to panel_id level
#Not sure if I need to do this stage but the data are at purchase level and I want a count at panelID level
#Slice data by the first row that a main shopper (panel_id) appears in the dataset i.e. shopping age at point of entering panel
HFSS_by_panel_ID <- HFSSFINAL22_23 %>%
  group_by(panel_id) %>%
  slice(1)


#GRAPH 1: Barchart of panel members by council area of residence
ggplot(HFSS_by_panel_ID, aes(x=CouncilArea2019Code))+
panel_by_councilarea <- HFSS_by_panel_ID %>% 
  group_by(CouncilArea2019Code) %>% 
  tally()  geom_bar() +
  theme_minimal()

ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID$CouncilArea2019Code), fill = 'blue')+
  

# GRAPH 2: Main shopper by age
  panel_by_shopperage <- HFSS_by_panel_ID %>% # 74 age groups from 20-99
  group_by(main_shopper_age) %>% 
  tally()  

summary(panel_by_shopperage$main_shopper_age)

panel_by_shopperage = panel_by_shopperage %>% 
  mutate(percent = (n/sum(n)*100))

ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID$main_shopper_age), fill = 'blue') # data are normally distributed



# GRAPH 3: Main shopper by highest qualification reported
panel_by_highestqual <- HFSS_by_panel_ID %>% # 
  group_by(highest_qualification_of_main_earner) %>% 
  tally()  

summary(panel_by_highestqual$highest_qualification_of_main_earner)

panel_by_highestqual = panel_by_highestqual %>% 
  mutate(percent = (n/sum(n)*100))

ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID$highest_qualification_of_main_earner), fill = 'blue') # most panel members have tertiary level education


# GRAPH 4: Main shopper by ethnicity
panel_by_ethnicity <- HFSS_by_panel_ID %>% # 
  group_by(ethnicity) %>% 
  tally()  

panel_by_ethnicity = panel_by_ethnicity %>% 
  mutate(percent = (n/sum(n)*100))

ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID$ethnicity), fill = 'blue') # majority of panel members are 'white-British', 'white - other', 'unknown'



# GRAPH 5: Households by household income
panel_by_HHincome <- HFSS_by_panel_ID %>% # 
  group_by(household_income) %>% 
  tally()  

panel_by_HHincome = panel_by_HHincome %>% 
  mutate(percent = (n/sum(n)*100))

HFSS_by_panel_ID %>%  #Explore missing data four household income. 376 households did not want to answer
  group_by(household_income, social_class) %>%
  tally()

ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID$household_income), fill = 'blue') # majority of households reporting inome were around the middel of the distritbution

#Missing values for household income (376 households chose not to answer)
#0 = chose not to answer
#9 = unknown - NO NINES IN THE DATASET
missing_HHincome <- HFSS_by_panel_ID %>%  #Explore missing data for household income. 376 households did not want to answer
  group_by(household_income, social_class) %>%
  tally()
  
missing_HHincome0 <- filter(missing_HHincome, household_income == 0,) 
missing_HHincome0 <- missing_HHincome0 %>%
  mutate(percent = (n/sum(n)*100))

missing_HHincome9 <- filter(missing_HHincome, household_income == 9,) 
missing_HHincome9 <- missing_HHincome9 %>%
  mutate(percent = (n/sum(n)*100))


# GRAPH 6: Households by social class
panel_by_social_class <- HFSS_by_panel_ID %>% # 
  group_by(social_class) %>% 
  tally()  

panel_by_social_class = panel_by_social_class %>% 
  mutate(percent = (n/sum(n)*100))

ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID$social_class), fill = 'blue') # majority of households reporting income were around the middle of the distritbution



# GRAPH 7: Households by life-stage
panel_by_life_stage <- HFSS_by_panel_ID %>% 
  group_by(number_of_children) %>% 
  tally()  

panel_by_life_stage = panel_by_life_stage %>% 
  mutate(percent = (n/sum(n)*100))

ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID$number_of_children), fill = 'blue') 



# GRAPH 8: Households by council tax band
panel_by_counciltax <- HFSS_by_panel_ID %>%
  group_by(CouncilArea2019Code) %>% 
  tally()  

panel_by_counciltax = panel_by_counciltax %>% 
  mutate(percent = (n/sum(n)*100))

ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID$CouncilArea2019Code), fill = 'blue') 


# GRAPH 9: Households by tenure
panel_by_tenure <- HFSS_by_panel_ID %>% 
  group_by(tenure) %>%  
  tally()  

panel_by_tenure = panel_by_tenure %>% 
  mutate(percent = (n/sum(n)*100))

ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID$tenure), fill = 'blue') # Fairly even split between owned outright, owned mortgage. rented

#Can we isolate stores which are exempt i.e. have less than 50 employees in total?
#We can approximate this using the following categories on line 206, this may include some chains e.g. baker and butcher chains, market stalls used by larger commercial outfits,large
# (cont) large dairy suppliers e.g. McQueens, Grahams etc...
# 
HFSSFINAL22_23[HFSSFINAL22_23$`Level 3` == 'Milkman', 'All Other Outlets','Bakers', 'Market Stalls', 'Total Other Independents', 'Other Chemist', 'Other Drugstores', 'Total Butchers',]
filteredexempt_HFSSFINAL22_23 <- HFSSFINAL22_23 %>% filter(`Level 3` %in% c('Milkman', 'All Other Outlets','Bakers', 'Market Stalls', 'Total Other Independents', 'Other Chemist', 'Other Drugstores', 'Total Butchers'))





#Measuring attrition

# Attrition in time between reported shops




HFSSFINAL22_23PC$purchdate <- lubridate::dmy(HFSSFINAL22_23PC$purchdate) #convert date variable from character to date format

shopping_freq_attrition <- HFSSFINAL22_23PC %>% 
  group_by(HFSSFINAL22_23PC$panel_id) %>%
  dplyr::mutate(
    first = dplyr::first(HFSSFINAL22_23PC$purchdate),
    second = dplyr::second(HFSSFINAL22_23PC$purchdate),
    last = dplyr:: last(HFSSFINAL22_23PC$purchdate),
  )

library(dplyr)
ds2 <- ds %>% 
  group_by(pnum) %>% 
  summarise(visitmax = max(visit), visitmin = min(visit), delay = visitmax - visitmin)
