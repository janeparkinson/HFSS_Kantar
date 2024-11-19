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
HFSSFINAL22_23PC <- read_parquet("/PHI_conf/PHSci-HFSS/Kantar analysis/Working Data/HFSSFINAL22_23PC.parquet")

#Average number of shopping trip per household between Jan 2022 and December 2023
#Range of between 1 and 712 shopping trips per household between WK1 2022 and WK52 2023
#Data are very positively skewed i.e. mean will ve larger than the median or mode as it takes into account all extremes
#Mean number of shopping trips from WK1 2022-WK52 2023 = 143
#Median number of shopping trips from WK1 2022-WK52 2023 = 112
Total_shopping_trips <- HFSSFINAL22_23PC%>%
  group_by(panel_id) %>%
  summarise(count = n_distinct(purchdate))

 summary(Total_shopping_trips)
 
ggplot() +
  geom_bar(aes(x = Mean_shopping_trips$count), fill = 'blue') 


#What proportion of HFSS and non-HFSS products were bought on promotion in 2022/23?
HFSS_bought_on_promotion <- HFSSFINAL22_23PC%>%
  group_by(promcode, `HFSS Category`) %>%
  count()

HFSS_bought_on_promotion <- HFSS_bought_on_promotion[order(HFSS_bought_on_promotion$"HFSS Category", -HFSS_bought_on_promotion$n), ]


HFSS_bought_on_promotion = HFSS_bought_on_promotion %>% 
  group_by(`HFSS Category`)%>%
  mutate(percent = (n/sum(n)*100))





#Collapse to panel_id level
#Not sure if I need to do this stage but the data are at purchase level and I want a count at panelID level
HFSS_by_panel_ID <- HFSSFINAL22_23PC %>%
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
 
mean(HFSSFINAL22_23PC$main_shopper_age)

ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID$main_shopper_age), fill = 'blue') # data are normally distributed



# GRAPH 3: Main shopper by highest qualification reported
panel_by_highestqual <- HFSS_by_panel_ID %>% # 
  group_by(highest_qualification_of_main_earner) %>% 
  tally()  

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


ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID$household_income), fill = 'blue') # majority of households reporting inome were around the middel of the distritbution



# GRAPH 6: Households by social class
panel_by_social_class <- HFSS_by_panel_ID %>% # 
  group_by(social_class) %>% 
  tally()  

panel_by_social_class = panel_by_social_class %>% 
  mutate(percent = (n/sum(n)*100))


ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID$social_class), fill = 'blue') # majority of households reporting inome were around the middel of the distritbution



# GRAPH 7: Households by life-stage
panel_by_life_stage <- HFSS_by_panel_ID %>% 
  group_by(number_of_children) %>% 
  tally()  

panel_by_life_stage = panel_by_life_stage %>% 
  mutate(percent = (n/sum(n)*100))

panel_by_life_stage2 <- HFSS_by_panel_ID %>% 
  group_by() %>% 
  tally()  

ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID), fill = 'blue') 



# GRAPH 8: Households by council tax band
panel_by_counciltax <- HFSS_by_panel_ID %>%
  group_by() %>% 
  tally()  

panel_by_counciltax = panel_by_counciltax %>% 
  mutate(percent = (n/sum(n)*100))

ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID), fill = 'blue') 




# GRAPH 9: Households by tenure
panel_by_tenure <- HFSS_by_panel_ID %>% 
  group_by(tenure) %>% 
  tally()  

panel_by_tenure = panel_by_tenure %>% 
  mutate(percent = (n/sum(n)*100))

ggplot() +
  geom_bar(aes(x = HFSS_by_panel_ID$tenure), fill = 'blue') # Fairly even split between owned outright, owned mortgage. rented




