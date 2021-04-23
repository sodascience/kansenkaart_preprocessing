# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding perinatal outcomes to the cohort.
#   - Writing `scratch/03_outcomes.rds`.
#
# (c) ODISSEI Social Data Science team 2021

#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(lubridate)
library(readxl)


#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds("scratch/02_predictors.rds")

# load the configuration
cfg <- config::get("data_preparation")
loc <- config::get("file_locations")


# import percentile weight boys & girls
boys_weight_tab <- read_excel(loc$boys_weight_data) %>%
  rename(gestational_age = zwangerschapsduur,
         perc_10_boys = "0.1_percentile") %>%
  select(c(gestational_age, perc_10_boys))

girls_weight_tab <- read_excel(loc$girls_weight_data) %>%
  rename(gestational_age = zwangerschapsduur,
         perc_10_girls = "0.1_percentile") %>%
  select(c(gestational_age, perc_10_girls))


# create function for cleaning the perinatal data
CleanPerinatal <- function(file) {

  path = file.path(loc$data_folder, loc$prnl_data_2009)

  prnl_tab <- read_sav(path) %>% 
    select(c("RINPERSOONS_KIND_UITGEBREID", "RINPERSOON_KIND",
             "Gewichtkind_ruw", "Amddd", "Geslachtkind")) %>%
    left_join(boys_weight_tab, by = c("Amddd" = "gestational_age")) %>%
    left_join(girls_weight_tab, by = c("Amddd" = "gestational_age"))


  # create low birth weight & premature_birth variable
  prnl_tab <- prnl_tab %>%
    mutate(
      low_birthweight = ifelse((Geslachtkind == 1 & Gewichtkind_ruw < perc_10_boys) |
                                 (Geslachtkind == 2 & Gewichtkind_ruw < perc_10_girls),
                               1, 0),
      
      # create low birth weight varaible for infants with > 294 gestational age
      low_birthweight = ifelse((Geslachtkind == 1 & Amddd > 294 & Gewichtkind_ruw < 3318) |
                                 (Geslachtkind == 2 & Amddd > 294 & Gewichtkind_ruw < 3185),
                               1, low_birthweight),
      low_birthweight = ifelse((Geslachtkind == 1 & Amddd > 294 & Gewichtkind_ruw > 3318) |
                                 (Geslachtkind == 2 & Amddd > 294 & Gewichtkind_ruw > 3185),
                               0, low_birthweight),
      
      # convert to NA for infants with < 161 gestational age
      low_birthweight  = ifelse(Amddd < 161, NA, low_birthweight),
      
      # premature birth for infants with < 259 gestational age
      premature_birth = ifelse(Amddd < 259, 1, 0)
    )

}

perined_dat <- rbind(CleanPerinatal(loc$prnl_data_2008), CleanPerinatal(loc$prnl_data_2009), 
                     CleanPerinatal(loc$prnl_data_2010), CleanPerinatal(loc$prnl_data_2011),
                     CleanPerinatal(loc$prnl_data_2012), CleanPerinatal(loc$prnl_data_2013),
                     CleanPerinatal(loc$prnl_data_2014), CleanPerinatal(loc$prnl_data_2015),
                     CleanPerinatal(loc$prnl_data_2016))









