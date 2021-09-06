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
library(readxl)


#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "02_predictors.rds"))


# create outcomes
cohort_dat <- cohort_dat %>%
  mutate(
    # perinatal mortality: 24 weeks to < 7 days
    perinatal_mortality = ifelse((diff_days < 7 & !is.na(diff_days)) | 
                                    RINPERSOONS == "I", 1, 0), 
     
    # neonatal mortality: 24 weeks to < 28 days
    neonatal_mortality = ifelse((diff_days < 28 & !is.na(diff_days)) |
                                  RINPERSOONS == "I", 1, 0),
    
    # infant mortality: 24 weeks to < 365 days
    infant_mortality = ifelse((diff_days < 365 & !is.na(diff_days)) |
                                RINPERSOONS == "I", 1, 0)
    
  )



#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))

