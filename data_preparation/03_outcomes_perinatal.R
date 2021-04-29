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
  dplyr::rename(
    gestational_age = zwangerschapsduur,
    perc_10_boys = "0.1_percentile"
    ) %>%
  select(c(gestational_age, perc_10_boys))

girls_weight_tab <- read_excel(loc$girls_weight_data) %>%
  dplyr::rename(
    gestational_age = zwangerschapsduur,
    perc_10_girls = "0.1_percentile"
    ) %>%
  select(c(gestational_age, perc_10_girls))


# function to get latest perined version of specified year
get_prnl_filename <- function(year) {

  fl <- list.files(
    path = file.path(loc$data_folder, "GezondheidWelzijn/PRNL"),
    pattern = paste0("PRN\\s", year, "V\\d{1}.sav"),
    full.names = TRUE
    )
  
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
  
}


# create function for cleaning the perinatal data
CleanPerinatal <- function(file_name) {

  prnl_tab <- read_sav(file_name) %>% 
    mutate(RINPERSOONS_KIND_UITGEBREID = as_factor(RINPERSOONS_KIND_UITGEBREID, levels = "value")) %>%
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

# load perined data 
perined_dat <- tibble(
  RINPERSOONS_KIND_UITGEBREID = factor(),
  RINPERSOON_KIND = integer(),
  Gewichtkind_ruw = double(),
  Amddd = double(),
  Geslachtkind = double(),
  perc_10_boys = double(),
  perc_10_girls = double(),
  low_birthweight = integer(),
  premature_birth = integer(),
)

for (year in seq(format(dmy(cfg$child_birth_date_min), "%Y"), format(dmy(cfg$child_birth_date_max), "%Y"))){
  
  perined_dat <- CleanPerinatal(get_prnl_filename(year)) %>% 
    # add year
    mutate(perined_year = year) %>%
    bind_rows(perined_dat, .)

}


cohort_dat <- left_join(cohort_dat, perined_dat, by = c("RINPERSOON" = "RINPERSOON_KIND",
                                                        "RINPERSOONS" = "RINPERSOONS_KIND_UITGEBREID"))


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))

