# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding high school education outcomes to the cohort.
#
# (c) ODISSEI Social Data Science team 2021

#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(lubridate)

#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "02_predictors.rds"))


#### HIGH SCHOOL ####

# function to get latest hoogsteopl version of specified year
get_school_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "Onderwijs/HOOGSTEOPLTAB/", year),
    pattern = paste0("HOOGSTEOPL", year, "TABV[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

school_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                     education = integer())
for (year in seq.int(cfg$high_school_year_min, cfg$high_school_year_max)) {
  school_dat <- read_sav(get_school_filename(year), 
                         col_select = c("RINPERSOONS", "RINPERSOON", "OPLNIVSOI2016AGG4HGMETNIRWO")) %>%
    rename(education = OPLNIVSOI2016AGG4HGMETNIRWO) %>%
    mutate(
      RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
      education = ifelse(education == "----", NA, education),
      education = as.numeric(education),
      year_age_16 = year
      ) %>%
    # add to income parents
    bind_rows(school_dat, .)
}


# create 16 years old varaible for cohort_dat 
cohort_dat <- cohort_dat %>%
  mutate(year_age_16 = as.integer(format(birthdate, "%Y")) + 16)

cohort_dat <- inner_join(cohort_dat, school_dat, 
                        by = c("RINPERSOON", "RINPERSOONS", "year_age_16"))


# create dummy variables
cohort_dat <- cohort_dat %>%
  # remove category "weet niet of onbekend"
  filter(education != 9999) %>%   
  mutate(
    vmbo_hoog_plus = ifelse(education >= 1220, 1, 0),
    havo_plus = ifelse(education == 1222 | education >= 2130, 1, 0),
    vwo_plus = ifelse(education %in% c(2132, 3113, 3210, 3212, 3213), 1, 0)
  )


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))
