# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding students outcomes to the cohort.
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



#### EDUCATION ####

get_hoogstopl_filename <- function(year) {
# get all HOOGSTEOPLTAB files with the specified year
fl <- list.files(
  path = file.path(loc$data_folder, "Onderwijs/HOOGSTEOPLTAB", year),
  pattern = "(?i)(.sav)", 
  full.names = TRUE
)
# return only the latest version
sort(fl, decreasing = TRUE)[1]
}


education_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(),
                        education_attained = factor(), education_followed = factor(), 
                        year = integer())
for (year in seq(as.integer(cfg$education_year_min), as.integer(cfg$education_year_max))) {

  if (year < 2019) {
    education_dat <- read_sav(get_hoogstopl_filename(year),
                              col_select = c("RINPERSOONS", "RINPERSOON", "OPLNIVSOI2016AGG4HGMETNIRWO", 
                                             "OPLNIVSOI2016AGG4HBMETNIRWO")) %>%
      rename(education_attained = OPLNIVSOI2016AGG4HBMETNIRWO, 
             education_followed = OPLNIVSOI2016AGG4HGMETNIRWO) %>%
      mutate(
        RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
        education_attained = ifelse(education_attained == "----", NA, education_attained), 
        education_followed = ifelse(education_followed == "----", NA, education_followed), 
        year = year
      ) %>%
      # add to data 
      bind_rows(education_dat, .)
    
  } else if (year >= 2019) {
    education_dat <- read_sav(get_hoogstopl_filename(year),
                              col_select = c("RINPERSOONS", "RINPERSOON", "OPLNIVSOI2021AGG4HBmetNIRWO", 
                                             "OPLNIVSOI2021AGG4HGmetNIRWO")) %>%
      rename(education_attained = OPLNIVSOI2021AGG4HBmetNIRWO, 
             education_followed = OPLNIVSOI2021AGG4HGmetNIRWO) %>%
      mutate(
        RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
        education_attained = ifelse(education_attained == "----", NA, education_attained), 
        education_followed = ifelse(education_followed == "----", NA, education_followed), 
        year = year
      ) %>%
      # add to data 
      bind_rows(education_dat, .)
    
  }
}


# add year at which the child is a specific age to the cohort
# age T if child is born between jan-sept
# age T + 1 if the child is born between oct-dec
cohort_dat <- 
  cohort_dat %>%
  mutate(
    year = year(birthdate), 
    year = ifelse(month(birthdate) %in% seq(1:9), (year + cfg$outcome_age), 
                  (year + cfg$outcome_age + 1)))

# join to cohort
cohort_dat <- 
  cohort_dat %>%
  left_join(education_dat, 
            by = c("RINPERSOONS", "RINPERSOON", "year")) 


#### OUTCOMES ####

# create outcomes
cohort_dat <- 
  cohort_dat %>%
  mutate(
    high_school_attained = ifelse(education_attained >= 2110, 1, 0),
    hbo_followed        = ifelse(education_followed >= 3110, 1, 0),
    uni_followed        = ifelse(education_followed %in% c(3113, 3212, 3213), 1, 0),
    
    high_school_attained = ifelse(is.na(high_school_attained), 0, high_school_attained),
    hbo_followed         = ifelse(is.na(hbo_followed), 0, hbo_followed),
    uni_followed         = ifelse(is.na(uni_followed), 0, uni_followed)
  ) %>%
  select(-c(education_attained, education_followed))

rm(education_dat)


#### PREFIX ####

# add prefix to outcomes
outcomes <- c("high_school_attained", "hbo_followed", "uni_followed")
suffix <- "c21_"


# rename outcomes
cohort_dat <- 
  cohort_dat %>%
  rename_with(~str_c(suffix, .), .cols = all_of(outcomes)) %>% 
  ungroup() 


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))



