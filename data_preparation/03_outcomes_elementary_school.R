# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding elementary school outcomes to the cohort.
#
# (c) ODISSEI Social Data Science team 2021

#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(lubridate)

#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds("scratch/02_predictors.rds")

# load the configuration
cfg <- config::get("data_preparation")
loc <- config::get("file_locations")


#### ELEMENTARY SCHOOL OUTCOMES ####

# function to get latest inschrwpo version of specified year
get_inschrwpo_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "/Onderwijs/INSCHRWPOTAB"),
    pattern = paste0("INSCHRWPOTAB", year, "V[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


school_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = integer(), WPOLEERJAAR = integer(), 
                     WPOREKENEN = integer(), WPOTAALLV = integer(), WPOTAALTV = integer(), 
                     WPOTOETSADVIES = integer(), WPOADVIESVO = integer(), WPOADVIESHERZ = integer())
                     
for (year in seq(as.integer(cfg$elementary_school_year_min), as.integer(cfg$elementary_school_year_max))) {
  school_dat <- 
      # read file from disk
      read_sav(get_inschrwpo_filename(year), 
               col_select = c("RINPERSOONS", "RINPERSOON", "WPOLEERJAAR", "WPOREKENEN",
                              "WPOTAALLV", "WPOTAALTV", "WPOTOETSADVIES", "WPOADVIESVO",
                              "WPOADVIESHERZ")) %>% 
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
      # select only children that are in the cohort
      filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>% 
      # add year
      mutate(year = year) %>% 
      # add to income children
      bind_rows(school_dat, .)
}

# add to data
cohort_dat <- left_join(cohort_dat, school_dat, by = c("RINPERSOONS", "RINPERSOON"))

# only keep pupils who are in group 8
cohort_dat <- cohort_dat %>%
  filter(WPOLEERJAAR == 8)

sapply(cohort_dat, function(x) sum(is.na(x)))


# create outcome variables
cohort_dat <- cohort_dat %>%
  mutate(
    # rekenen, lezen & taalverzorging
    wpo_math     = ifelse(WPOREKENEN == 3 | WPOREKENEN == 4, 1, 0),
    wpo_reading  = ifelse(WPOTAALLV == 4, 1, 0),
    wpo_language = ifelse(WPOTAALTV == 4, 1, 0),
    
    # cito test outcomes
    vmbo_hoog_plus_test = ifelse(WPOTOETSADVIES %in% c(42, 44, 60, 61, 70), 1, 0),
    havo.plus.test      = ifelse(WPOTOETSADVIES %in% c(60, 61, 70), 1, 0),
    vwo.plus.test       = ifelse(WPOTOETSADVIES == 70, 1, 0),
    
    # final high school advice 
    # replace 80 (= Geen specifiek advies mogelijk) with NA
    WPOADVIESVO = ifelse(WPOADVIESVO == 80, NA, WPOADVIESVO),
    # replace final school advice (wpoadviesvo) with wpoadviesherz if wpoadviesherz is not missing
    final_school_advice = ifelse(!is.na(WPOADVIESHERZ), WPOADVIESHERZ, WPOADVIESVO),
    
    vmbo_hoog_plus_final = ifelse(final_school_advice %in% c(40, 41, 42, 43, 44, 45, 50, 
                                                             51, 52, 53, 60, 61, 70), 1, 0),
    havo_plus_final      = ifelse(final_school_advice %in% c(60, 61, 70), 1, 0),
    vwo_plus_final       = ifelse(WPOTOETSADVIES == 70, 1, 0)
 
  )

# replace NA
cohort_dat <- cohort_dat %>%
  mutate(
    wpo_math     = ifelse(is.na(WPOREKENEN), NA, wpo_math),
    wpo_reading  = ifelse(is.na(WPOTAALLV), NA, wpo_reading),
    wpo_language = ifelse(is.na(WPOTAALTV), NA, wpo_language),
    
    # cito test outcomes
    vmbo_hoog_plus_test = ifelse(is.na(WPOTOETSADVIES), NA, vmbo_hoog_plus_test),
    havo.plus.test      = ifelse(is.na(WPOTOETSADVIES), NA, havo.plus.test),
    vwo.plus.test       = ifelse(is.na(WPOTOETSADVIES), NA, vwo.plus.test),
    
    # final school advice outcomes
    vmbo_hoog_plus_final = ifelse(is.na(final_school_advice), NA, vmbo_hoog_plus_final),
    havo_plus_final      = ifelse(is.na(final_school_advice), NA, havo_plus_final),
    vwo_plus_final       = ifelse(is.na(final_school_advice), NA, vwo_plus_final)
    
  )


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))

