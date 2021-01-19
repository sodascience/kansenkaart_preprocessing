# Kansenkaart data preparation pipeline
#
# 1. Cohort creation. 
#   - Selecting the cohort based on filtering criteria. 
#   - Adding parent information to the cohort.
#   - Adding postal code information to the cohort.
#   - Writing `scratch/01_cohort.rds`.
#
# (c) ODISSEI Social Data Science team 2021

### PACKAGES ###
library(tidyverse)
library(lubridate)
library(haven)

#### CONFIGURATION ####
# load the configuration
cfg <- config::get("data_preparation")
loc <- config::get("file_locations")

# TODO: configuration entries for table locations

#### SELECT COHORT FROM GBA ####
gba_path <- file.path(loc$data_folder, "Bevolking/GBAPERSOONTAB/GBAPERSOON2018TABV2.sav")
gba_dat <-  
  read_sav(gba_path, col_select = c("RINPERSOON", "RINPERSOONS", "GBAGEBOORTELAND", "GBAGESLACHT", 
                                    "GBAGEBOORTEJAAR", "GBAGEBOORTEMAAND", "GBAGEBOORTEDAG", "GBAGENERATIE", 
                                    "GBAHERKOMSTGROEPERING")) %>% 
  mutate(birthdate = ymd(paste(GBAGEBOORTEJAAR, GBAGEBOORTEMAAND, GBAGEBOORTEDAG, sep = "-"))) %>% 
  select(-GBAGEBOORTEJAAR, -GBAGEBOORTEMAAND, -GBAGEBOORTEDAG)

cohort_dat <- 
  gba_dat %>% 
  filter(birthdate >= dmy(cfg$child_birth_date_min), 
         birthdate <= dmy(cfg$child_birth_date_max))

#### LIVE CONTINUOUSLY IN NL FROM 2014 TO 2018 ####
# We only include children who live continuously in the Netherlands between child_live_start and child_live_end.
adres_path <- file.path(loc$data_folder, "Bevolking/GBAADRESOBJECTBUS/GBAADRESOBJECT2018V1.sav")
adres_tab  <- read_sav(adres_path)

start_date  <- dmy(cfg$child_live_start)
end_date    <- dmy(cfg$child_live_end)
cutoff_days <- as.numeric(difftime(end_date, start_date, units = "days")) - cfg$child_live_slack_days

# throw out anything with an end date before start_date, and anything with a start date after end_date
# then also set the start date of everything to start_date, and the end date of everything to end_date
# then compute the timespan of each record
adres_tab <- 
  adres_tab %>% 
  filter(GBADATUMEINDEADRESHOUDING > start_date, GBADATUMAANVANGADRESHOUDING < end_date) %>% 
  mutate(
    recordend   = as_date(ifelse(GBADATUMEINDEADRESHOUDING > end_date, end_date, GBADATUMEINDEADRESHOUDING)),
    recordstart = as_date(ifelse(GBADATUMAANVANGADRESHOUDING < start_date, start_date, GBADATUMAANVANGADRESHOUDING)),
    timespan    = difftime(recordend, recordstart, units = "days")
  )

# group by person and sum the total number of days
# then compute whether this person lived in the Netherlands continuously
days_tab <- 
  adres_tab %>% 
  select(RINPERSOON, timespan) %>% 
  mutate(timespan = as.numeric(timespan)) %>% 
  group_by(RINPERSOON) %>% 
  summarize(total_days = sum(timespan)) %>% 
  mutate(continuous_living = total_days >= cutoff_days) %>% 
  select(RINPERSOON, continuous_living)

# add to cohort and filter
cohort_dat <- 
  left_join(cohort_dat, days_tab, by = "RINPERSOON") %>% 
  filter(continuous_living) %>% 
  select(-continuous_living)

#### PARENT LINK ####
# add parent id to cohort
kindouder_path <- file.path(loc$data_folder, "Bevolking/KINDOUDERTAB/KINDOUDER2018TABV1.sav")
cohort_dat <- left_join(cohort_dat, read_sav(kindouder_path))

# add parents birth dates to cohort
cohort_dat <- 
  cohort_dat %>% 
  left_join(gba_dat %>% select(RINPERSOON, birthdate), by = c("RINPERSOONMa" = "RINPERSOON"), suffix = c("", "_ma")) %>% 
  left_join(gba_dat %>% select(RINPERSOON, birthdate), by = c("RINPERSOONpa" = "RINPERSOON"), suffix = c("", "_pa"))

# filter out children with too old or too young parents
cohort_dat <- 
  cohort_dat %>% 
  mutate(
    age_at_birth_ma = as.period(birthdate - birthdate_ma) / years(1),
    age_at_birth_pa = as.period(birthdate - birthdate_pa) / years(1)
  ) %>% 
  filter(
    age_at_birth_ma >= cfg$parent_min_age, age_at_birth_ma <= cfg$parent_max_age,
    age_at_birth_pa >= cfg$parent_min_age, age_at_birth_pa <= cfg$parent_max_age
  )


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "01_cohort.rds"))

