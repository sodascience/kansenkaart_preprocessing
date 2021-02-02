# Kansenkaart data preparation pipeline
#
# 1. Cohort creation. 
#   - Selecting the cohort based on filtering criteria. 
#   - Adding parent information to the cohort.
#   - Adding postal code / region information to the cohort.
#   - Writing `scratch/01_cohort.rds`.
#
# (c) ODISSEI Social Data Science team 2021

#### PACKAGES ####
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
  select(-GBAGEBOORTEJAAR, -GBAGEBOORTEMAAND, -GBAGEBOORTEDAG) %>% 
  as_factor(only_labelled = TRUE, levels = "labels")

cohort_dat <- 
  gba_dat %>% 
  filter(birthdate %within% interval(dmy(cfg$child_birth_date_min), dmy(cfg$child_birth_date_max)))

#### LIVE CONTINUOUSLY IN NL ####
# We only include children who live continuously in the Netherlands between child_live_start and child_live_end.
adres_path <- file.path(loc$data_folder, "Bevolking/GBAADRESOBJECTBUS/GBAADRESOBJECT2018V1.sav")
adres_tab  <- read_sav(adres_path)

start_date  <- dmy(cfg$child_live_start)
end_date    <- dmy(cfg$child_live_end)
cutoff_days <- as.numeric(difftime(end_date, start_date, units = "days")) - cfg$child_live_slack_days

# throw out anything with an end date before start_date, and anything with a start date after end_date
# then also set the start date of everything to start_date, and the end date of everything to end_date
# then compute the timespan of each record
# TODO: check this once more
adres_tab <- 
  adres_tab %>% 
  filter(GBADATUMEINDEADRESHOUDING %within% interval(start_date, end_date)) %>% 
  as_factor(only_labelled = TRUE, levels = "labels") %>% 
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
cohort_dat <- left_join(
  x = cohort_dat, 
  y = read_sav(kindouder_path) %>% as_factor(only_labelled = TRUE, levels = "labels") %>% select(-RINPERSOONS), 
  by = "RINPERSOON"
)

# add parents birth dates to cohort
cohort_dat <- 
  cohort_dat %>% 
  left_join(gba_dat %>% select(RINPERSOON, birthdate), by = c("RINPERSOONMa" = "RINPERSOON"), suffix = c("", "_ma")) %>% 
  left_join(gba_dat %>% select(RINPERSOON, birthdate), by = c("RINPERSOONpa" = "RINPERSOON"), suffix = c("", "_pa"))

# filter out children with too old or too young parents
cohort_dat <- 
  cohort_dat %>% 
  mutate(
    age_at_birth_ma = interval(birthdate_ma, birthdate) / years(1),
    age_at_birth_pa = interval(birthdate_pa, birthdate) / years(1)
  ) %>% 
  filter(
    age_at_birth_ma >= cfg$parent_min_age, age_at_birth_ma <= cfg$parent_max_age,
    age_at_birth_pa >= cfg$parent_min_age, age_at_birth_pa <= cfg$parent_max_age
  )


#### REGION LINK ####

# find childhood home
adres_path <- file.path(loc$data_folder, "Bevolking/GBAADRESOBJECTBUS/GBAADRESOBJECT2018V1.sav")
adres_tab  <- read_sav(adres_path) %>% as_factor(only_labelled = TRUE, levels = "labels")


if (cfg$childhood_home_first) {
  # take the first address registration to be their childhood home
  home_tab <- 
    adres_tab %>% 
    filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>% 
    arrange(GBADATUMAANVANGADRESHOUDING) %>% 
    group_by(RINPERSOON) %>% 
    summarise(childhood_home = RINOBJECTNUMMER[1])
} else {
  # for each person, throw out the registrations from after they are 18 and select the longest-registered address from 0
  # to 18
  home_tab <- 
    inner_join(adres_tab, cohort_dat %>% select(RINPERSOON, RINPERSOONS, birthdate)) %>% 
    mutate(birthday_cutoff = birthdate + years(cfg$childhood_home_cutoff_year)) %>% 
    filter(GBADATUMAANVANGADRESHOUDING %within% interval(birthdate, birthday_cutoff)) %>% 
    mutate(duration = difftime(min(GBADATUMEINDEADRESHOUDING, birthday_cutoff), GBADATUMAANVANGADRESHOUDING)) %>% 
    group_by(RINPERSOON) %>%  
    arrange(-duration, .by_group = TRUE) %>% 
    summarise(childhood_home = RINOBJECTNUMMER[1])
}

# add childhood home to data
cohort_dat <- left_join(cohort_dat, home_tab)


# clean the postcode table
vslpc_path <- file.path(loc$data_folder, "BouwenWonen/VSLPOSTCODEBUS/VSLPOSTCODEBUSV2020031.sav")
vslpc_tab  <- read_sav(vslpc_path)

# only consider postal codes valid on target_date and create postcode-3 level
vslpc_tab <- 
  vslpc_tab %>% 
  filter(dmy(cfg$postcode_target_date) %within% interval(DATUMAANVPOSTCODENUMADRES, DATUMEINDPOSTCODENUMADRES)) %>% 
  mutate(postcode3 = as.character(floor(as.numeric(POSTCODENUM)/10))) %>% 
  select(RINOBJECTNUMMER, postcode4 = POSTCODENUM, postcode3)

# add the postal codes to the cohort
cohort_dat <- left_join(cohort_dat, vslpc_tab, by = c("childhood_home" = "RINOBJECTNUMMER"))


# add region/neighbourhood codes to cohort
vslgwb_path <- file.path(loc$data_folder, "BouwenWonen/VSLGWBTAB/VSLGWB2019TAB03V1.sav")
vslgwb_tab  <- read_sav(vslgwb_path)

# select region/neighbourhood from the target date
vslgwb_tab <- 
  vslgwb_tab %>% 
  select("childhood_home" = "RINOBJECTNUMMER", 
         "wijk_code"      = paste0("WC", year(dmy(cfg$gwb_target_date))), 
         "buurt_code"     = paste0("BC", year(dmy(cfg$gwb_target_date))))

cohort_dat <- left_join(cohort_dat, vslgwb_tab)


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "01_cohort.rds"))

