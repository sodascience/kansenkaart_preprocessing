# Kansenkaart data preparation pipeline
#
# 1. Cohort creation. 
#   - Selecting the cohort based on filtering criteria. 
#   - Adding parent information to the cohort.
#   - Adding postal code / region information to the cohort.
#   - Writing `scratch/01_cohort.rds`.
#
# (c) ODISSEI Social Data Science team 2025




#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(readxl)
library(dplyr)


#### SELECT COHORT FROM GBA ####
#gba_path <- file.path(loc$data_folder, loc$gba_data)
gba_path <- 'G:/Bevolking/GBAPERSOONTAB/2018/GBAPERSOON2018TABV2.sav'
gba_dat <-  
  read_sav(gba_path, col_select = c("RINPERSOONS", "RINPERSOON", "GBAGESLACHT", 
                                    "GBAGEBOORTEJAAR", "GBAGEBOORTEMAAND", "GBAGEBOORTEDAG", "GBAGENERATIE", 
                                    "GBAHERKOMSTGROEPERING", "GBAGEBOORTELAND")) %>% 
  mutate(birthdate = dmy(paste(GBAGEBOORTEDAG, GBAGEBOORTEMAAND, GBAGEBOORTEJAAR, sep = "-"))) %>% 
  select(-GBAGEBOORTEJAAR, -GBAGEBOORTEMAAND, -GBAGEBOORTEDAG) %>%
  mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
         GBAHERKOMSTGROEPERING = as_factor(GBAHERKOMSTGROEPERING, levels = "labels"),
         GBAGEBOORTELAND = as_factor(GBAGEBOORTELAND, levels = "labels"),
         GBAGENERATIE = as_factor(GBAGENERATIE, levels = "labels"),
         GBAGESLACHT = as_factor(GBAGESLACHT, levels = "labels")) %>%
  mutate(GBAGESLACHT = as.factor(as.character(GBAGESLACHT))) %>%
  rename(sex = GBAGESLACHT) %>%
  mutate(sex = recode(sex, 'Mannen' = 'Men', 'Vrouwen' = 'Women'))


cohort_dat <- 
  gba_dat %>% 
  filter(birthdate %within% interval(dmy(cfg$child_birth_date_min), dmy(cfg$child_birth_date_max)))


# record sample size
sample_size <- tibble(n_0_birth_cohort = nrow(cohort_dat))

# SAVE STARTING COHORT
write_rds(cohort_dat, file.path(loc$scratch_folder, "00_start_cohort.rds"))



#### PARENT LINK ####
# add parent id to cohort
kindouder_path <- file.path(loc$data_folder, loc$kind_data)
cohort_dat <- left_join(
  x = cohort_dat, 
  y = read_sav(kindouder_path) %>% select(-(XKOPPELNUMMER)) %>% 
    as_factor(only_labelled = TRUE, levels = "values"),
  by = c("RINPERSOONS", "RINPERSOON")
)


# create sample for those that cannot be matched to their parents
cohort_dat_no_parents <- 
  cohort_dat %>%
  filter(is.na(RINPERSOONSMa) & is.na(RINPERSOONSpa))

# SAVE STARTING COHORT
write_rds(cohort_dat_no_parents, file.path(loc$scratch_folder, "00_cohort_no_parents.rds"))
rm(cohort_dat_no_parents)


# remove children without parents
cohort_dat <-
  cohort_dat %>%
  filter(!is.na(RINPERSOONSMa) & !is.na(RINPERSOONSpa))

# record sample size
sample_size <- sample_size %>% mutate(n_1_child_parent_link = nrow(cohort_dat))


#### AGE PARENTS AT BIRTH ####

parents_age <-
  gba_dat %>%
  filter(RINPERSOON %in% c(cohort_dat$RINPERSOONMa, cohort_dat$RINPERSOONpa)) %>%
  select("RINPERSOONS", "RINPERSOON", "birthdate")


# add parents date to child birth date
cohort_dat <-
  cohort_dat %>%
  left_join(parents_age %>% rename(birthdate_ma = birthdate), 
            c("RINPERSOONSMa" = "RINPERSOONS",
              "RINPERSOONMa" = "RINPERSOON"))
cohort_dat <-
  cohort_dat %>%
  left_join(parents_age %>% rename(birthdate_pa = birthdate), 
            c("RINPERSOONSpa" = "RINPERSOONS",
              "RINPERSOONpa" = "RINPERSOON"))
  

# age of parents at child birth
cohort_dat <-
  cohort_dat %>%
  mutate(age_ma_at_birth = as.numeric(birthdate - birthdate_ma) %/% 365.25, 
         age_pa_at_birth = as.numeric(birthdate - birthdate_pa) %/% 365.25)

rm(parents_age, gba_dat)


#### REGION LINK ####

# find childhood home
adres_path <- file.path(loc$data_folder, loc$gbaao_data)
adres_tab  <- read_sav(adres_path) %>% 
  as_factor(only_labelled = TRUE, levels = "values") %>%
  mutate(
    GBADATUMAANVANGADRESHOUDING = ymd(GBADATUMAANVANGADRESHOUDING),
    GBADATUMEINDEADRESHOUDING = ymd(GBADATUMEINDEADRESHOUDING)
  )


# take date at which the child is a specific age
age_tab <- cohort_dat %>%
  select(RINPERSOONS, RINPERSOON, birthdate) %>%
  mutate(home_address_date = birthdate %m+% years(cfg$childhood_home_age)) %>%
  select(-birthdate)

# take the address registration on a specific age
home_tab <-
  adres_tab %>%
  filter(RINPERSOON %in% cohort_dat$RINPERSOON & RINPERSOONS %in% cohort_dat$RINPERSOONS) %>%
  # add home_address_age to dat
  left_join(age_tab, by = c("RINPERSOONS", "RINPERSOON")) %>%
  # take addresses that are still open on a specific date
  filter(
    GBADATUMAANVANGADRESHOUDING <= home_address_date &
      GBADATUMEINDEADRESHOUDING >= home_address_date
  ) %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  summarise(
    childhood_home = RINOBJECTNUMMER[1],
    type_childhood_home = SOORTOBJECTNUMMER[1])



# add childhood home to data
cohort_dat <- inner_join(cohort_dat, home_tab)

# free up memory
rm(adres_tab, home_tab, age_tab)


# clean the postcode table
vslpc_path <- file.path(loc$data_folder, loc$postcode_data)
vslpc_tab  <- read_sav(vslpc_path) %>%
  mutate(
    SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"),
    DATUMAANVPOSTCODENUMADRES = ymd(DATUMAANVPOSTCODENUMADRES),
    DATUMEINDPOSTCODENUMADRES = ymd(DATUMEINDPOSTCODENUMADRES),
    POSTCODENUM = ifelse(POSTCODENUM == "----", NA, POSTCODENUM)
    ) %>%
  filter(!is.na(POSTCODENUM))

# only consider postal codes valid on target_date and create postcode-3 level
vslpc_tab <- 
  vslpc_tab %>% 
  filter(dmy(cfg$postcode_target_date) %within% interval(DATUMAANVPOSTCODENUMADRES, DATUMEINDPOSTCODENUMADRES)) %>% 
  mutate(postcode3 = as.character(floor(as.numeric(POSTCODENUM)/10))) %>% 
  select(SOORTOBJECTNUMMER, RINOBJECTNUMMER, postcode4 = POSTCODENUM, postcode3)

# add the postal codes to the cohort
cohort_dat <- inner_join(cohort_dat, vslpc_tab, 
                         by = c("type_childhood_home" = "SOORTOBJECTNUMMER", 
                                "childhood_home" = "RINOBJECTNUMMER"))

# add region/neighbourhood codes to cohort
vslgwb_path <- file.path(loc$data_folder, loc$vslgwb_data) 
vslgwb_tab  <- read_sav(vslgwb_path) %>%
  mutate(SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"))
  

# select region/neighborhood from the target date
vslgwb_tab <- 
  vslgwb_tab %>% 
  select("type_childhood_home" = "SOORTOBJECTNUMMER", 
         "childhood_home"      = "RINOBJECTNUMMER", 
         "municipality_code"       = paste0("gem", year(dmy(cfg$gwb_target_date))), 
         "neighborhood_code"           = paste0("wc", year(dmy(cfg$gwb_target_date)))
         ) %>%
  mutate(municipality_code = ifelse(municipality_code == "----", NA, municipality_code)
         ) %>%
  filter(!is.na(municipality_code))


cohort_dat <- inner_join(cohort_dat, vslgwb_tab)

# add corop regions
corop_tab  <- read_excel(loc$corop_data) %>%
  select("municipality_code" = paste0("GM", year(dmy(cfg$corop_target_date))), 
         "corop_code" = paste0("COROP", year(dmy(cfg$corop_target_date)))) %>%
  unique()

cohort_dat <- left_join(cohort_dat, corop_tab, by = "municipality_code") %>%
  select(-c(type_childhood_home, childhood_home))


# free up memory
rm(vslpc_tab, vslgwb_tab, corop_tab)

# mutate factor regions
cohort_dat <- 
  cohort_dat %>% 
  mutate(across(c("postcode3", "postcode4", "municipality_code", 
                  "neighborhood_code", "corop_code"), as.factor))

# record sample size
sample_size <- sample_size %>% mutate(n_2_region_link = nrow(cohort_dat))

#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "01_cohort.rds"))

write_rds(sample_size, file.path(loc$scratch_folder, "01_sample_size.rds"))

