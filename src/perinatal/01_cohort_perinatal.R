# Kansenkaart data preparation pipeline
#
# 1. Cohort creation. 
#   - Selecting the cohort based on filtering criteria. 
#   - Adding parent information to the cohort.
#   - Adding postal code / region information to the cohort.
#   - Writing `scratch/01_cohort.rds`.
#
# (c) ODISSEI Social Data Science team 2024



#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(readxl)
library(dplyr)


#### SELECT COHORT FROM PERINED ####
# only consider births where gestational age > 24 weeks.
# drop observations where RIN is unknown
perined_path <- file.path(loc$perined_data)
cohort_dat <- 
  read_rds(perined_path) %>%
  select(-amww) %>%
  mutate_at(c("Rinpersoon_Kind"),~na_if(.,"")) %>% drop_na() %>%
  mutate(gesl = factor(gesl, levels = c("1", "2", "3", "UNK", ""),
                           labels = c("jongen", "meisje", NA, NA, NA)),
         sterfte =  factor(sterfte, levels = c("0", "1", "2", "3", "4", "5", "6"),
                           labels = c("Niet overleden", "Ante partum", "Durante partum", 
                                      "Postpartum 0-7 dagen", "Postpartum 8-28 dagen", 
                                      "Postpartum > 28 dagen", "Fase overlijden onduidelijk")),
         etnic = factor(etnic, levels = c("12", "3", "4", "5", "13", "8", "14", "11", "UNK"),
                        labels = c("Kaukasisch", "Noord-Afrikaans", "Overig Afrikaans", "Turks",
                                   "Hindoestaans", "(Overig) Aziatisch", "Latijns Amerikaans",
                                   "Overig (waaronder gemengd)", "Onbekend")),
         ddgeb = ymd(ddgeb)) %>%
  rename("RINPERSOONS" = "Rinpersoons_Kind",
         "RINPERSOON" = "Rinpersoon_Kind",
         "geslacht" = "gesl",
         "birthdate" = "ddgeb",
         "etniciteit" = "etnic") %>%
  # select birth dates for children 
  filter(birthdate %within% interval(dmy(cfg$child_birth_date_min), 
                                 dmy(cfg$child_birth_date_max)))


# remove gestational age below given days
cohort_dat <- 
  cohort_dat %>%
  filter(!(amddd < cfg$cut_off_days_min), !(amddd > cfg$cut_off_days_max)) 

# record sample size
sample_size <- tibble(n_0_birth_cohort = nrow(cohort_dat))


#### MIGRATION BACKGROUND OF CHILDREN ####

# load gba data for migration background
gba_path <- file.path(loc$data_folder, loc$gba_data)
gba_dat <-  
  read_sav(gba_path, col_select = c("RINPERSOONS", "RINPERSOON", "GBAGESLACHT", 
                                    "GBAGEBOORTEJAAR", "GBAGEBOORTEMAAND", "GBAGEBOORTEDAG", "GBAGENERATIE", 
                                    "GBAHERKOMSTGROEPERING")) %>% 
  mutate(birthdate = dmy(paste(GBAGEBOORTEDAG, GBAGEBOORTEMAAND, GBAGEBOORTEJAAR, sep = "-"))) %>% 
  select(-GBAGEBOORTEJAAR, -GBAGEBOORTEMAAND, -GBAGEBOORTEDAG) %>%
  mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
         GBAHERKOMSTGROEPERING = as_factor(GBAHERKOMSTGROEPERING, levels = "labels"),
         GBAGENERATIE = as_factor(GBAGENERATIE, levels = "labels"),
         GBAGESLACHT = as_factor(GBAGESLACHT), levels = "labels") %>%
  mutate(across(c("GBAHERKOMSTGROEPERING", "GBAGENERATIE", "GBAGESLACHT"),
                as.character)) 
  

# There are some records where RINPERSOONS is not R in perined, but do show up in GBAPersoontab
# by linking by both RINPERSOONS and RINPERSOON we lose these records (391 records in total)
# test <- cohort_dat %>% filter(RINPERSOONS != "R") %>% inner_join(gba_dat, by = "RINPERSOON")

# add infant migration background information to cohort
# should this be an inner_join? That would delete all records not in GBApersoontab,
# while left_join would keep the unmatched records and fill in NA.

# deal with missing genders by replacing missing ones with gender from gbapersoontab.
# does not work, for now use gender variable from GBApersoontab
cohort_dat <- 
  cohort_dat %>% 
  left_join(gba_dat %>% select(RINPERSOONS, RINPERSOON, GBAHERKOMSTGROEPERING, 
                                GBAGENERATIE, GBAGESLACHT), 
             by = c("RINPERSOONS", "RINPERSOON")) %>% 
  mutate(geslacht = as.character(geslacht),
         GBAGESLACHT = as.character(GBAGESLACHT),
         GBAGESLACHT = ifelse(is.na(GBAGESLACHT), geslacht, GBAGESLACHT),
         GBAGESLACHT = ifelse(GBAGESLACHT == "Mannen", "jongen", GBAGESLACHT), 
         GBAGESLACHT = ifelse(GBAGESLACHT == "Vrouwen", "meisje", GBAGESLACHT)
         ) %>%
  select(-geslacht) %>%
  rename("geslacht" = "GBAGESLACHT") %>%
  filter(!is.na(geslacht)) 


#### PARENT LINK ####
# add parent id to cohort   
kindouder_path <- file.path(loc$data_folder, loc$kind_data)
cohort_dat <- 
  cohort_dat %>% 
  inner_join(read_sav(kindouder_path) %>% select(-(XKOPPELNUMMER)) %>%
               as_factor(only_labelled = TRUE, levels = "values"), 
             by = c("RINPERSOONS", "RINPERSOON")) %>%
  filter(!is.na(RINPERSOONMa))

# record sample size
sample_size <- sample_size %>% mutate(n_1_child_parent_link = nrow(cohort_dat))

#### REGION LINK ####
# find childhood home
adres_path <- file.path(loc$data_folder, loc$gbaao_data)
adres_tab <- read_sav(adres_path) %>%
  as_factor(only_labelled = TRUE, levels = "values") %>%
  mutate(GBADATUMAANVANGADRESHOUDING = ymd(GBADATUMAANVANGADRESHOUDING),
         GBADATUMEINDEADRESHOUDING = ymd(GBADATUMEINDEADRESHOUDING))

# keep earliest known address that we observe for the child
home_tab_child <- 
  adres_tab %>% filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  summarize(childhood_home = RINOBJECTNUMMER[1],
            type_childhood_home = SOORTOBJECTNUMMER[1])

# keep address of mom at date of birth for stillbirths
home_tab_mom <- 
  inner_join(adres_tab %>% filter(RINPERSOON %in% cohort_dat$RINPERSOONMa), 
             cohort_dat %>% select(c(RINPERSOONSMa, RINPERSOONMa, birthdate)), 
             by = c("RINPERSOONS" = "RINPERSOONSMa",
                    "RINPERSOON" = "RINPERSOONMa"), 
             relationship = "many-to-many") %>%
  filter(birthdate %within% interval(GBADATUMAANVANGADRESHOUDING, 
                                     GBADATUMEINDEADRESHOUDING)) %>%
  group_by(RINPERSOONS, RINPERSOON) %>% 
  summarise(mom_home = RINOBJECTNUMMER[1],
            type_mom_home = SOORTOBJECTNUMMER[1])
  

# join child home address
cohort_dat <- left_join(cohort_dat, home_tab_child)

# join mother home address for stillbirth children 
cohort_dat <- left_join(cohort_dat, home_tab_mom, 
                         by = c("RINPERSOONSMa" = "RINPERSOONS",
                                "RINPERSOONMa" = "RINPERSOON"), 
                         suffix = c("", "_ma"))


# replace address of stillbirth with those of the mother
cohort_dat <-
  cohort_dat %>%
  mutate(
    type_childhood_home = as.character(type_childhood_home),
    type_mom_home = as.character(type_mom_home),
    childhood_home = ifelse(is.na(childhood_home), mom_home, childhood_home),
    type_childhood_home = ifelse(is.na(type_childhood_home), type_mom_home, type_childhood_home),
    type_childhood_home = as.factor(type_childhood_home)     
    ) %>%
  # this step removed 27 children
  filter(!is.na(childhood_home))
  

# free up memory
rm(adres_tab, home_tab_child, home_tab_mom)


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

# select region/neighbourhood from the target date
vslgwb_tab <- 
  vslgwb_tab %>% 
  select("type_childhood_home" = "SOORTOBJECTNUMMER", 
         "childhood_home"      = "RINOBJECTNUMMER", 
         "gemeente_code"       = paste0("gem", year(dmy(cfg$gwb_target_date))), 
         "wijk_code"           = paste0("wc", year(dmy(cfg$gwb_target_date))), 
         "buurt_code"          = paste0("bc", year(dmy(cfg$gwb_target_date))))

cohort_dat <- inner_join(cohort_dat, vslgwb_tab)

# add corop regions
corop_tab  <- read_excel(loc$corop_data) %>%
  select("gemeente_code" = paste0("GM", year(dmy(cfg$corop_target_date))), 
         "corop_code" = paste0("COROP", year(dmy(cfg$corop_target_date)))) %>%
  unique()

cohort_dat <- inner_join(cohort_dat, corop_tab, by = "gemeente_code") %>%
  select(-c(type_childhood_home, childhood_home, 
            mom_home, type_mom_home))


# free up memory
rm(vslpc_tab, vslgwb_tab, corop_tab)


# mutate factor regions
cohort_dat <- 
  cohort_dat %>%
  mutate(across(c("postcode3", "postcode4", "gemeente_code",
                  "wijk_code", "buurt_code", "corop_code"), as.factor))

# record sample size
sample_size <- sample_size %>% mutate(n_2_region_link = nrow(cohort_dat))


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "01_cohort.rds"))

write_rds(sample_size, file.path(loc$scratch_folder, "01_sample_size.rds"))
