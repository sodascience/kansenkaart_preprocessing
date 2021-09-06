# Kansenkaart data preparation pipeline
#
# 1. Cohort creation. 
#   - Selecting the cohort based on filtering criteria.
#   - Writing `scratch/01_cohort.rds`.
#
# (c) ODISSEI Social Data Science team 2021


#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(readxl)

#### PERINED ####

# function to get latest perined version of specified year
get_prnl_filename <- function(year) {
  fl <- list.files(
    path = file.path("G:/GezondheidWelzijn/PRNL/", year, "/"), 
    pattern = paste0(year, "V[0-9]+(?i)(.sav)"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


# create perinatal data
cohort_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                      rinpersoons_moeder = factor(), rinpersoon_moeder = character(),
                      herkomst = factor(), generatie = factor(), leeftijdmoeder = double(),
                      amddd = double(), datumkind = double(), geslachtkind = factor(), 
                      gewichtkind_ruw = double(), meerling = factor(), 
                      sterfte = factor(), year = integer())
for (year in seq(format(dmy(cfg$child_birth_date_min), "%Y"), 
                 format(dmy(cfg$child_birth_date_max), "%Y"))){
  
  cohort_dat <- read_sav(get_prnl_filename(year), col_select = 
                            c("rinpersoons_kind_uitgebreid", "rinpersoon_kind", 
                              "rinpersoons_moeder", "rinpersoon_moeder",
                              "herkomst", "generatie", "leeftijdmoeder",
                              "amddd", "datumkind", "geslachtkind", "gewichtkind_ruw", 
                              "meerling", "sterfte")) %>% 
    rename(RINPERSOONS = rinpersoons_kind_uitgebreid,
           RINPERSOON = rinpersoon_kind) %>%
    mutate(
      RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
      rinpersoons_moeder = as_factor(rinpersoons_moeder, levels = "values"),
      geslachtkind = tolower(as_factor(geslachtkind, levels = "labels")),
      meerling = tolower(as_factor(meerling, levels = "labels")),
      sterfte = tolower(as_factor(sterfte, levels = "labels")), 
      herkomst = tolower(as_factor(herkomst, levels = "labels")), 
      generatie = tolower(as_factor(generatie, levels = "labels")), 
      year = year) %>%
    bind_rows(cohort_dat, .)
  
}

# remove gestational age below given days
cohort_dat <- cohort_dat %>%
  unique() %>%
  filter(!(amddd < cfg$cut_off_days_min),
         !(amddd > cfg$cut_off_days_max))


#### DO AND DOODOORZTAB ####

## CLEAN DO ##

# function to get latest do version of specified year 
get_do_filename <- function(year) {
  fl <- list.files(
    path = file.path("G:/GezondheidWelzijn/DO/"), 
    pattern = paste0("DO", year, "V[0-9]+(?i)(.sav)"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

# function to get latest do version of specified year 
get_do_map_filename <- function(year) {
  fl <- list.files(
    path = file.path("G:/GezondheidWelzijn/DO/", year, "/"), 
    pattern = paste0("DO ", year, "V[0-9]+(?i)(.sav)"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

death_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                    date_of_death = character(), year = as.numeric())
for (year in seq(format(dmy(cfg$child_birth_date_min), "%Y"),
                 format(dmy(cfg$child_birth_date_max), "%Y"))) {
  
  if (year <= 2011) {
    death_dat <- read_sav(get_do_filename(year), 
                          col_select = c("rinpersoons", "rinpersoon",
                                         "ovljr", "ovlmnd", "ovldag")) %>%
      # convert to uppercase
      rename_all(toupper) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
             date_of_death = paste(OVLJR, OVLMND, OVLDAG, sep = "-"),
             year = year, 
             RINPERSOON = ifelse(RINPERSOON == "", NA, RINPERSOON)) %>%
      select(-c(OVLJR, OVLMND, OVLDAG)) %>%
      # add to death data
      bind_rows(death_dat, .)
    
  } else if (year == 2012) {
    death_dat <- read_sav(get_do_map_filename(year), 
                          col_select = c("rinpersoons", "rinpersoon",
                                         "ovljr", "ovlmnd", "ovldag")) %>%
      # convert to uppercase
      rename_all(toupper) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
             date_of_death = paste(OVLJR, OVLMND, OVLDAG, sep = "-"),
             year = year, 
             RINPERSOON = ifelse(RINPERSOON == "", NA, RINPERSOON)) %>%
      select(-c(OVLJR, OVLMND, OVLDAG)) %>%
      # add to death data
      bind_rows(death_dat, .)
  }
}

# post_processing
death_dat <- death_dat %>%
  mutate(date_of_death = ymd(date_of_death))


## CLEAN DOODOORZTAB ##

# function to get latest doodoorztab version of specified year 
get_dood_filename <- function(year) {
  fl <- list.files(
    path = file.path("G:/GezondheidWelzijn/DOODOORZTAB/", year, "/"),
    pattern = paste0("DOODOORZ", year, "TABV[0-9]+(?i)(.sav)"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

# function to get latest gbaoverlijdenstab version of specified year 
get_gba_filename <- function(year) {
  fl <- list.files(
    path = file.path("G:/Bevolking/GBAOVERLIJDENTAB/", year, "/"),
    pattern = "(?i)(.sav)",
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


dood_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), year = numeric())
gba_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), date_of_death = character())
for (year in seq(2013, format(dmy(cfg$child_birth_date_max), "%Y"))) {
  
  dood_dat <- read_sav(get_dood_filename(year), 
                       col_select = c("RINPERSOONS", "RINPERSOON")) %>%
    mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
           year = year) %>%
    # add to death data
    bind_rows(dood_dat, .)
  
  gba_dat <- read_sav(get_gba_filename(year)) %>%
    rename(date_of_death = GBADatumOverlijden) %>%
    mutate(
      RINPERSOONS = as_factor(RINPERSOONS, levels = "value")
    ) %>%
    # add to death data
    bind_rows(gba_dat, .)
}

# post-processing
dood_dat <- inner_join(dood_dat, gba_dat) %>%
  mutate(date_of_death = ymd(date_of_death))


# create one death data
death_dat <- rbind(death_dat, dood_dat) %>%
  select(-year) %>%
  unique() %>%
  distinct(RINPERSOONS, RINPERSOON, .keep_all = TRUE)
rm(dood_dat, gba_dat)


cohort_dat <- left_join(cohort_dat, death_dat) 

# free up memory
rm(death_dat)


# create diffence between birth and death date
cohort_dat <- cohort_dat %>%
  mutate(
    datumkind = ymd(datumkind),
    diff_days = as.numeric(difftime(date_of_death, datumkind, units = "days"))
  ) 


# filter out children with too old or too young mothers
cohort_dat <- cohort_dat %>%
  filter(leeftijdmoeder >= cfg$parent_min_age &
           leeftijdmoeder <= cfg$parent_max_age)


#### REGION LINK ####

# find childhood home
adres_path <- file.path(loc$data_folder, loc$gbaao_data)
adres_tab  <- read_sav(adres_path) %>%
  as_factor(only_labelled = TRUE, levels = "values") %>%
  mutate(
    GBADATUMAANVANGADRESHOUDING = ymd(GBADATUMAANVANGADRESHOUDING),
    GBADATUMEINDEADRESHOUDING = ymd(GBADATUMEINDEADRESHOUDING)
  )


# take the address registration to be their childhood home at date of birth
home_tab <- 
  adres_tab %>% 
  inner_join(cohort_dat %>% select(c(RINPERSOONS, RINPERSOON, datumkind)),
             by = c("RINPERSOONS", "RINPERSOON")) %>%
  filter(datumkind %within% interval(GBADATUMAANVANGADRESHOUDING, GBADATUMEINDEADRESHOUDING)) %>%
  group_by(RINPERSOONS, RINPERSOON) %>% 
  summarise(childhood_home = RINOBJECTNUMMER[1],
            type_childhood_home = SOORTOBJECTNUMMER[1])
  

mom_home_tab <- 
  adres_tab %>% 
  inner_join(cohort_dat %>% select(c(rinpersoons_moeder, rinpersoon_moeder, datumkind)), 
             by = c("RINPERSOONS" = "rinpersoons_moeder",
                    "RINPERSOON" = "rinpersoon_moeder")) %>%
  filter(datumkind %within% interval(GBADATUMAANVANGADRESHOUDING, GBADATUMEINDEADRESHOUDING)) %>%
  group_by(RINPERSOONS, RINPERSOON) %>% 
  summarise(mom_home = RINOBJECTNUMMER[1],
            type_mom_home = SOORTOBJECTNUMMER[1])
  
 
# add childhood home to data
cohort_dat <- left_join(cohort_dat, home_tab, by = c("RINPERSOONS", "RINPERSOON"))

            
# add mom home to data
cohort_dat <- left_join(cohort_dat, mom_home_tab, 
                          by = c("rinpersoons_moeder" = "RINPERSOONS",
                                 "rinpersoon_moeder" = "RINPERSOON"))


# replace homelink if NA with mom homelink
cohort_dat <- cohort_dat %>%
  mutate(
    across(c(type_childhood_home, type_mom_home), as.character)
    ) %>%
  mutate(
    childhood_home = ifelse(is.na(childhood_home), mom_home, childhood_home),
    type_childhood_home = ifelse(is.na(type_childhood_home), 
                                 type_mom_home, type_childhood_home),
    type_childhood_home = factor(type_childhood_home, 
                                 levels = c("B", "H", "D", "O"))
  ) %>%
  filter(!is.na(childhood_home)) %>%
  select(-c(mom_home, type_mom_home))


# free up memory
rm(adres_tab, home_tab, mom_home_tab)


# clean the postcode table
vslpc_path <- file.path(loc$data_folder, loc$postcode_data)
vslpc_tab  <- read_sav(vslpc_path) %>%
  mutate(
    SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"),
    DATUMAANVPOSTCODENUMADRES = ymd(DATUMAANVPOSTCODENUMADRES),
    DATUMEINDPOSTCODENUMADRES = ymd(DATUMEINDPOSTCODENUMADRES),
    POSTCODENUM = ifelse(POSTCODENUM == "----", NA, POSTCODENUM)
  )


# only consider postal codes valid on target_date and create postcode-3 level
vslpc_tab <- 
  vslpc_tab %>% 
  filter(dmy(cfg$postcode_target_date) %within% interval(DATUMAANVPOSTCODENUMADRES, DATUMEINDPOSTCODENUMADRES)) %>% 
  mutate(postcode3 = as.character(floor(as.numeric(POSTCODENUM)/10))) %>% 
  select(SOORTOBJECTNUMMER, RINOBJECTNUMMER, postcode4 = POSTCODENUM, postcode3)


# add the postal codes to the cohort
cohort_dat <- inner_join(cohort_dat, vslpc_tab, by = c("type_childhood_home" = "SOORTOBJECTNUMMER", 
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

cohort_dat <- inner_join(cohort_dat, vslgwb_tab,
                         by = c("childhood_home", "type_childhood_home"))

# add corop regions
corop_tab  <- read_excel(loc$corop_data) %>%
  select("gemeente_code" = paste0("GM", year(dmy(cfg$gwb_target_date))), 
         "corop_code" = paste0("COROP", year(dmy(cfg$gwb_target_date)))) %>%
  unique()

cohort_dat <- inner_join(cohort_dat, corop_tab, by = "gemeente_code")

# free up memory
rm(vslpc_tab, vslgwb_tab, corop_tab)


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "01_cohort.rds"))
