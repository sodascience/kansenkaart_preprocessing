# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding high school education outcomes to the cohort.
#
# (c) ODISSEI Social Data Science team 2025


#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(readxl)


#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "02_predictors.rds"))

sample_size <- read_rds(file.path(loc$scratch_folder, "02_sample_size.rds"))



#### LIVE CONTINUOUSLY IN NL ####

# We only include children who live continuously in the Netherlands 
adres_path <- file.path(loc$data_folder, loc$gbaao_data)
adres_tab  <- read_sav(adres_path) %>%
  # select only children
  filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>% 
  mutate(
    RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
    GBADATUMAANVANGADRESHOUDING = ymd(GBADATUMAANVANGADRESHOUDING),
    GBADATUMEINDEADRESHOUDING = ymd(GBADATUMEINDEADRESHOUDING)
  ) %>%
  select(-c(SOORTOBJECTNUMMER, RINOBJECTNUMMER))


# residency requirement for people between child_live_start and child_live_end
residency_tab <- 
  cohort_dat %>%
  select(RINPERSOONS, RINPERSOON, birthdate) %>%
  mutate(start_date = ymd(paste0(year(birthdate), "-01-01")) %m+% years(cfg$child_live_age), 
         end_date = ymd(paste0(year(birthdate), "-12-31")) %m+% years(cfg$child_live_age),
         cutoff_days = as.numeric(difftime(end_date, start_date, units = "days")) - 
           cfg$child_live_slack_days) %>%
  select(-birthdate)


# throw out anything with an end date before start_date, and anything with a start date after end_date
# then also set the start date of everything to start_date, and the end date of everything to end_date
# then compute the timespan of each record
adres_tab <- 
  adres_tab %>% 
  inner_join(residency_tab, by = c("RINPERSOONS", "RINPERSOON")) %>%
  filter(!(GBADATUMEINDEADRESHOUDING < start_date),
         !(GBADATUMAANVANGADRESHOUDING > end_date)) %>%
  mutate(
    recordstart = as_date(ifelse(GBADATUMAANVANGADRESHOUDING < start_date, start_date, GBADATUMAANVANGADRESHOUDING)),
    recordend   = as_date(ifelse(GBADATUMEINDEADRESHOUDING > end_date, end_date, GBADATUMEINDEADRESHOUDING)) ,
    timespan    = difftime(recordend, recordstart, units = "days")
  ) %>%
  select(-c(GBADATUMAANVANGADRESHOUDING, GBADATUMEINDEADRESHOUDING, 
            start_date, end_date))

# group by person and sum the total number of days
# then compute whether this person lived in the Netherlands continuously
days_tab <- 
  adres_tab %>% 
  select(RINPERSOONS, RINPERSOON, timespan, cutoff_days) %>% 
  mutate(timespan = as.numeric(timespan)) %>% 
  group_by(RINPERSOONS, RINPERSOON, cutoff_days) %>% 
  summarize(total_days = sum(timespan)) %>%  
  mutate(continuous_living = total_days >= cutoff_days) %>% 
  select(RINPERSOONS, RINPERSOON, continuous_living)

# add to cohort and filter
cohort_dat <- 
  left_join(cohort_dat, days_tab, by = c("RINPERSOONS", "RINPERSOON")) %>% 
  filter(continuous_living) %>% 
  select(-continuous_living)

# free up memory
rm(adres_tab, days_tab, residency_tab)

sample_size <- sample_size %>% 
  mutate(n_5_child_residency = nrow(cohort_dat))


# create a table with incomes at the cpi_base_year level
# first, load consumer price index data (2015 = 100)
# source: CBS statline
cpi_tab <- read_excel(loc$cpi_index_data) %>% 
  mutate(year = as.numeric(year))

# set cpi_base_year = 100
cpi_tab <- cpi_tab %>%
  mutate(
    cpi = cpi / cpi_tab %>% filter(year == cfg$cpi_base_year) %>% pull(cpi) * 100)


#### HIGH SCHOOL ####

# function to get latest hoogsteopl version of specified year
get_school_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "Onderwijs/HOOGSTEOPLTAB", year),
    pattern = paste0("HOOGSTEOPL", year, "TABV[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

school_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                     education = integer())
for (year in seq.int(cfg$high_school_year_min, cfg$high_school_year_max)) {
  
  if (year <= 2018) {
    school_dat <- read_sav(get_school_filename(year), 
                           col_select = c("RINPERSOONS", "RINPERSOON", "OPLNIVSOI2016AGG4HGMETNIRWO")) %>%
      rename(education = OPLNIVSOI2016AGG4HGMETNIRWO) %>%
      mutate(
        RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
        education = ifelse(education == "----", NA, education),
        education = as.numeric(education),
        year = year
      ) %>%
      # add to data
      bind_rows(school_dat, .)
    
    
  } else if (year >= 2019) {
    school_dat <- read_sav(get_school_filename(year), 
                           col_select = c("RINPERSOONS", "RINPERSOON", 
                                          "OPLNIVSOI2021AGG4HGmetNIRWO")) %>%
      rename(education = OPLNIVSOI2021AGG4HGmetNIRWO) %>%
      mutate(
        RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
        education = ifelse(education == "----", NA, education),
        education = as.numeric(education),
        year = year
      ) %>%
      # add to data
      bind_rows(school_dat, .)
  }
}


# create 16 years old varaible for cohort_dat 
cohort_dat <- cohort_dat %>%
  mutate(year = as.integer(format(birthdate, "%Y")) + 16)


cohort_dat <- inner_join(cohort_dat, school_dat, 
                         by = c("RINPERSOON", "RINPERSOONS", "year"))


# create dummy variables
cohort_dat <- 
  cohort_dat %>%
  # remove category "weet niet of onbekend"
  filter(education != 9999) %>%   
  mutate(
    vmbo_gl = ifelse(education >= 1220, 1, 0),
    havo    = ifelse(education == 1222 | education >= 2130, 1, 0),
    vwo     = ifelse(education %in% c(2132, 3113, 3210, 3212, 3213), 1, 0)
  ) %>%
  select(-education)


rm(school_dat)


#### YOUTH HEALTH COSTS ####


# create a table with incomes at the cpi_base_year level
# first, load consumer price index data (2015 = 100)
# source: CBS statline
cpi_tab <- read_excel(loc$cpi_index_data) %>% 
  mutate(year = as.numeric(year))

# set cpi_base_year = 100
cpi_tab <- cpi_tab %>%
  mutate(
    cpi = cpi / cpi_tab %>% filter(year == cfg$cpi_base_year) %>% pull(cpi) * 100)



# function to get latest inschrwpo version of specified year
get_health_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "GezondheidWelzijn/ZVWZORGKOSTENTAB", year),
    pattern = paste0("ZVWZORGKOSTEN", year, "TABV[0-9]+(?i)(.sav)"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


health_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                     youth_health_costs = double(), year = integer())
for (year in seq(as.integer(cfg$high_school_year_min), as.integer(cfg$high_school_year_max))) {
  
  if (year == 2014) {
    health_tab <- read_sav(get_health_filename(2014),
                           col_select = c("RINPERSOONS", "RINPERSOON", "ZVWKHUISARTS", "ZVWKGEBOORTEZORG", 
                                          "ZVWKFARMACIE", "ZVWKMONDZORG", "ZVWKZIEKENHUIS",        
                                          "ZVWKPARAMEDISCH", "ZVWKHULPMIDDEL", "ZVWKZIEKENVERVOER", 
                                          "ZVWKBUITENLAND", "ZVWKOVERIG", "ZVWKGERIATRISCH", 
                                          "ZVWKWYKVERPLEGING", "NOPZVWKHUISARTSINSCHRIJF")) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values")) %>%
      # select only children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON)
    
  } else if (year == 2015 | year == 2016 | year == 2017) {
    health_tab <- read_sav(get_health_filename(year),
                           col_select = c("RINPERSOONS", "RINPERSOON", "ZVWKHUISARTS", "ZVWKMULTIDISC",
                                          "ZVWKFARMACIE", "ZVWKMONDZORG", "ZVWKZIEKENHUIS", "ZVWKGEBOORTEZORG",        
                                          "ZVWKPARAMEDISCH", "ZVWKHULPMIDDEL", "ZVWKZIEKENVERVOER", 
                                          "ZVWKBUITENLAND", "ZVWKOVERIG", "ZVWKGERIATRISCH", 
                                          "ZVWKWYKVERPLEGING", "NOPZVWKHUISARTSINSCHRIJF")) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values")) %>%
      # select only children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON)
    
  } else if (year >= 2018) {
    health_tab <- read_sav(get_health_filename(year),
                           col_select = c("RINPERSOONS", "RINPERSOON", "ZVWKHUISARTS", "ZVWKMULTIDISC",
                                          "ZVWKFARMACIE", "ZVWKMONDZORG", "ZVWKZIEKENHUIS", "ZVWKEERSTELIJNSVERBLIJF",        
                                          "ZVWKPARAMEDISCH", "ZVWKHULPMIDDEL", "ZVWKZIEKENVERVOER", 
                                          "ZVWKBUITENLAND", "ZVWKOVERIG", "ZVWKGERIATRISCH", "ZVWKGEBOORTEZORG",   
                                          "ZVWKWYKVERPLEGING", "NOPZVWKHUISARTSINSCHRIJF")) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values")) %>%
      # select only children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON)
  }
  
  health_tab <- health_tab %>%
    # replace negative values with 0
    mutate(across(grep("^ZVWK", names(health_tab), value = TRUE),
                  function(x) ifelse(x < 0, 0, x))) %>%
    # sum of all healthcare costs
    mutate(
      youth_health_costs = rowSums(across(grep("^ZVWK", names(health_tab), value = TRUE)), na.rm = TRUE),
      youth_health_costs = youth_health_costs - (NOPZVWKHUISARTSINSCHRIJF + ZVWKGEBOORTEZORG), 
      year = year # add year
    ) %>%
    select(RINPERSOONS, RINPERSOON, youth_health_costs, year)
  
  # add to health dat
  health_dat <- bind_rows(health_dat, health_tab)
  
}
rm(health_tab)


# deflate
health_dat <- 
  health_dat %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(youth_health_costs = youth_health_costs / (cpi / 100)) %>% 
  select(-cpi)


# add to data
cohort_dat <- left_join(cohort_dat, health_dat, 
                        by = c("RINPERSOONS", "RINPERSOON", "year"))

rm(health_dat)

# convert NA to 0
cohort_dat <- cohort_dat %>%
  mutate(youth_health_costs = ifelse(is.na(youth_health_costs), 0, youth_health_costs))



#### YOUTH PROTECTION ####

# function to get latest jgdbeschermbus version of specified year
get_protection_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "VeiligheidRecht/JGDBESCHERMBUS"),
    pattern = paste0("JGDBESCHERM", year, "BUSV[0-9]+(?i)(.sav)"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


protection_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(),
                         year = integer())
for (year in seq(2015, as.integer(cfg$high_school_year_max))) {
  
  protection_dat <-
    read_sav(get_protection_filename(year),
             col_select = c("RINPERSOONS", "RINPERSOON")) %>%
    mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
           year = year) %>%
    # add to protection dat
    bind_rows(protection_dat, .)
}

# use protection data 2015 dat to 2014 year since we do not have protection data 2014
protection_dat <-
  read_sav(get_protection_filename(2015),
           col_select = c("RINPERSOONS", "RINPERSOON")) %>%
  mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
         year = 2014) %>%
  # add to protection dat
  bind_rows(protection_dat, .)


# create youth protection outcome
protection_dat <- protection_dat %>%
  mutate(youth_protection = 1) %>%
  unique()

# add to data
cohort_dat <- left_join(cohort_dat, protection_dat,
                        by = c("RINPERSOONS", "RINPERSOON", "year"))
rm(protection_dat)

# convert NA to 0
cohort_dat <- 
  cohort_dat %>%
  mutate(youth_protection = ifelse(is.na(youth_protection), 0, youth_protection))

## update address of kids under child protection ##
adres_tab <- read_sav(file.path(loc$data_folder, loc$gbaao_data)) %>%
  mutate(
    RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
    SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"),
    GBADATUMAANVANGADRESHOUDING = ymd(GBADATUMAANVANGADRESHOUDING),
    GBADATUMEINDEADRESHOUDING = ymd(GBADATUMEINDEADRESHOUDING))

# identify children under child protection, their birthdate and their mother.
protection_tab <- cohort_dat %>%
  filter(youth_protection == 1) %>%
  select(RINPERSOON, birthdate, RINPERSOONMa) %>%
  rename("RINPERSOON_kind" = "RINPERSOON")

# get home address of mother (at time of child's birth) of kids who are under child protection
birth_adres <- adres_tab %>% 
  filter(RINPERSOON %in% protection_tab$RINPERSOONMa) %>%
  left_join(protection_tab, by = c("RINPERSOON" = "RINPERSOONMa"), relationship = "many-to-many") %>%
  filter(birthdate %within% interval(GBADATUMAANVANGADRESHOUDING, GBADATUMEINDEADRESHOUDING)) %>%
  group_by(RINPERSOON_kind, RINPERSOON) %>%
  summarise(childhood_home = RINOBJECTNUMMER[1],
            type_childhood_home = SOORTOBJECTNUMMER[1]) %>%
  select(-RINPERSOON) %>%
  rename("RINPERSOON" = "RINPERSOON_kind")

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

# add the postal codes to the group of kids under youth protection 
birth_adres <- inner_join(birth_adres, vslpc_tab, 
                          by = c("type_childhood_home" = "SOORTOBJECTNUMMER", 
                                 "childhood_home" = "RINOBJECTNUMMER"))

# add region/neighbourhood codes to the group of kids under youth protection 
vslgwb_path <- file.path(loc$data_folder, loc$vslgwb_data) 
vslgwb_tab  <- read_sav(vslgwb_path) %>%
  mutate(SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"))

# select region/neighbourhood from the target date
vslgwb_tab <- 
  vslgwb_tab %>% 
  select("type_childhood_home" = "SOORTOBJECTNUMMER", 
         "childhood_home"      = "RINOBJECTNUMMER", 
         "municipality_code"       = paste0("gem", year(dmy(cfg$gwb_target_date))), 
         "neighborhood_code"           = paste0("wc", year(dmy(cfg$gwb_target_date))))

birth_adres <- inner_join(birth_adres, vslgwb_tab)

# add corop regions
corop_tab  <- read_excel(loc$corop_data) %>%
  select("municipality_code" = paste0("GM", year(dmy(cfg$corop_target_date))), 
         "corop_code" = paste0("COROP", year(dmy(cfg$corop_target_date)))) %>%
  unique()

birth_adres <- left_join(birth_adres, corop_tab, by = "municipality_code") %>%
  select(-c(type_childhood_home, childhood_home))

# rename variables 
birth_adres <- 
  birth_adres %>% 
  mutate(across(c("postcode3", "postcode4", "municipality_code", 
                  "neighborhood_code", "corop_code"), as.character)) %>%
  rename("postcode3_" = "postcode3",
         "postcode4_" = "postcode4", 
         "municipality_code_" = "municipality_code",
         "neighborhood_code_" = "neighborhood_code", 
         "corop_code_" = "corop_code")

# merge to cohort data and only change the address of kids under youth protection. 
# discard records of those without address information and transform variables to factor.
cohort_dat <- left_join(cohort_dat, birth_adres, by = "RINPERSOON") %>% 
  mutate(postcode3 = case_when(youth_protection == 1 ~ postcode3_, youth_protection == 0 ~ postcode3),
         postcode4 = case_when(youth_protection == 1 ~ postcode4_, youth_protection == 0 ~ postcode4),
         municipality_code = case_when(youth_protection == 1 ~ municipality_code_, youth_protection == 0 ~ municipality_code),
         neighborhood_code = case_when(youth_protection == 1 ~ neighborhood_code_, youth_protection == 0 ~ neighborhood_code),
         corop_code = case_when(youth_protection == 1 ~ corop_code_, youth_protection == 0 ~ corop_code)) %>%
  filter(!is.na(postcode3)) %>%
  select(-c(postcode3_, postcode4_, municipality_code_, neighborhood_code_, corop_code_)) %>%
  mutate(across(c("postcode3", "postcode4", "municipality_code", 
                  "neighborhood_code",  "corop_code"), as.factor))

rm(birth_adres, corop_tab, protection_tab, vslgwb_tab, vslpc_tab)


#### LIVING SPACE PER HOUSEHOLD MEMBER ####


# load home addresses
# adres_tab <- read_sav(file.path(loc$data_folder, loc$gbaao_data)) %>%
#   mutate(
#     RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
#     SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"),
#     GBADATUMAANVANGADRESHOUDING = ymd(GBADATUMAANVANGADRESHOUDING),
#     GBADATUMEINDEADRESHOUDING = ymd(GBADATUMEINDEADRESHOUDING))


adres_tab <- 
  adres_tab %>% 
  # select only children
  filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>%
  filter(!(GBADATUMEINDEADRESHOUDING < ymd(paste0(cfg$high_school_year_min, "0101"))),
         !(GBADATUMAANVANGADRESHOUDING > ymd(paste0(cfg$high_school_year_max, "0101"))))


# create 1 january variable 
cohort_dat <- cohort_dat %>%
  mutate(january = ymd(paste0(year, "-01-01")))


# add 1 januari data to adres tab to filter for home addresses at 1 jan
adres_tab <- 
  adres_tab %>%
  left_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, january)) %>%
  filter(january %within% interval(GBADATUMAANVANGADRESHOUDING, GBADATUMEINDEADRESHOUDING)) 


# add home addresses to data
cohort_dat <- left_join(cohort_dat, adres_tab %>% 
                          select(RINPERSOONS, RINPERSOON, 
                                 SOORTOBJECTNUMMER, RINOBJECTNUMMER),
                        by = c("RINPERSOONS", "RINPERSOON"))
rm(adres_tab)


# LIVING SPACE 

woon_dat <-
  read_sav(file.path(loc$data_folder, loc$woon_data),
           col_select = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER", "VBOOPPERVLAKTE", 
                          "AANVLEVCYCLWOONNIETWOON", "EINDLEVCYCLWOONNIETWOON")) %>%
  mutate(SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"), 
         AANVLEVCYCLWOONNIETWOON = ymd(AANVLEVCYCLWOONNIETWOON), 
         EINDLEVCYCLWOONNIETWOON = ifelse(EINDLEVCYCLWOONNIETWOON == "88888888",
                                          paste0(cfg$high_school_year_max, "1231"),
                                          EINDLEVCYCLWOONNIETWOON),
         EINDLEVCYCLWOONNIETWOON = ymd(EINDLEVCYCLWOONNIETWOON)
  ) %>%
  filter(!(EINDLEVCYCLWOONNIETWOON < ymd(paste0(cfg$high_school_year_min, "-01-01"))),
         !(AANVLEVCYCLWOONNIETWOON > ymd(paste0(cfg$high_school_year_max, "-01-01"))))


# add 1 januari data to woon tab to filter for home addresses at 1 jan
woon_dat <- 
  woon_dat %>%
  left_join(cohort_dat %>% select(SOORTOBJECTNUMMER, RINOBJECTNUMMER, january), 
            by = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER")) %>%
  filter(january %within% interval(AANVLEVCYCLWOONNIETWOON, EINDLEVCYCLWOONNIETWOON)) %>%
  unique()



# add living space to data
cohort_dat <- left_join(cohort_dat,
                        woon_dat %>% select(SOORTOBJECTNUMMER, RINOBJECTNUMMER, VBOOPPERVLAKTE, january),
                        by = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER", "january"))  %>%
  mutate(VBOOPPERVLAKTE = as.numeric(VBOOPPERVLAKTE))

rm(woon_dat)



# NUMBER OF HOUSEHOLD MEMBERS


# function to get latest eigendom version of specified year
get_eigendom_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "BouwenWonen/EIGENDOMTAB"),
    pattern = paste0("EIGENDOM", year, "TABV[0-9]+(?i)(.sav)"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


household_members <- tibble(SOORTOBJECTNUMMER = factor(), RINOBJECTNUMMER = character(), 
                            AantalBewoners = double(), year = integer())
for (year in seq(as.integer(cfg$high_school_year_min), as.integer(cfg$high_school_year_max))) {
  
  household_members <- read_sav(get_eigendom_filename(year), 
                                col_select = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER",
                                               "AantalBewoners")) %>%
    mutate(SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"),
           AantalBewoners = as.numeric(AantalBewoners),
           year = year) %>%
    # add to household member dat
    bind_rows(household_members, .)
}

cohort_dat <- cohort_dat %>%
  left_join(household_members, by = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER", "year")) %>%
  mutate(AantalBewoners = as.numeric(AantalBewoners)) 


# create living space per household member outcome
cohort_dat <- cohort_dat %>%
  mutate(AantalBewoners = ifelse(AantalBewoners == 0, NA, AantalBewoners),
         living_space_pp = VBOOPPERVLAKTE / AantalBewoners) %>%
  select(-c(january, SOORTOBJECTNUMMER, RINOBJECTNUMMER, 
            VBOOPPERVLAKTE, AantalBewoners))


rm(household_members)



#### PRIMARY SCHOOL CLASS COMPOSITION ####
# load class cohort data
class_cohort_dat <- read_rds(file.path(loc$scratch_folder, "class_cohort.rds"))

# combine the main sample and class sample
class_cohort_dat <- bind_rows(
  class_cohort_dat %>% select ("RINPERSOON", "RINPERSOONS", "GBAGEBOORTELANDMOEDER", "GBAGEBOORTELANDVADER", "income_parents_perc"), 
  cohort_dat %>% select ("RINPERSOON", "RINPERSOONS", "GBAGEBOORTELANDMOEDER", "GBAGEBOORTELANDVADER", "income_parents_perc"), 
)


# function to get latest inschrwpo version of specified year
get_inschrwpo_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "Onderwijs/INSCHRWPOTAB"),
    pattern = paste0("INSCHRWPOTAB", year, "V[0-9]+(?i)(.sav)"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

school_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), WPOLEERJAAR = character(), 
                     WPOBRIN_crypt = character(), WPOBRINVEST = character(), WPOTYPEPO = character())

for (year in seq(as.integer(cfg$primary_classroom_year_min), as.integer(cfg$primary_classroom_year_max))) {
  school_dat <- 
    # read file from disk
    read_sav(get_inschrwpo_filename(year), 
             col_select = c("RINPERSOONS", "RINPERSOON", "WPOLEERJAAR", 
                            "WPOBRIN_crypt", "WPOBRINVEST", "WPOTYPEPO")) %>% 
    mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
    # add year
    mutate(year = year) %>% 
    # add to income children
    bind_rows(school_dat, .)
}

# keep group 8 pupils
school_dat <- school_dat %>%
  filter(RINPERSOONS == "R") %>%
  mutate(WPOLEERJAAR = trimws(as.character(WPOLEERJAAR))) %>%
  filter(WPOLEERJAAR == "8") %>%
  select(-WPOLEERJAAR)


# link to classroom sample
school_dat <- school_dat %>%
  left_join(class_cohort_dat, 
            by = c("RINPERSOON", "RINPERSOONS"))%>%
  # drop all children who are not in the large classroom sample 
  filter(!is.na(income_parents_perc))

# primary school ID
school_dat <- school_dat %>%
  mutate(across(c("WPOBRIN_crypt", "WPOBRINVEST"), as.character)) %>%
  mutate(school_ID = paste0(WPOBRIN_crypt, WPOBRINVEST)) %>%
  select(-c(WPOBRIN_crypt, WPOBRINVEST))


# create parents rank income outcomes
school_dat <- school_dat %>%
  mutate(
    # create dummy for below 25th 
    income_below_25th = ifelse(income_parents_perc < 0.25, 1, 0),
    # create dummy for below 50th 
    income_below_50th = ifelse(income_parents_perc < 0.50, 1, 0),
    # create dummy for above 75th
    income_above_75th = ifelse(income_parents_perc > 0.75, 1, 0)
  )


# create outcome for children with both parents born in a foreign country
school_dat <- school_dat %>%
  mutate(
    GBAGEBOORTELANDMOEDER = as_factor(GBAGEBOORTELANDMOEDER),
    GBAGEBOORTELANDVADER = as_factor(GBAGEBOORTELANDVADER)
  ) %>%
  mutate(
    foreign_born_parents = 
      ifelse((GBAGEBOORTELANDMOEDER != "Nederland" & 
                GBAGEBOORTELANDVADER != "Nederland"),  1, 0))


# classroom outcomes: class_foreign_born_parents, class_parents_below_25, class_parents_below_50, class_parents_above_75

# only keep classes with more than one student per class
school_dat <- school_dat %>%
  group_by(school_ID, year) %>%
  mutate(n = n()) %>%
  filter(n > 1)


# hold out mean function
hold_out_means <- function(x) {
  hold <- ((sum(x, na.rm = TRUE) - x) / (length(x) - 1))
  return(hold)
}



# hold out means = mean of the class without the child him/herself
school_dat <- school_dat %>%
  group_by(school_ID, year) %>%
  mutate(
    #    primary_N_students_per_school = n(),
    primary_class_foreign_born_parents = hold_out_means(foreign_born_parents),
    primary_class_income_below_25th = hold_out_means(income_below_25th),
    primary_class_income_below_50th = hold_out_means(income_below_50th),
    primary_class_income_above_75th = hold_out_means(income_above_75th)
  ) 


# keep unique observations,for duplicates select the last time the child is in 8th grade
school_dat <- school_dat %>%
  arrange(desc(year)) %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  filter(row_number() == 1)

# add to outcomes to cohort
cohort_dat <- cohort_dat %>%
  left_join (school_dat %>% 
               select(RINPERSOONS, RINPERSOON, primary_class_foreign_born_parents, primary_class_income_below_25th, primary_class_income_below_50th, primary_class_income_above_75th),
             by = c("RINPERSOONS", "RINPERSOON"))

rm(school_dat)

#### SECONDARY SCHOOL CLASS COMPOSITION ####

# function to get latest ONDERWIJSINSCHRTAB version of specified year
get_school_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "Onderwijs/ONDERWIJSINSCHRTAB"),
    pattern = paste0("ONDERWIJSINSCHRTAB", year, "V[0-9]+(?i)(.sav)"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


school_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), OPLNR = character(), VOLEERJAAR = character(), 
                     BRIN_crypt = character(), VOBRINVEST = character())

for (year in seq(as.integer(cfg$secondary_classroom_year_min ),as.integer(cfg$secondary_classroom_year_max))) {
  school_dat <- 
    # read file from disk
    read_sav(get_school_filename(year), 
             col_select = c("RINPERSOONS", "RINPERSOON","OPLNR", "VOLEERJAAR", 
                            "BRIN_crypt", "VOBRINVEST")) %>% 
    mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
    # add year
    mutate(year = year) %>% 
    # add to income children
    bind_rows(school_dat, .)
}

# keep those in grade 4 of secondary school 
school_dat <- school_dat %>%
  filter(VOLEERJAAR == "4", 
         RINPERSOONS == "R",
         !is.na(RINPERSOON)) 

# # find the level of secondary school 
# TODO: not sure what is happening here because ONDERWIJSSOORTVO is empty? 
# school_level <- read_sav(loc$opleiding_data) %>% 
#   select(OPLNR, ONDERWIJSSOORTVO)
# 
# school_dat <- school_dat %>%
#   left_join(school_level, by = "OPLNR") %>%
#   rename(school_level = ONDERWIJSSOORTVO )


# link to classroom sample
school_dat <- school_dat %>%
  left_join(class_cohort_dat, 
            by = c("RINPERSOON", "RINPERSOONS"))%>%
  # drop all children who are not in the classroom sample 
  filter(!is.na(income_parents_perc))


# generate secondary school ID
school_dat <- school_dat %>%
  mutate(across(c("BRIN_crypt", "VOBRINVEST"), as.character)) %>%
  mutate(school_ID = paste0(BRIN_crypt, VOBRINVEST)) %>%
  select(-c(BRIN_crypt, VOBRINVEST))


# create parents rank income outcomes
school_dat <- school_dat %>%
  mutate(
    # create dummy for below 25th 
    income_below_25th = ifelse(income_parents_perc < 0.25, 1, 0),
    # create dummy for below 50th 
    income_below_50th = ifelse(income_parents_perc < 0.50, 1, 0),
    # create dummy for above 75th
    income_above_75th = ifelse(income_parents_perc > 0.75, 1, 0)
  )


# create outcome for children with both parents born in a foreign country
school_dat <- school_dat %>%
  mutate(
    GBAGEBOORTELANDMOEDER = as_factor(GBAGEBOORTELANDMOEDER),
    GBAGEBOORTELANDVADER = as_factor(GBAGEBOORTELANDVADER)
  ) %>%
  mutate(
    foreign_born_parents = 
      ifelse((GBAGEBOORTELANDMOEDER != "Nederland" & 
                GBAGEBOORTELANDVADER != "Nederland"),  1, 0))


# classroom outcomes: class_foreign_born_parents, class_parents_below_25, class_parents_below_50, class_parents_above_75

# only keep classes with more than one student per class
school_dat <- school_dat %>%
  group_by(school_ID, year) %>%
  mutate(n = n()) %>%
  filter(n > 1)



# hold out means = mean of the class without the child him/herself
school_dat <- school_dat %>%
  group_by(school_ID, year) %>%
  mutate(
    #    secondary_class_N_students_per_school = n(),
    secondary_class_foreign_born_parents = hold_out_means(foreign_born_parents),
    secondary_class_income_below_25th = hold_out_means(income_below_25th),
    secondary_class_income_below_50th = hold_out_means(income_below_50th),
    secondary_class_income_above_75th = hold_out_means(income_above_75th)
  ) 


# keep unique observations,for duplicates select the last time the child is in 8th grade
school_dat <- school_dat %>%
  arrange(desc(year)) %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  filter(row_number() == 1)


# add to outcomes to cohort
cohort_dat <- cohort_dat %>%
  left_join (school_dat %>% 
               select(RINPERSOONS, RINPERSOON, secondary_class_foreign_born_parents, secondary_class_income_below_25th, secondary_class_income_below_50th, secondary_class_income_above_75th),
             by = c("RINPERSOONS", "RINPERSOON"))

rm(school_dat, class_cohort_dat)


#### PREFIX ####

# add prefix to outcomes
outcomes <- c("vmbo_gl", "havo", "vwo", 
              "youth_health_costs", "youth_protection", "living_space_pp", 
              
              "primary_class_foreign_born_parents", "primary_class_income_below_25th", 
              "primary_class_income_below_50th", "primary_class_income_above_75th",
              
              "secondary_class_foreign_born_parents", "secondary_class_income_below_25th", 
              "secondary_class_income_below_50th", "secondary_class_income_above_75th")
suffix <- "c16_"


# rename outcomes
cohort_dat <- 
  cohort_dat %>%
  rename_with(~str_c(suffix, .), .cols = all_of(outcomes)) %>% 
  ungroup() %>%
  #remove parents birth country
  select(-c(GBAGEBOORTELANDMOEDER, GBAGEBOORTELANDVADER))

sample_size <- sample_size %>% 
  mutate(n_6_child_outcomes = nrow(cohort_dat))


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))


#write sample size reduction table to scratch
sample_size <- sample_size %>% mutate(cohort_name = cohort)
write_rds(sample_size, file.path(loc$scratch_folder, "03_sample_size.rds"))
