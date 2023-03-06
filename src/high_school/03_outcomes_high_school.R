# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding high school education outcomes to the cohort.
#
# (c) ODISSEI Social Data Science team 2022



#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(readxl)

#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "02_predictors.rds"))


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


#### LIVING SPACE PER HOUSEHOLD MEMBER ####


# load home addresses
adres_tab <- read_sav(file.path(loc$data_folder, loc$gbaao_data)) %>%
  mutate(
    RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
    SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"),
    GBADATUMAANVANGADRESHOUDING = ymd(GBADATUMAANVANGADRESHOUDING),
    GBADATUMEINDEADRESHOUDING = ymd(GBADATUMEINDEADRESHOUDING))


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


#### PREFIX ####

# add prefix to outcomes
outcomes <- c("vmbo_gl", "havo", "vwo", 
              "youth_health_costs", "youth_protection", "living_space_pp")
suffix <- "c16_"


# rename outcomes
cohort_dat <- 
  cohort_dat %>%
  rename_with(~str_c(suffix, .), .cols = all_of(outcomes)) %>% 
  ungroup() 


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))

