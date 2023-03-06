# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding perinatal outcomes to the cohort.
#   - Writing `scratch/03_outcomes.rds`.
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


# import percentile weight boys & girls
boys_weight_tab <- read_excel(loc$birthweight_data, sheet = loc$boys_sheet, skip = 2) %>%
  rename(
    gestational_age = "Totaal aantal dagen",
    p10_boys = "p10...6"
  ) %>%
  select(c(gestational_age, p10_boys))

girls_weight_tab <- read_excel(loc$birthweight_data, sheet = loc$girls_sheet, skip = 2) %>%
  rename(
    gestational_age = "Totaal aantal dagen",
    p10_girls = "p10...6"
  ) %>%
  select(c(gestational_age, p10_girls))



#### PERINED ####

# function to get latest perined version of specified year
get_prnl_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "GezondheidWelzijn/PRNL", year), 
    pattern = paste0(year, "V[0-9]+(?i)(.sav)"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


# create perinatal data
perined_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                      rinpersoons_moeder = factor(), rinpersoon_moeder = character(), 
                      amddd = double(), datumkind = double(), geslachtkind = factor(), 
                      gewichtkind_ruw = double(), meerling = factor(), 
                      sterfte = factor(), year = integer())
for (year in seq(format(dmy(cfg$child_birth_date_min), "%Y"), 
                 format(dmy(cfg$child_birth_date_max), "%Y"))){
  
  perined_dat <- read_sav(get_prnl_filename(year), col_select = 
                            c("rinpersoons_kind_uitgebreid", "rinpersoon_kind", 
                              "rinpersoons_moeder", "rinpersoon_moeder",
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
                          year = year) %>%
  select(c("RINPERSOONS", "RINPERSOON", "rinpersoons_moeder", "rinpersoon_moeder",
           "amddd", "datumkind", "geslachtkind",
           "gewichtkind_ruw", "meerling", "sterfte", "year")) %>%
  bind_rows(perined_dat, .)

}

# remove gestational age below given days
perined_dat <- perined_dat %>%
  unique() %>%
  filter(!(amddd < cfg$cut_off_days_min),
         !(amddd > cfg$cut_off_days_max))


# merge percentile to perined data
perined_dat <- left_join(perined_dat, boys_weight_tab, by = c("amddd" = "gestational_age"))
perined_dat <- left_join(perined_dat, girls_weight_tab, by = c("amddd" = "gestational_age"))

rm(boys_weight_tab, girls_weight_tab)

# post-processing
perined_dat <- perined_dat %>% 
  mutate(datumkind = ymd(datumkind))


#### DO AND DOODOORZTAB ####

## CLEAN DO ##

# function to get latest do version of specified year 
get_do_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "GezondheidWelzijn/DO"), 
    pattern = paste0("DO", year, "V[0-9]+(?i)(.sav)"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

# function to get latest do version of specified year 
get_do_map_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "GezondheidWelzijn/DO", year), 
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
    path = file.path(loc$data_folder, "GezondheidWelzijn/DOODOORZTAB", year),
    pattern = paste0("DOODOORZ", year, "TABV[0-9]+(?i)(.sav)"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

# function to get latest gbaoverlijdenstab version of specified year 
get_gba_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "Bevolking/GBAOVERLIJDENTAB", year),
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

perined_dat <- left_join(perined_dat, death_dat) 

rm(death_dat)

# filter
perined_dat <- 
  perined_dat %>%
  mutate(
    diff_days = as.numeric(difftime(date_of_death, datumkind, units = "days"))
    ) %>%
  # remove all birth below certain days
  filter(diff_days >= cfg$cut_off_mortality_day | is.na(date_of_death))


#### OUTCOMES ####

# small for gestational age & preterm birth cohort
perined_dat <- perined_dat %>%
  mutate(
    # preterm birth for infants with < 259 gestational age
    preterm_birth = ifelse(amddd < 259, 1, 0),
    
    # create small for gestational age outcome
    sga = ifelse((geslachtkind == "jongen" & gewichtkind_ruw < p10_boys) |
                               (geslachtkind == "meisje" & gewichtkind_ruw < p10_girls),
                             1, 0)
  )


cohort_dat <- inner_join(cohort_dat, 
                         perined_dat %>%
                           select(RINPERSOONS, RINPERSOON, preterm_birth, sga))


# free up memory
rm(perined_dat)


#### PREFIX ####

# add prefix to outcomes
outcomes <- c("sga", "preterm_birth")
suffix <- "c00_"


# rename outcomes
cohort_dat <- 
  cohort_dat %>%
  rename_with(~str_c(suffix, .), .cols = all_of(outcomes)) %>% 
  ungroup() 



#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))

