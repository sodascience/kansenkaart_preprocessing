# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding perinatal outcomes to the cohort.
#   - Writing `scratch/03_outcomes.rds`.
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



#### MORTALITY OUTCOMES ####


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
death_dat <- 
  death_dat %>%
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
for (year in seq(2013, format(dmy(cfg$child_birth_date_max) + 1, "%Y"))) {
  
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
    ) %>% distinct() %>%
    # add to death data
    bind_rows(gba_dat, .)
}

# post-processing
dood_dat <- inner_join(dood_dat, gba_dat) %>%
  mutate(date_of_death = ymd(date_of_death)) %>%
  # select(-year) %>%
  distinct()


# create one death data
death_dat <- rbind(death_dat, dood_dat) %>%
  select(-year) %>%
  unique() %>%
  distinct(RINPERSOONS, RINPERSOON, .keep_all = TRUE)
rm(dood_dat, gba_dat)


cohort_dat <- left_join(cohort_dat, death_dat)

# free up memory
rm(death_dat)

# create difference between birth and death date
cohort_dat <- 
  cohort_dat %>%
  mutate(
    birthdate = ymd(birthdate),
    diff_days = as.numeric(difftime(date_of_death, birthdate, units = "days"))
  ) 


cohort_dat <- 
  cohort_dat %>% 
  mutate(
    # perinatal mortality: 24 weeks to <= 7 days
    perinatal_mortality = ifelse(
      diff_days <= 7 | (sterfte %in% c('Ante partum', 'Durante partum', 
                                       'Postpartum 0-7 dagen')), 1, 0),
    # neonatal mortality: 24 weeks to <= 28 days
    neonatal_mortality = ifelse(
      diff_days <= 28 | (sterfte %in% c('Ante partum', 'Durante partum', 
                                       'Postpartum 0-7 dagen', 'Postpartum 8-28 dagen')), 1, 0),
    # infant mortality: 24 weeks to < 365 days
    infant_mortality = ifelse(
      diff_days < 365 | (sterfte %in% c('Ante partum', 'Durante partum', 
                                        'Postpartum 0-7 dagen', 'Postpartum 8-28 dagen')), 1, 0), 
    # Remove NAs
    perinatal_mortality = ifelse(is.na(perinatal_mortality), 0, perinatal_mortality),
    neonatal_mortality = ifelse(is.na(neonatal_mortality), 0, neonatal_mortality),
    infant_mortality = ifelse(is.na(infant_mortality), 0, infant_mortality)
    ) 



#### BIG2 OUTCOMES ####


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


# merge percentile to perined data
cohort_dat <- left_join(cohort_dat, boys_weight_tab, by = c("amddd" = "gestational_age"))
cohort_dat <- left_join(cohort_dat, girls_weight_tab, by = c("amddd" = "gestational_age"))


# small for gestational age & preterm birth cohort
cohort_dat <- 
  cohort_dat %>%
  mutate(
    # preterm birth for infants with < 259 gestational age
    preterm_birth = ifelse(amddd < 259, 1, 0),
    
    # create small for gestational age outcome
    sga = ifelse((sex == "Men" & geboortegew < p10_boys) |
                   (sex == "Women" & geboortegew < p10_girls), 1, 0)) %>%
  select(-c(p10_boys, p10_girls))


# free up memory
rm(girls_weight_tab, boys_weight_tab)


#### PREFIX ####

# add prefix to outcomes
outcomes <- c('perinatal_mortality', 'neonatal_mortality', 
              'infant_mortality', 'sga', 'preterm_birth')
suffix <- "c00_"


# rename outcomes
cohort_dat <- 
  cohort_dat %>%
  rename_with(~str_c(suffix, .), .cols = all_of(outcomes)) %>% 
  ungroup() 

# record sample size
sample_size <- sample_size %>% mutate(n_4_child_outcomes = nrow(cohort_dat))

#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))

#write sample size reduction table to scratch
sample_size <- sample_size %>% mutate(cohort_name = cohort)
write_rds(sample_size, file.path(loc$scratch_folder, "03_sample_size.rds"))

