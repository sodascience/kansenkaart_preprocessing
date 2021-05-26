# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding child total health costs outcome to the cohort.
#   - Adding youth protection outcome to the cohort.
#
# (c) ODISSEI Social Data Science team 2021

#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(lubridate)
library(stringr)

#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds("scratch/02_predictors.rds")

# load the configuration
cfg <- config::get("data_preparation")
loc <- config::get("file_locations")



####  CHILD TOTAL HEALTH COSTS ####

cpi_tab <- 
  read_delim(loc$cpi_index_data, ";", skip = 5, 
             col_names = c("year", "cpi", "cpi_derived", "cpi_change", "cpi_change_derived"), 
             col_types = "ccccc") %>% 
  mutate(across(starts_with("cpi"), parse_number, locale = locale(decimal_mark = ","))) %>% 
  mutate(year = as.numeric(year))

# set cpi_base_year = 100
cpi_tab <- 
  cpi_tab %>% 
  mutate(
    cpi = cpi / cpi_tab %>% filter(year == cfg$cpi_base_year_child_health_costs) %>% pull(cpi) * 100,
    cpi_derived = cpi_derived / cpi_tab %>% filter(year == cfg$cpi_base_year_child_health_costs) %>% pull(cpi_derived) * 100
  )

# get all zvwzorgkosten files with the specified year
get_health_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "GezondheidWelzijn/ZVWZORGKOSTENTAB"),
    pattern = paste0("ZVWZORGKOSTEN", year, "TABV[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

# create function for cleaning
load_health_data <- function(file) {

  health_tab <- read_sav(file,
                         col_select = c("RINPERSOONS", "RINPERSOON", "ZVWKFARMACIE", "ZVWKGENBASGGZ", 
                   "ZVWKSPECGGZ", "ZVWKZIEKENHUIS", "ZVWKZIEKENVERVOER", "ZVWKEERSTELIJNSPSYCHO",
                   "ZVWKGERIATRISCH", "ZVWKOPHOOGFACTOR", "ZVWKGEBOORTEZORG", "ZVWKGGZ",
                   "ZVWKWYKVERPLEGING", "ZVWKHUISARTS", "ZVWKPARAMEDISCH", "ZVWKBUITENLAND",
                   "ZVWKHULPMIDDEL", "ZVWKOVERIG", "ZVWKMONDZORG")) %>% 
    as_factor(only_labelled = TRUE, levels = "values") 
    
  health_tab <- 
    health_tab %>%
    mutate(across(starts_with("ZVWK"), function(x) ifelse(x < 0, 0, x)))  # replace negative values with 0
    ) %>%
    mutate(
      # sum of all healthcare costs
      child_total_health_costs = rowSums(across(starts_with("ZVWK")))) 
    ) %>%
    select(RINPERSOONS, RINPERSOON, child_total_health_costs)
  
  # deflate
  if (str_extract(file, "\\d{4}") != cfg$cpi_base_year_child_health_costs){
    
    health_tab <- health_tab %>%
      mutate(year = as.numeric(str_extract(file, "\\d{4}"))) %>%
      left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
      mutate(child_total_health_costs = child_total_health_costs / (cpi / 100)) %>% 
      select(-cpi)
  } else{
    health_tab <- health_tab %>%
      mutate(year = as.numeric(str_extract(file, "\\d{4}"))) 
  }
  
  return(health_tab)
}
  
health_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = integer(), 
                     child_total_health_costs = double(), year = integer())
for (year in seq(as.integer(cfg$health_costs_year_min), as.integer(cfg$health_costs_year_max))) {
  health_dat <- load_health_data(get_health_filename(year)) %>%
    # add to health costs
    bind_rows(health_dat, .)
}

# compute mean child total health costs
health_dat <- health_dat %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  summarise(child_total_health_costs = mean(child_total_health_costs))
 
cohort_dat <- left_join(cohort_dat, health_dat)



####  YOUTH PROTECTION ####

# get all youth protection files with the specified year
get_youth_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "VeiligheidRecht/JGDBESCHERMBUS"),
    pattern = paste0("JGDBESCHERM", year, "BUSV[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

# rbind all youth protection files
youth_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = integer())
for (year in seq(as.integer(cfg$youth_protection_year_min), as.integer(cfg$youth_protection_year_max))) {
  youth_dat <- read_sav(get_youth_filename(year), 
                        col_select = c("RINPERSOONS", "RINPERSOON")) %>%
    as_factor(only_labelled = TRUE, levels = "value") %>%
    # add to health costs
    bind_rows(youth_dat, .)
}

# create youth protection dummy 
youth_dat <- youth_dat %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  summarize(youth_protection = 1)

# join to cohort dat and replace NA to 0
cohort_dat <- left_join(cohort_dat, youth_dat) %>%
  mutate(youth_protection = ifelse(is.na(youth_protection), 0, youth_protection))


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))
