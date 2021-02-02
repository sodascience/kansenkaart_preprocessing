# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding (socio)economic outcomes to the cohort.
#   - Adding education outcomes to the cohort.
#   - Adding health outcomes to the cohort.
#   - Writing `scratch/03_outcomes.rds`.
#
# (c) ODISSEI Social Data Science team 2021

#### PACKAGES ####
library(tidyverse)
library(haven)

#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds("scratch/02_predictors.rds")

# load the configuration
cfg <- config::get("data_preparation")
loc <- config::get("file_locations")


#### CHILD INCOME ####
# create a table with incomes at the cpi_base_year€ level
# first, load consumer price index data
# source: https://opendata.cbs.nl/statline/#/CBS/nl/dataset/83131NED/table?ts=1610019128426
cpi_tab <- 
  read_delim("resources/Consumentenprijzen__prijsindex_2015_100_07012021_123946.csv", ";", skip = 5, 
             col_names = c("year", "cpi", "cpi_derived", "cpi_change", "cpi_change_derived"), 
             col_types = "ccccc") %>% 
  mutate(across(starts_with("cpi"), parse_number, locale = locale(decimal_mark = ","))) %>% 
  mutate(year = as.numeric(year))

# set cpi_base_year = 100
cpi_tab <- 
  cpi_tab %>% 
  mutate(
    cpi = cpi / cpi_tab %>% filter(year == cfg$cpi_base_year) %>% pull(cpi) * 100,
    cpi_derived = cpi_derived / cpi_tab %>% filter(year == cfg$cpi_base_year) %>% pull(cpi_derived) * 100
  )

# get inpa data from each requested year into single data frame
get_inpa_filename <- function(year) {
  # function to get latest inpa version of specified year
  # get all inpa files with the specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INPATAB/"),
    pattern = paste0("INPA", year, "TABV[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

# TODO: child_income_year_min and max in config
inpa_children <- tibble(RINPERSOON = integer(), INPPERSBRUT = double(), year = integer())
for (year in seq(as.integer(cfg$child_income_year_min), as.integer(cfg$child_income_year_max))) {
  inpa_children <- 
    # read file from disk
    read_sav(get_inpa_filename(year), col_select = c("RINPERSOON", "INPPERSBRUT")) %>% 
    # select only incomes of parents
    filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>% 
    # add year
    mutate(year = year) %>% 
    # add to inpa_parents
    bind_rows(inpa_children, .)
}

# remove negative and NA incomes
inpa_children <-
  inpa_children %>% 
  mutate(INPPERSBRUT = ifelse(INPPERSBRUT == 9999999999 | INPPERSBRUT < 0, NA, INPPERSBRUT)) 

# deflate
inpa_children <- 
  inpa_children %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(INPPERSBRUT = INPPERSBRUT / (cpi / 100)) %>% 
  select(-cpi)

# compute mean
inpa_children <- 
  inpa_children %>% 
  group_by(RINPERSOON) %>% 
  summarize(income = mean(INPPERSBRUT, na.rm = TRUE))

# add to data
cohort_dat <- left_join(cohort_dat, inpa_children, by = "RINPERSOON")

# free up memory
rm(inpa_children)

# compute income transformations
cohort_dat <- 
  cohort_dat %>% 
  mutate(
    income_1log = log(income + 1),
    income_rank = rank(income, na.last = "keep", ties.method = "average"),
    income_perc = income_rank / max(income_rank)
  )


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))
