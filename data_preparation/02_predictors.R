# Kansenkaart data preparation pipeline
#
# 2. Predictor creation.
#    - Adding parent income and income percentile to the cohort.
#    - Adding migration background information to the cohort.
#    - Adding gender information to the cohort.
#    - Writing `scratch/02_predictor.rds`
#
# (c) ODISSEI Social Data Science team 2021

#### PACKAGES ####
library(tidyverse)
library(haven)

#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds("scratch/01_cohort.rds")

# load the configuration
cfg <- config::get("data_preparation")
loc <- config::get("file_locations")
data <- config::get("02_predictors_data")


#### PARENT INCOME ####
# create a table with incomes at the cpi_base_year€ level
# first, load consumer price index data
# source: https://opendata.cbs.nl/statline/#/CBS/nl/dataset/83131NED/table?ts=1610019128426
cpi_tab <- 
  read_delim(data$cpi_index_data, ";", skip = 5, 
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
    path = file.path(loc$data_folder, "InkomenBestedingen/INTEGRAAL PERSOONLIJK INKOMEN/"),
    pattern = paste0("PERSOONINK", year, "TABV[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

parents <- c(cohort_dat$RINPERSOONMa, cohort_dat$RINPERSOONpa)
inpa_parents <- tibble(RINPERSOON = integer(), PERSBRUT = double(), year = integer())
for (year in seq(as.integer(cfg$parent_income_year_min), as.integer(cfg$parent_income_year_max))) {
  inpa_parents <- 
    # read file from disk
    read_sav(get_inpa_filename(year), col_select = c("RINPERSOON", "PERSBRUT")) %>% 
    # select only incomes of parents
    filter(RINPERSOON %in% parents) %>% 
    # add year
    mutate(year = year) %>% 
    # add to inpa_parents
    bind_rows(inpa_parents, .)
}

# remove negative and NA incomes
inpa_parents <-
  inpa_parents %>% 
  mutate(PERSBRUT = ifelse(PERSBRUT == 9999999999 | PERSBRUT < 0, NA, PERSBRUT)) 

# deflate
inpa_parents <- 
  inpa_parents %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(PERSBRUT = PERSBRUT / (cpi / 100)) %>% 
  select(-cpi)

# compute mean
inpa_parents <- 
  inpa_parents %>% 
  group_by(RINPERSOON) %>% 
  summarize(income   = mean(PERSBRUT, na.rm = TRUE),
            income_n = sum(!is.na(PERSBRUT)))

# table of the number of years the mean income is based on
print(table(`income years` = inpa_parents$income_n))

# add to data
# mothers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = inpa_parents %>% rename(income_ma = income, income_n_ma = income_n), 
  by = c("RINPERSOONMa" = "RINPERSOON")
)
# fathers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = inpa_parents %>% rename(income_pa = income, income_n_pa = income_n), 
  by = c("RINPERSOONpa" = "RINPERSOON")
)
# parents
cohort_dat <- cohort_dat %>% mutate(income_parents = income_ma + income_pa)

# free up memory
rm(inpa_parents)

# compute income transformations
cohort_dat <- 
  cohort_dat %>% 
  mutate(
    income_parents_1log = log(income_parents + 1),
    income_parents_rank = rank(income_parents, na.last = "keep", ties.method = "average"),
    income_parents_perc = income_parents_rank / max(income_parents_rank)
  )


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "02_predictors.rds"))
