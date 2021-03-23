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
  read_delim(loc$cpi_index_data, ";", skip = 5, 
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

# get income data from each requested year into single data frame
get_ipi_filename <- function(year) {
  # function to get latest ipi version of specified year
  # get all ipi files with the specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INTEGRAAL PERSOONLIJK INKOMEN/"),
    pattern = paste0("PERSOONINK", year, "TABV[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

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


income_children <- tibble(RINPERSOON = integer(), income = double(), year = integer())
for (year in seq(as.integer(cfg$child_income_year_min), as.integer(cfg$child_income_year_max))) {
  if (year < 2011) {
    # use IPI tab
    income_children <- 
      # read file from disk
      read_sav(get_ipi_filename(year), col_select = c("RINPERSOON", "PERSBRUT")) %>% 
      rename(income = PERSBRUT) %>% 
      # select only incomes of children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>% 
      # add year
      mutate(year = year) %>% 
      # add to income children
      bind_rows(income_children, .)
  } else {
    # use INPA tab
    income_children <- 
      # read file from disk
      read_sav(get_inpa_filename(year), col_select = c("RINPERSOON", "INPPERSBRUT")) %>% 
      rename(income = INPPERSBRUT) %>% 
      # select only incomes of children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>% 
      # add year
      mutate(year = year) %>% 
      # add to income children
      bind_rows(income_children, .)
  }
}

# remove negative and NA incomes
income_children <-
  income_children %>% 
  mutate(income = ifelse(income == 9999999999 | income < 0, NA, income)) 

# deflate
income_children <- 
  income_children %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(income = income / (cpi / 100)) %>% 
  select(-cpi)

# compute mean
income_children <- 
  income_children %>% 
  group_by(RINPERSOON) %>% 
  summarize(income_n = sum(!is.na(income)),
            income = mean(income, na.rm = TRUE))

# table of the number of years the mean income is based on
print(table(`income years` = income_children$income_n))

# add to data
cohort_dat <- left_join(cohort_dat, income_children, by = "RINPERSOON")

# free up memory
rm(income_children)

# compute income transformations
cohort_dat <- 
  cohort_dat %>% 
  mutate(
    income_1log = log(income + 1),
    income_rank = rank(income, na.last = "keep", ties.method = "average"),
    income_perc = income_rank / max(income_rank)
  )


# recode migration background variable
western_tab <- read_sav("resources/LANDAKTUEELREF10.sav")

cohort_dat <- cohort_dat %>% 
  left_join(western_tab, by = c("GBAHERKOMSTGROEPERING" = "LANDEN")) %>%
  rename(migration = LANDTYPE) %>%
  mutate(migration         = ifelse(GBAHERKOMSTGROEPERING == "Nederland", "Nederland", migration),
         migration         = ifelse(GBAHERKOMSTGROEPERING == "Turkije", "Turkije", migration),
         migration         = ifelse(GBAHERKOMSTGROEPERING == "Marokko", "Marokko", migration),
         migration         = ifelse(GBAHERKOMSTGROEPERING == "Suriname", "Suriname", migration),
         migration         = ifelse(GBAHERKOMSTGROEPERING == "Nederlandse Antillen (oud)", "Nederlandse Antillen (oud)", migration),
         total_non_western = ifelse(migration == "NietWesters" |  migration == "Turkije" |
                                      migration == "Marokko" | migration == "Suriname" | 
                                      migration == "Nederlandse Antillen (oud)", 1, 0)) # create total non western dummy
         
  


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))
