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
library(lubridate)

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
    income_1log = log1p(income),
    income_rank = rank(income, na.last = "keep", ties.method = "average"),
    income_perc = income_rank / max(income_rank)
  )

#### HIGHER EDUCATION ####
hopl_tab <- 
  read_sav(file.path(loc$data_folder, loc$hoogste_opl_data)) %>% 
  mutate(RINPERSOONS = as_factor(RINPERSOONS))

hopl_tab <-
  hopl_tab %>% 
  rename(
    edu_followed = OPLNIVSOI2016AGG4HGMETNIRWO, 
    edu_attained = OPLNIVSOI2016AGG4HBMETNIRWO
  ) %>% 
  mutate(across(c(edu_followed, edu_attained), na_if, 9999)) %>% 
  mutate(
    hbo_attained = as.integer(edu_attained > 3000),
    wo_attained  = as.integer(edu_attained %in% c(3113, 3212, 3213))
    # hbo_followed     = as.integer(edu_followed > 3000), # codes above 3000 indicate hbo
    # wo_followed      = as.integer(edu_followed %in% c(3113, 3212, 3213)),
    # edu_lvl_followed = factor(floor(edu_followed/1000), levels = 1:3, labels = c("lower", "middle", "higher")),
    # edu_lvl_attained = factor(floor(edu_attained/1000), levels = 1:3, labels = c("lower", "middle", "higher"))
  ) %>% 
  select(-edu_attained, -edu_followed)

cohort_dat <- left_join(cohort_dat, hopl_tab)

#### HOURLY INCOME ####
# these come from the spolis tab
get_spolis_filename <- function(year) {
  # function to get latest ipi version of specified year
  # get all ipi files with the specified year
  fl <- list.files(
    path = file.path(loc$data_folder, loc$spolis_data),
    pattern = paste0("SPOLISBUS", year, "TABV[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

# create dataframe with all spolis entries for selected years
spolis_tab <- tibble(
  RINPERSOON    = integer(), 
  hourly_income = double(), 
  work_hours    = double(), 
  contract_type = factor()
)

for (year in seq(as.integer(cfg$child_income_year_min), as.integer(cfg$child_income_year_max))) {
  spolis_tab <- 
    # read file from disk
    read_sav(get_spolis_filename(year), col_select = c("RINPERSOON", "SBASISLOON", "SBASISUREN", "SCONTRACTSOORT")) %>% 
    rename(
      hourly_income = SBASISLOON,
      work_hours    = SBASISUREN,
      contract_type = SCONTRACTSOORT
    ) %>% 
    # select only incomes of children
    filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>% 
    # add year
    mutate(
      contract_type = as_factor(contract_type),
      year = year
    ) %>% 
    # add to income children
    bind_rows(spolis_tab, .)
}

# remove negative and NA incomes
spolis_tab <-
  spolis_tab %>% 
  mutate(hourly_income = ifelse(hourly_income == 9999999999 | hourly_income < 0, NA, hourly_income)) 

# deflate
spolis_tab <- 
  spolis_tab %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(hourly_income = hourly_income / (cpi / 100)) %>% 
  select(-cpi)

# compute aggregates
longest_contract_tab <- 
  spolis_tab %>% 
  group_by(RINPERSOON, contract_type) %>% 
  summarise(total_hours = sum(work_hours, na.rm = TRUE)) %>% 
  arrange(-total_hours, .by_group = TRUE) %>% 
  summarise(longest_contract_type = contract_type[1])

# get number of weeks in the used years
total_weeks <- interval(
  ymd(paste0(cfg$child_income_year_min, "-01-01")), 
  ymd(paste0(cfg$child_income_year_max, "-12-31"))
) / weeks()

income_hours_tab <- 
  spolis_tab %>% 
  group_by(RINPERSOON) %>% 
  summarize(
    spolis_n       = n(),
    hourly_income  = weighted.mean(x = hourly_income, w = work_hours, na.rm = TRUE),
    hours_per_week = sum(work_hours) / total_weeks
  )

# combine variables
spolis_tab <- left_join(income_hours_tab, longest_contract_tab)

# compute flex contract dummy
spolis_tab %>% mutate(flex_contract = ifelse(longest_contract_type == "Bepaalde tijd", 1, 0))

# add to cohort data
cohort_dat <- left_join(cohort_dat, spolis_tab)

# post-process: compute additional values
cohort_dat <- 
  cohort_dat %>% 
  mutate(
    has_worked         = ifelse(hours_per_week > 0, 1, 0),
    hours_per_week_NA0 = replace_na(hours_per_week, 0),
    hourly_income_1log = log1p(hours_per_week),
    hourly_income_rank = rank(hourly_income, na.last = "keep", ties.method = "average"),
    hourly_income_perc = hourly_income_rank / max(hourly_income_rank)
  )

#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))
