# Kansenkaart data preparation pipeline
#
# 2. Predictor creation.
#    - Adding parent income and income percentile to the cohort.
#    - Adding migration background information to the cohort.
#    - Writing `scratch/02_predictor.rds`
#
# (c) ODISSEI Social Data Science team 2024



#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(readxl)


#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "01_cohort.rds")) %>%
  mutate(birth_year = year(birthdate)) %>%
  filter(birth_year == 1999)



# create a table with incomes at the cpi_base_year level
# first, load consumer price index data (2015 = 100)
# source: CBS statline
cpi_tab <- read_excel(loc$cpi_index_data) %>% 
  mutate(year = as.numeric(year))

# set cpi_base_year = 100
cpi_tab <- cpi_tab %>%
  mutate(
    cpi = cpi / cpi_tab %>% filter(year == cfg$cpi_base_year) %>% pull(cpi) * 100)


#### LIVE CONTINUOUSLY IN NL FOR PARENTS ####


# We only include parents who live continuously in the Netherlands 
adres_path <- file.path(loc$data_folder, loc$gbaao_data)
adres_tab  <- read_sav(adres_path) %>%
  # select only parents
  filter((RINPERSOON %in% cohort_dat$RINPERSOONpa) | 
           (RINPERSOON %in% cohort_dat$RINPERSOONMa)) %>% 
  mutate(
    RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
    GBADATUMAANVANGADRESHOUDING = ymd(GBADATUMAANVANGADRESHOUDING),
    GBADATUMEINDEADRESHOUDING = ymd(GBADATUMEINDEADRESHOUDING)
  ) %>%
  select(-c(SOORTOBJECTNUMMER, RINOBJECTNUMMER))


# residency requirement for parents between parent_income_year_min and parent_income_year_max
residency_tab <- 
  cohort_dat %>%
  select(RINPERSOONSMa, RINPERSOONMa, RINPERSOONSpa, RINPERSOONpa) %>%
  mutate(start_date = ymd(paste0(cfg$parent_income_year_min, "-01-01")), 
         end_date = ymd(paste0(cfg$parent_income_year_max, "-12-31")),
         cutoff_days = as.numeric(difftime(end_date, start_date, units = "days")) - 
           cfg$child_live_slack_days) %>%
  unique()


# throw out anything with an end date before start_date, and anything with a start date after end_date
# then also set the start date of everything to start_date, and the end date of everything to end_date
# then compute the time span of each record for parents
# mothers
adres_ma_tab <- 
  adres_tab %>% 
  inner_join(residency_tab %>% select(RINPERSOONSMa, RINPERSOONMa, start_date, 
                                      end_date, cutoff_days) %>% unique(), 
             by = c("RINPERSOON" = "RINPERSOONMa", "RINPERSOONS" = "RINPERSOONSMa")) %>%
  filter(!(GBADATUMEINDEADRESHOUDING < start_date),
         !(GBADATUMAANVANGADRESHOUDING > end_date)) %>%
  mutate(
    recordstart = as_date(ifelse(GBADATUMAANVANGADRESHOUDING < start_date, start_date, GBADATUMAANVANGADRESHOUDING)),
    recordend   = as_date(ifelse(GBADATUMEINDEADRESHOUDING > end_date, end_date, GBADATUMEINDEADRESHOUDING)) ,
    timespan    = difftime(recordend, recordstart, units = "days")
  ) %>%
  select(-c(GBADATUMAANVANGADRESHOUDING, GBADATUMEINDEADRESHOUDING, 
            start_date, end_date))
# fathers
adres_pa_tab <- 
  adres_tab %>% 
  left_join(residency_tab %>% select(RINPERSOONSpa, RINPERSOONpa, start_date, 
                                     end_date, cutoff_days) %>% unique(), 
            by = c("RINPERSOON" = "RINPERSOONpa", "RINPERSOONS" = "RINPERSOONSpa")) %>%
  filter(!(GBADATUMEINDEADRESHOUDING < start_date),
         !(GBADATUMAANVANGADRESHOUDING > end_date)) %>%
  mutate(
    recordstart = as_date(ifelse(GBADATUMAANVANGADRESHOUDING < start_date, start_date, GBADATUMAANVANGADRESHOUDING)),
    recordend   = as_date(ifelse(GBADATUMEINDEADRESHOUDING > end_date, end_date, GBADATUMEINDEADRESHOUDING)) ,
    timespan    = difftime(recordend, recordstart, units = "days")
  ) %>%
  select(-c(GBADATUMAANVANGADRESHOUDING, GBADATUMEINDEADRESHOUDING, 
            start_date, end_date))

adres_tab <- rbind(adres_ma_tab, adres_pa_tab)


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


# add to data
# mothers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = days_tab, 
  by = c("RINPERSOONMa" = "RINPERSOON", "RINPERSOONSMa" = "RINPERSOONS")
) %>%
  rename(continuous_living_ma = continuous_living)
# fathers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = days_tab, 
  by = c("RINPERSOONpa" = "RINPERSOON", "RINPERSOONSpa" = "RINPERSOONS")
) %>%
  rename(continuous_living_pa = continuous_living)


# remove parents if they both do not live continuously in NL
cohort_dat <-
  cohort_dat %>%
  filter(continuous_living_pa == TRUE | continuous_living_ma == TRUE) %>%
  select(-c(continuous_living_pa, continuous_living_ma))


# free up memory
rm(adres_tab, adres_ma_tab, adres_pa_tab, days_tab, residency_tab)



#### PARENT INCOME ####

# get income data from each requested year into single data frame
get_ipi_filename <- function(year) {
  # function to get latest ipi version of specified year
  # get all ipi files with the specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INTEGRAAL PERSOONLIJK INKOMEN", year),
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
    path = file.path(loc$data_folder, "InkomenBestedingen/INPATAB"),
    pattern = paste0("INPA", year, "TABV[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

parents <- c(cohort_dat$RINPERSOONMa, cohort_dat$RINPERSOONSMa, 
             cohort_dat$RINPERSOONpa, cohort_dat$RINPERSOONSpa)

income_parents <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                         income = double(), year = integer())
for (year in seq(as.integer(2003), as.integer(2020))) {
  if (year < 2011) {
    # use IPI tab
    income_parents <- 
      # read file from disk
      read_sav(get_ipi_filename(year), col_select = c("RINPERSOONS", "RINPERSOON", "PERSBRUT")) %>% 
      rename(income = PERSBRUT) %>% 
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
      # select only incomes of parents
      filter(RINPERSOON %in% parents) %>% 
      # add year
      mutate(year = year) %>% 
      # add to income parents
      bind_rows(income_parents, .)
  } else {
    # use INPA tab
    income_parents <- 
      # read file from disk
      read_sav(get_inpa_filename(year), col_select = c("RINPERSOONS", "RINPERSOON", "INPPERSBRUT")) %>% 
      rename(income = INPPERSBRUT) %>% 
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
      # select only incomes of parents
      filter(RINPERSOON %in% parents) %>% 
      # add year
      mutate(year = year) %>% 
      # add to income parents
      bind_rows(income_parents, .)
  }
}

# remove NA incomes
income_parents <-
  income_parents %>% 
  mutate(
    income = ifelse(income == 999999999 | income == 9999999999, NA, income))

# deflate
income_parents <- 
  income_parents %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(income = income / (cpi / 100)) %>% 
  select(-cpi)


# add to data
# mothers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = income_parents %>% rename(income_ma = income, year_ma = year), 
  by = c("RINPERSOONMa" = "RINPERSOON", "RINPERSOONSMa" = "RINPERSOONS")) 
# fathers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = income_parents %>% rename(income_pa = income, year_pa = year), 
  by = c("RINPERSOONpa" = "RINPERSOON", "RINPERSOONSpa" = "RINPERSOONS", 
         "year_ma" = "year_pa")) %>%
  rename(income_year = year_ma)


# parents
cohort_dat <- 
  cohort_dat %>% 
  rowwise() %>%
  mutate(income_parents =  sum(income_ma, income_pa, na.rm = TRUE),
         income_parents = ifelse((is.na(income_ma) & is.na(income_pa)), 
                                 NA, income_parents))


# if income_parents is negative then income_parents becomes NA
cohort_dat <- 
  cohort_dat %>%
  mutate(income_parents = ifelse(income_parents < 0, NA, income_parents)) %>%
  # remove income if income_parents is NA (parents income is NA is all years)
  filter(!is.na(income_parents))


# compute income transformations
cohort_dat <- 
  cohort_dat %>%
  group_by(income_year) %>%
  mutate(
    income_parents_rank = rank(income_parents, ties.method = "average"),
    income_parents_perc = income_parents_rank / max(income_parents_rank)
  ) %>%
  select(-c(income_parents_rank, income_ma, income_pa)) %>%
  ungroup()


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "02_predictors_lifecycle.rds"))


