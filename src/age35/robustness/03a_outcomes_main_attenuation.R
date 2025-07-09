# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding (socio)economic outcomes to the cohort.
#   - Adding education outcomes to the cohort.
#   - Adding health outcomes to the cohort.
#   - Adding housing outcomes to the cohort.
#   - Writing `scratch/03_outcomes.rds`.
#
# (c) ODISSEI Social Data Science team 2024



#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(readxl)


#### CONFIGURATION ####

# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "02_predictors_attenuation.rds")) %>%
# create variable that reflects the year the child turned a specific age 
  mutate(year = year(birthdate %m+% years(cfg$outcome_age))) %>%
  ungroup()



# create a table with incomes at the cpi_base_year level
# first, load consumer price index data (2015 = 100)
# source: CBS statline
cpi_tab <- read_excel(loc$cpi_index_data) %>% 
  mutate(year = as.numeric(year))

# set cpi_base_year = 100
cpi_tab <- cpi_tab %>%
  mutate(
    cpi = cpi / cpi_tab %>% filter(year == cfg$cpi_base_year) %>% pull(cpi) * 100)


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
  mutate(start_date = ymd(paste0(year(birthdate), "-01-01")) %m+% years(cfg$child_live_start), 
         end_date = ymd(paste0(year(birthdate), "-12-31")) %m+% years(cfg$child_live_end),
         cutoff_days = as.numeric(difftime(end_date, start_date, units = "days")) - 
           cfg$child_live_slack_days) %>%
  select(-birthdate)


# throw out anything with an end date before start_date, and anything with a start date after end_date
# then also set the start date of everything to start_date, and the end date of everything to end_date
# then compute the time span of each record
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




### CHILD HOUSEHOLD INCOME  ####


# get income data from each requested year into single data frame
get_ihi_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INTEGRAAL HUISHOUDENS INKOMEN/", year),
    pattern = paste0("HUISHBVRINK", year),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

get_inha_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INHATAB/"),
    # pattern = paste0("INHA", year, "TABV[0-9]+\\(?i).sav"),
    pattern = paste0("INHA", year),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

get_koppelpersoon_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/VEHTAB"),
    pattern = paste0("KOPPELPERSOONHUISHOUDEN", year),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

get_koppeltabel_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/VEHTAB"),
    pattern = paste0("KOPPELTABEL", year),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


income_household <- tibble(RINPERSOONS = factor(), RINPERSOON = character(),
                           household_income = double(), year = integer())
for (year in seq(as.integer(cfg$child_income_year_min), as.integer(cfg$child_income_year_max))) {
  
  if (year <= 2010) {
    
    # load koppel data person to household
    koppel_hh <- read_sav(get_koppeltabel_filename(year)) %>%
      # select only incomes of children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>%
      mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"),
             RINPERSOONS = as_factor(RINPERSOONS, levels = "value"))
    
  } else {
    
    # load koppel data person to household
    koppel_hh <- read_sav(get_koppelpersoon_filename(year)) %>%
      # select only incomes of children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>%
      mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"),
             RINPERSOONS = as_factor(RINPERSOONS, levels = "value"))
  }
  
  if (year < 2011) {
    # use tab
    income_children <-
      # read file from disk
      read_sav(get_ihi_filename(year),
               col_select = c("RINPERSOONSKERN", "RINPERSOONKERN", "BVRBRUTINKH")) %>%
      rename(household_income = BVRBRUTINKH,
             RINPERSOONSHKW = RINPERSOONSKERN,
             RINPERSOONHKW = RINPERSOONKERN) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
             year = year)
    
  } else {
    # use tab
    income_children <-
      # read file from disk
      read_sav(get_inha_filename(year),
               col_select = c("RINPERSOONSHKW", "RINPERSOONHKW", "INHBRUTINKH")) %>%
      rename(household_income     = INHBRUTINKH) %>%
      mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"),
             year = year)
    
  }
  
  income_children <- inner_join(income_children, koppel_hh,
                                by = c("RINPERSOONSHKW", "RINPERSOONHKW")) %>%
    select(-c(RINPERSOONSHKW, RINPERSOONHKW))
  
  income_household <- bind_rows(income_household, income_children) %>%
    # select only incomes of children
    filter(RINPERSOON %in% cohort_dat$RINPERSOON)
  
}
rm(income_children, koppel_hh)

# remove NA incomes
income_household <-
  income_household %>%
  mutate(household_income = ifelse(household_income == 999999999 | household_income == 9999999999, NA, household_income))

# deflate
income_household <-
  income_household %>%
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>%
  mutate(household_income = household_income / (cpi / 100)) %>%
  select(-cpi)


# add year variable at which the child is a specific age
# to compute the mean of child income in a period.
income_household <-
  income_household %>%
  rename(income_year = year) %>%
  arrange(RINPERSOON) %>%
  left_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, year),
            by = c("RINPERSOONS", "RINPERSOON"))


# compute the mean between the range year and the previous year
income_household <-
  income_household %>%
  mutate(year = ifelse(income_year == (year - 1), (year - 1), year),
         year = ifelse(income_year  == year, year, NA)) %>%
  filter(!is.na(year)) %>%
  select(-year)


# compute mean
income_household <-
  income_household %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  summarize(income_n = sum(!is.na(household_income)),
            household_income = mean(household_income, na.rm = TRUE))

# table of the number of years the mean income is based on
print(table(`income years` = income_household$income_n))

# add to data
cohort_dat <- left_join(cohort_dat, income_household %>% select(-income_n),
                        by = c("RINPERSOONS", "RINPERSOON"))

# free up memory
rm(income_household)


# if income is negative then household_income becomes NA
cohort_dat <-
  cohort_dat %>%
  mutate(household_income = ifelse(household_income < 0, NA, household_income)) %>%
  # remove income if income is NA 
  filter(!is.na(household_income))


# compute income transformations
cohort_dat <- 
  cohort_dat %>%
  group_by(birth_year) %>%
  mutate(
    household_income_rank = rank(household_income, ties.method = "average"),
    household_income_perc = household_income_rank / max(household_income_rank)
  ) %>%
  select(-c(household_income_rank)) %>%
  ungroup()



#### CHILD INCOME ####

# get income data from each requested year into single data frame
get_ipi_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INTEGRAAL PERSOONLIJK INKOMEN/", year),
    pattern = paste0("PERSOONINK", year), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

get_inpa_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INPATAB/"),
    pattern = paste0("INPA", year), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


income_children <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                          income = double(), year = integer())
for (year in seq(as.integer(cfg$child_income_year_min), as.integer(cfg$child_income_year_max))) {
  if (year < 2011) {
    # use IPI tab
    income_children <- 
      # read file from disk
      read_sav(get_ipi_filename(year), col_select = c("RINPERSOONS", "RINPERSOON", "PERSBRUT")) %>% 
      rename(income = PERSBRUT) %>% 
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
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
      read_sav(get_inpa_filename(year), col_select = c("RINPERSOONS", "RINPERSOON", "INPPERSBRUT")) %>% 
      rename(income = INPPERSBRUT) %>% 
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
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
  mutate(income = ifelse(income == 9999999999, NA, income)) 

# deflate
income_children <- 
  income_children %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(income = income / (cpi / 100)) %>% 
  select(-cpi)


# add year variable at which the child is a specific age
# to compute the mean of child income in a period. 
income_children <-
  income_children %>%
  rename(income_year = year) %>%
  left_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, year), 
            by = c("RINPERSOONS", "RINPERSOON"))


# compute the mean of the year the child is a specific age and the year before that
income_children <-
  income_children %>%
  arrange(RINPERSOON) %>%
  mutate(year = ifelse(income_year == (year - 1), (year - 1), year),
         year = ifelse(income_year  == year, year, NA)) %>%
  filter(!is.na(year)) %>%
  select(-year)
  

# compute mean
income_children <- 
  income_children %>% 
  group_by(RINPERSOONS, RINPERSOON) %>% 
  summarize(income_n = sum(!is.na(income)),
            income = mean(income, na.rm = TRUE))


# table of the number of years the mean income is based on
print(table(`income years` = income_children$income_n))

# add to data
cohort_dat <- left_join(cohort_dat, income_children %>% select(-income_n),
                        by = c("RINPERSOONS", "RINPERSOON"))

# free up memory
rm(income_children)


# if income is negative then income becomes NA
cohort_dat <- 
  cohort_dat %>%
  mutate(income = ifelse(income < 0, NA, income)) %>%
# remove income if income is NA 
  filter(!is.na(income))


# compute income transformations
cohort_dat <- 
  cohort_dat %>%
  group_by(birth_year) %>%
  mutate(
    income_rank = rank(income, ties.method = "average"),
    income_perc = income_rank / max(income_rank)
  ) %>%
  select(-c(income_rank)) %>%
  ungroup()



#### PREFIX ####

# add prefix to outcomes
outcomes <- c("income", "income_perc", "household_income", "household_income_perc")
suffix <- "c30_"


# rename outcomes
cohort_dat <- 
  cohort_dat %>%
  rename_with(~str_c(suffix, .), .cols = all_of(outcomes)) %>% 
  ungroup() 



#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes_attenuation.rds"))


