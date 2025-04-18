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
cohort_dat <- read_rds(file.path(loc$scratch_folder, "02_predictors.rds")) %>%
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
    path = file.path(loc$data_folder, "InkomenBestedingen/KOPPELPERSOONHUISHOUDEN"),
    pattern = paste0("KOPPELPERSOONHUISHOUDEN", year),
    full.names = TRUE   
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

get_koppeltabel_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/KOPPELPERSOONHUISHOUDEN"),
    pattern = paste0("Koppeltabel_IPI_IHI_IVB", year),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


income_household <- tibble(RINPERSOONS = factor(), RINPERSOON = character(),
                           household_income = double(), year = integer())
for (year in seq(2003, 2020)) {
  
  if (year <= 2010) {
    
    # load koppel data person to household
    koppel_hh <- read_sav(get_koppeltabel_filename(year)) %>%
      # select only incomes of children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>%
      mutate(RINPERSOONSKERN = as_factor(RINPERSOONSKERN, levels = "value"),
             RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
      rename(RINPERSOONSHKW = RINPERSOONSKERN,
             RINPERSOONHKW = RINPERSOONKERN)
    
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
      mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"),
             year = year)
    
  } else {
    # use tab
    income_children <-
      # read file from disk
      read_sav(get_inha_filename(year),
               col_select = c("RINPERSOONSHKW", "RINPERSOONHKW", "INHBRUTINKH")) %>%
      rename(household_income = INHBRUTINKH) %>%
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


#---------------------------------


mean_income_dat <- data.frame()

# 2003 - 2004
for (i in seq(2004, 2020)) {
  

 mean_income <-
    income_household %>%
    filter(year %in% c(i, (i - 1))) %>%
    group_by(RINPERSOONS, RINPERSOON) %>%
   dplyr::summarize(household_income = mean(household_income, na.rm=TRUE)) %>%
   mutate(income_age = i)
     
 
 mean_income_dat <- bind_rows(mean_income_dat, mean_income)             
} 
rm(i, mean_income)

# 2003 - 2004
# 2005 - 2006
# 2007 - 2008
# 2009 - 2010
# 2011 - 2012
# 2013 - 2014
# 2015 - 2016
# 2017 - 2018
# 2019 - 2020


# join to data
cohort_dat <- cohort_dat %>% left_join(mean_income_dat, by = c('RINPERSOONS', 'RINPERSOON'))

rm(mean_income_dat, income_household)


# if income is negative then household_income becomes NA
cohort_dat <-
  cohort_dat %>%
  mutate(household_income = ifelse(household_income < 0, NA, household_income)) %>%
  # remove income if income is NA 
  filter(!is.na(household_income))


# compute income transformations
cohort_dat <- 
  cohort_dat %>%
  group_by(income_age) %>%
  mutate(
    household_income_rank = rank(household_income, ties.method = "average"),
    household_income_perc = household_income_rank / max(household_income_rank)
  ) %>%
  select(-c(household_income_rank)) %>%
  ungroup()


# calculate age
cohort_dat <-
  cohort_dat %>%
  mutate(age_at_income = income_age - birth_year)



#### PREFIX ####

# add prefix to outcomes
outcomes <- c("household_income", "household_income_perc")
suffix <- "c30_"


# rename outcomes
cohort_dat <- 
  cohort_dat %>%
  rename_with(~str_c(suffix, .), .cols = all_of(outcomes)) %>% 
  ungroup() 



#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes_lifecycle.rds"))


