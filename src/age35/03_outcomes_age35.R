# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding (socio)economic outcomes to the cohort.
#   - Adding education outcomes to the cohort.
#   - Adding health outcomes to the cohort.
#   - Adding housing outcomes to the cohort.
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
cohort_dat <- read_rds(file.path(loc$scratch_folder, "02_predictors.rds")) %>%
# create variable that reflects the year the child turned a specific age 
  mutate(year = year(birthdate %m+% years(cfg$outcome_age))) %>%
  ungroup()


sample_size <- read_rds(file.path(loc$scratch_folder, "02_sample_size.rds"))


# load function for recording number of missings, negatives, not merged
source("src/function_kansenkaart_preprocessing.R")


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


sample_size <- sample_size %>% mutate(n_5_child_residency = nrow(cohort_dat))



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

get_koppeltabel_filename1 <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/KOPPELPERSOONHUISHOUDEN"),
    pattern = paste0("Koppeltabel_IPI_IHI_IVB", year),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


get_koppeltabel_filename2 <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/KOPPELPERSOONHUISHOUDEN"),
    pattern = paste0("KOPPELPERSOONHUISHOUDEN", year),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

children <- cohort_dat %>%
  select(RINPERSOONS, RINPERSOON, year) %>%
  mutate(year_min = year - 1) %>%
  pivot_longer(cols = starts_with('year'),
               values_to = 'year') %>% select(-name)

income_household <- tibble(RINPERSOONS = factor(), RINPERSOON = character(),
                           household_income = double(), primary_household_income = double(),
                           disposable_household_income = double(), year = integer())
for (year in seq(as.integer(cfg$child_income_year_min), as.integer(cfg$child_income_year_max))) {
  
  if (year <= 2010) {
    
    # load koppel data person to household
    koppel_hh <- read_sav(get_koppeltabel_filename1(year)) %>%
      # select only incomes of children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>%
      mutate(RINPERSOONSKERN = as_factor(RINPERSOONSKERN, levels = "value"),
             RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
      rename(RINPERSOONSHKW = RINPERSOONSKERN,
             RINPERSOONHKW = RINPERSOONKERN)
    
  } else {
    
    # load koppel data person to household
    koppel_hh <- read_sav(get_koppeltabel_filename2(year)) %>%
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
               col_select = c("RINPERSOONSKERN", "RINPERSOONKERN", "BVRBRUTINKH", 
                              "BVRPRIMINKH", "BVRBESTINKH")) %>%
      rename(household_income = BVRBRUTINKH,
             primary_household_income = BVRPRIMINKH,
             disposable_household_income = BVRBESTINKH, 
             RINPERSOONSHKW = RINPERSOONSKERN,
             RINPERSOONHKW = RINPERSOONKERN) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
             RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"),
             year = year)
    
  
  } else {
    # use tab
    income_children <-
      # read file from disk
      read_sav(get_inha_filename(year),
               col_select = c("RINPERSOONSHKW", "RINPERSOONHKW", "INHBRUTINKH", 
                              'INHBESTINKH', "INHPRIMINKH")) %>%
      rename(household_income = INHBRUTINKH,
             primary_household_income = INHPRIMINKH, 
             disposable_household_income = INHBESTINKH) %>%
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
  mutate(
    household_income = ifelse(household_income == 999999999 | household_income == 9999999999, NA, household_income),
    primary_household_income = ifelse(primary_household_income == 999999999 | primary_household_income == 9999999999, NA, primary_household_income),
    disposable_household_income = ifelse(disposable_household_income == 999999999 | disposable_household_income == 9999999999, NA, disposable_household_income))

#--------------------------------------------------------------

# link income to children
children_dat <- left_join(children, income_household, 
                          by = c("RINPERSOONS", "RINPERSOON", 'year'))

# number of missings
missings <- missings_function(children_dat, c('household_income', 'primary_household_income', 'disposable_household_income'))
missings_tab <- bind_rows(missings_tab, child_funct(missings, 'missing'))

# number of negatives
negatives <- negatives_function(children_dat, c('household_income', 'primary_household_income', 'disposable_household_income'))
negatives_tab <- bind_rows(negatives_tab, child_funct(negatives, 'negative'))

# number of zeros
zeros <- zeros_function(children_dat, c('household_income', 'primary_household_income', 'disposable_household_income'))
zeros_tab <- bind_rows(zeros_tab, child_funct(zeros, 'zero'))
rm(missings, negatives, zeros)

#--------------------------------------------------------------

# if negative then NA
income_household <-
  income_household %>% 
  mutate(
    household_income = ifelse(household_income < 0, NA, household_income), 
    primary_household_income = ifelse(primary_household_income < 0, NA, primary_household_income),
    disposable_household_income = ifelse(disposable_household_income < 0, NA, disposable_household_income))


# deflate
income_household <-
  income_household %>%
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>%
  mutate(household_income = household_income / (cpi/100), 
         disposable_household_income = disposable_household_income / (cpi/100), 
         primary_household_income = primary_household_income / (cpi/100)) %>%
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
  summarize(
    household_income = mean(household_income, na.rm = TRUE),
    disposable_household_income = mean(disposable_household_income, na.rm = TRUE),
    primary_household_income = mean(primary_household_income, na.rm = TRUE))


# add to data
cohort_dat <- left_join(cohort_dat, income_household,
                        by = c("RINPERSOONS", "RINPERSOON"))

# not merged
not_merged_tab <- 
  not_merged_func(c('household_income', 'disposable_household_income', 
                    'primary_household_income'))


# free up memory
rm(income_household)


# remove income if income is NA 
cohort_dat <- cohort_dat %>% filter(!is.na(household_income))


# compute income transformations
cohort_dat <- 
  cohort_dat %>%
  group_by(birth_year) %>%
  mutate(
    household_income_rank = rank(household_income, ties.method = "average", na.last = 'keep'),
    household_income_perc = household_income_rank / max(household_income_rank, na.rm = T),
    
    disposable_household_income_rank = rank(disposable_household_income, ties.method = "average", na.last = 'keep'),
    disposable_household_income_perc = disposable_household_income_rank / 
      max(disposable_household_income_rank, na.rm = T),
    
    primary_household_income_rank = rank(primary_household_income,  ties.method = "average", na.last = 'keep'),
    primary_household_income_perc = primary_household_income_rank / 
      max(primary_household_income_rank, na.rm = T)
  ) %>%
  select(-c(household_income_rank, disposable_household_income_rank, primary_household_income_rank)) %>%
  ungroup()


# create two dummies
cohort_dat <-
  cohort_dat %>%
  mutate(
    top_20_household_income = ifelse(household_income_perc >= 0.8, 1, 0),
    bottom_20_household_income = ifelse(household_income_perc <= 0.2, 1, 0))


sample_size <- sample_size %>% mutate(n_6_child_household_income = nrow(cohort_dat))



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


income_children <- data.frame()

for (year in seq(as.integer(cfg$child_income_year_min), as.integer(cfg$child_income_year_max))) {
  if (year < 2011) {
    # use IPI tab
    income_children <- 
      # read file from disk
      read_sav(get_ipi_filename(year), col_select = c("RINPERSOONS", "RINPERSOON", 
                                                      "PERSBRUT", "PERSPRIM")) %>% 
      rename(income = PERSBRUT, 
             earnings = PERSPRIM) %>% 
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
      # select only incomes of children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>% 
      # add year
      mutate(year = year) %>% 
      # add to income children
      bind_rows(income_children, .)
    
  } else if (year >= 2017) {
    # use INPA tab
    income_children <- 
      # read file from disk
      read_sav(get_inpa_filename(year), col_select = c("RINPERSOONS", "RINPERSOON", 
                                                       "INPPERSBRUT", "INPPERSPRIM")) %>% 
      rename(income = INPPERSBRUT,
             earnings = INPPERSPRIM) %>% 
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"), 
             earnings = ifelse(earnings == "Behoort tot een huishouden zonder waargenomen inkomen", NA, earnings),
             earnings = as.double(earnings)) %>%
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
      read_sav(get_inpa_filename(year), col_select = c("RINPERSOONS", "RINPERSOON", 
                                                       "INPPERSBRUT", "INPPERSPRIM")) %>% 
      rename(income = INPPERSBRUT,
             earnings = INPPERSPRIM) %>% 
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"), 
             earnings = as.double(earnings)) %>%
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
  mutate(income = ifelse(income == 9999999999 | income == 999999999, NA, income), 
         earnings = ifelse(earnings == 9999999999 | earnings == 999999999, NA, earnings)) 


#--------------------------------------------------------------

# link income to children 
children_dat <- left_join(children, income_children, 
                          by = c("RINPERSOONS", "RINPERSOON", 'year'))

# number of missings
missings <- missings_function(children_dat, c('income', 'earnings'))
missings_tab <- bind_rows(missings_tab, child_funct(missings, 'missing'))

# number of negatives
negatives <- negatives_function(children_dat, c('income', 'earnings'))
negatives_tab <- bind_rows(negatives_tab, child_funct(negatives, 'negative'))

# number of zeros
zeros <- zeros_function(children_dat, c('income', 'earnings'))
zeros_tab <- bind_rows(zeros_tab, child_funct(zeros, 'zero'))
rm(missings, negatives, zeros)

#--------------------------------------------------------------


# if negative then NA
income_children <-
  income_children %>% 
  mutate(income = ifelse(income < 0, NA, income), 
         earnings = ifelse(earnings < 0, NA, earnings))

# deflate
income_children <- 
  income_children %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(income = income / (cpi / 100), 
         earnings = earnings / (cpi / 100)) %>% 
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
  summarize(income = mean(income, na.rm = TRUE),
            earnings = mean(earnings, na.rm = TRUE))

# add to data
cohort_dat <- left_join(cohort_dat, income_children,
                        by = c("RINPERSOONS", "RINPERSOON"))

# not merged
not_merged_tab <- 
  not_merged_func(c('income', 'earnings'))


# free up memory
rm(income_children)


# remove income if income is NA 
# cohort_dat <- cohort_dat %>% filter(!is.na(income))


# compute income transformations
cohort_dat <- 
  cohort_dat %>%
  group_by(birth_year) %>%
  mutate(
    income_rank = rank(income, ties.method = "average", na.last = 'keep'),
    income_perc = income_rank / max(income_rank, na.rm = T),
    
    earnings_rank = rank(earnings, ties.method = "average", na.last = 'keep'),
    earnings_perc = earnings_rank / max(earnings_rank, na.rm = T)) %>%
  select(-c(income_rank, earnings_rank)) %>%
  ungroup()

# create two dummies
cohort_dat <-
  cohort_dat %>%
  mutate(
    top_20_income = ifelse(income_perc >= 0.8, 1, 0),
    bottom_20_income = ifelse(income_perc <= 0.2, 1, 0))


sample_size <- sample_size %>% mutate(n_7_child_income = nrow(cohort_dat))



#### HIGHER EDUCATION ####

# function to get latest version of specified year
get_school_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "Onderwijs/HOOGSTEOPLTAB", year),
    pattern = paste0("HOOGSTEOPL", year, "TABV[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


hopl_tab <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                   edu_attained = factor(), year = integer())
for (year in seq(as.integer(cfg$child_outcome_year_min), as.integer(cfg$child_outcome_year_max))) {
  
  if (year <= 2018) {
    # read file from disk
    hopl_tab <- read_sav(get_school_filename(year), 
                        col_select = c("RINPERSOONS", "RINPERSOON", 
                                       "OPLNIVSOI2016AGG4HBMETNIRWO")) %>% 
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"), 
             OPLNIVSOI2016AGG4HBMETNIRWO = as_factor(OPLNIVSOI2016AGG4HBMETNIRWO, 
                                                     levels = "value")) %>%
               rename(edu_attained = OPLNIVSOI2016AGG4HBMETNIRWO) %>%
      # add year
      mutate(year = year) %>% 
      # select only children in the data
      inner_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, year)) %>%
      # add to children data
      bind_rows(hopl_tab, .)

  } else if (year >= 2019) {
    
    # read file from disk
    hopl_tab <- read_sav(get_school_filename(year), 
                        col_select = c("RINPERSOONS", "RINPERSOON", 
                                       "OPLNIVSOI2021AGG4HBmetNIRWO")) %>% 
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"), 
             OPLNIVSOI2021AGG4HBmetNIRWO = as_factor(OPLNIVSOI2021AGG4HBmetNIRWO, 
                                                     levels = "value")) %>%
               rename(edu_attained = OPLNIVSOI2021AGG4HBmetNIRWO) %>%
      # add year
      mutate(year = year) %>% 
      # select only children in the data
      inner_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, year)) %>%
      # add to children data
      bind_rows(hopl_tab, .)
    }
  }


cohort_dat <- left_join(cohort_dat, hopl_tab, 
                        by = c("RINPERSOONS", "RINPERSOON", "year"))

# not merged
not_merged_tab <- not_merged_func('edu_attained')


# free up memory
rm(hopl_tab)


cohort_dat <-
  cohort_dat %>% 
  mutate(
    hbo_attained = as.integer(edu_attained %in% c("3110", "3111", "3211", "3112",
                                                  "3113", "3212", "3213")),
    wo_attained  = as.integer(edu_attained %in% c("3113", "3212", "3213"))
  ) %>%
  select(-edu_attained)



#### HOURLY INCOME ####

get_spolis_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "Spolis/SPOLISBUS"),
    pattern = paste0("SPOLISBUS", year, "V[0-9]+\\.sav"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

# create dataframe with all spolis entries for selected years
spolis_tab <- tibble(
  RINPERSOONS   = factor(),
  RINPERSOON    = character(),
  hourly_wage = double(),
  work_hours    = double(),
  contract_type = factor(),
  year = integer()
)

for (year in seq(as.integer(cfg$child_outcome_year_min), as.integer(cfg$child_outcome_year_max))) {
  spolis_tab <-
    # read file from disk
    read_sav(get_spolis_filename(year),
             col_select = c("RINPERSOONS", "RINPERSOON", "SBASISLOON",
                            "SBASISUREN", "SCONTRACTSOORT")) %>%
    # add year
    mutate(year = year) %>%
    rename(
      hourly_wage = SBASISLOON,
      work_hours    = SBASISUREN,
      contract_type = SCONTRACTSOORT
    ) %>%
    # add year
    mutate(
      contract_type = as_factor(contract_type),
      RINPERSOONS = as_factor(RINPERSOONS, levels = "value")
    ) %>%
    # select only wages of children
    filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>%
    inner_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, year)) %>%
    # add to children wages
    bind_rows(spolis_tab, .)
}


# remove negative incomes
spolis_tab <-
  spolis_tab %>%
  mutate(hourly_wage = ifelse(hourly_wage < 0, NA, hourly_wage))

# deflate
spolis_tab <-
  spolis_tab %>%
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>%
  mutate(hourly_wage = hourly_wage / (cpi / 100)) %>%
  select(-cpi)

# compute aggregates
longest_contract_tab <-
  spolis_tab %>%
  group_by(RINPERSOONS, RINPERSOON, contract_type) %>%
  summarise(total_hours = sum(work_hours, na.rm = TRUE)) %>%
  arrange(
    desc(total_hours),    # most hours contract at top
    desc(contract_type),  # in case of same hours, prefer "onbepaalde tijd" over "bepaalde tijd"
    .by_group = TRUE
  ) %>%
  summarise(longest_contract_type = contract_type[1])


income_hours_tab <-
  spolis_tab %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  summarize(
    spolis_n       = n(),
    hourly_wage  = sum(hourly_wage, na.rm = TRUE) / sum(work_hours, na.rm = TRUE),
    hrs_work_pw = sum(work_hours) / 52 # total number of weeks in a year
  )


# combine variables
spolis_tab <- left_join(income_hours_tab, longest_contract_tab) %>%
  # compute permanent contract dummy
  mutate(permanent_contract = ifelse(longest_contract_type == "Onbepaalde tijd", 1, 0))

# add to cohort data
cohort_dat <- left_join(cohort_dat, spolis_tab)

# not merged
not_merged_tab <- not_merged_func(c('permanent_contract', 'hourly_wage', 'hrs_work_pw'))


# free up memory
rm(spolis_tab, longest_contract_tab, income_hours_tab)

# post-process: compute additional values
cohort_dat <-
  cohort_dat %>%
  ungroup() %>%
  mutate(
    hourly_wage_max_11 = ifelse(hourly_wage < 11, 1, 0),
    hourly_wage_max_14 = ifelse(hourly_wage < 14, 1, 0)
  ) %>%
  mutate(
    hourly_wage_max_11 = ifelse(is.na(hourly_wage), NA, hourly_wage_max_11),
    hourly_wage_max_14 = ifelse(is.na(hourly_wage), NA, hourly_wage_max_14)
  ) %>%
  select(-c(spolis_n, longest_contract_type))


#### SOCIOECONOMIC ####

secm_tab <- 
  read_sav(file.path(loc$data_folder, loc$secm_data), 
           col_select = c("RINPERSOONS", "RINPERSOON", "AANVSECM", "EINDSECM", "SECM")) %>%
  # select only children in the data
  filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>% 
  mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
         SECM = as_factor(SECM, levels = "values")) %>%
  mutate(
    AANVSECM = ymd(AANVSECM),
    EINDSECM = ymd(EINDSECM)
  ) %>%
  # remove entries outside of the target date
  filter(!(EINDSECM <= ymd(paste0(cfg$child_outcome_year_min, "-01-01"))), 
         !(AANVSECM >= ymd(paste0(cfg$child_outcome_year_max, "-12-31")))) 


# keep records measured a specific age
secm_tab <- 
  secm_tab %>%
  inner_join(cohort_dat %>%
              select(c("RINPERSOONS", "RINPERSOON", "year")),
            by = c("RINPERSOONS", "RINPERSOON")) %>%
  mutate(secm_dat = ymd(paste0(year, "-12-31"))) %>%
  filter(secm_dat %within% interval(AANVSECM, EINDSECM)) 
  

cohort_dat <- left_join(cohort_dat, 
                        secm_tab %>% select(RINPERSOONS, RINPERSOON, SECM))

# not merged
not_merged_tab <- not_merged_func('SECM')


# free up memory
rm(secm_tab)


# create outcomes
cohort_dat <- 
  cohort_dat %>%
  mutate(
    # 11  = Werknemer
    # 12  = Directeur-grootaandeelhouder
    # 13  = Zelfstandig ondernemer
    # 14  = Overige zelfstandige
    employed = as.integer(SECM %in% c(11, 12, 13, 14)),
    
    # 22 =  Ontvanger bijstandsuitkering
    social_assistance = as.integer(SECM == 22),
    
    # 24  = Ontvanger uitkering ziekte/AO
    disability = as.integer(SECM == 24)
  ) %>%
  select(-SECM)



#### HEALTH COSTS ####

# function to get latest version of specified year
get_health_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "GezondheidWelzijn/ZVWZORGKOSTENTAB", year),
    pattern = paste0("ZVWZORGKOSTEN", year), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


health_tab <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                     pharma = integer(), specialist_mhc = integer(), 
                     hospital = integer(), total_health_costs = double(), 
                     year = integer())
for (year in seq(as.integer(cfg$child_outcome_year_min), as.integer(cfg$child_outcome_year_max))) {
  
  if (year == 2016 | year == 2017) {
    health_tab <- read_sav(get_health_filename(year),
                           col_select = c("RINPERSOONS", "RINPERSOON", "ZVWKHUISARTS", "ZVWKMULTIDISC",
                                          "ZVWKFARMACIE", "ZVWKMONDZORG", "ZVWKZIEKENHUIS", "ZVWKGEBOORTEZORG",        
                                          "ZVWKPARAMEDISCH", "ZVWKHULPMIDDEL", "ZVWKZIEKENVERVOER", 
                                          "ZVWKBUITENLAND", "ZVWKOVERIG", "ZVWKGERIATRISCH", 
                                          "ZVWKWYKVERPLEGING", "NOPZVWKHUISARTSINSCHRIJF", 
                                          "ZVWKSPECGGZ", "ZVWKGENBASGGZ")) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values"), 
             year = year) %>%
      # select only children
      inner_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, year)) %>%
      # replace negative values with 0
      mutate(across(.cols = starts_with("ZVWK"),
             .fns = ~ ifelse(.x < 0 , 0, .x))) %>%
      # replace NA values with 0
      mutate(across(.cols = starts_with("ZVWK"),
                    .fns = ~ ifelse(is.na(.x), 0, .x))
      ) %>%
      ungroup() %>%
      mutate(pharma             = ifelse(ZVWKFARMACIE > 0, 1, 0),
             basic_mhc          = ifelse((ZVWKGENBASGGZ > 0 | ZVWKSPECGGZ > 0), 1, 0),
             specialist_mhc     = ifelse(ZVWKSPECGGZ > 0, 1, 0),
             hospital           = ifelse(ZVWKZIEKENHUIS > 0, 1, 0),
             total_health_costs = rowSums(across(c(ZVWKHUISARTS, ZVWKMULTIDISC, ZVWKFARMACIE, 
                                                   ZVWKMONDZORG, ZVWKZIEKENHUIS, ZVWKPARAMEDISCH, 
                                                   ZVWKHULPMIDDEL, ZVWKZIEKENVERVOER, ZVWKBUITENLAND, 
                                                   ZVWKOVERIG, ZVWKGERIATRISCH, ZVWKWYKVERPLEGING))),
             total_health_costs = total_health_costs - (NOPZVWKHUISARTSINSCHRIJF + ZVWKGEBOORTEZORG)
      ) %>%
      select(c("RINPERSOONS", "RINPERSOON", "pharma", "basic_mhc", "specialist_mhc",
             "hospital", "total_health_costs", "year")) %>%
      bind_rows(health_tab, .)
    
  } else if (year >= 2018) {
   
     health_tab <- read_sav(get_health_filename(year),
                           col_select = c("RINPERSOONS", "RINPERSOON", "ZVWKHUISARTS", "ZVWKMULTIDISC",
                                          "ZVWKFARMACIE", "ZVWKMONDZORG", "ZVWKZIEKENHUIS", 
                                          "ZVWKEERSTELIJNSVERBLIJF", "ZVWKPARAMEDISCH", "ZVWKHULPMIDDEL", 
                                          "ZVWKZIEKENVERVOER", "ZVWKBUITENLAND", "ZVWKOVERIG", 
                                          "ZVWKGERIATRISCH", "ZVWKGEBOORTEZORG", "ZVWKWYKVERPLEGING", 
                                          "NOPZVWKHUISARTSINSCHRIJF", "ZVWKSPECGGZ", "ZVWKGENBASGGZ")) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values"), 
             year = year) %>%
      # select only children
      inner_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, year)) %>%
      # replace negative values with 0
      mutate(across(.cols = starts_with("ZVWK"),
                    .fns = ~ ifelse(.x < 0 , 0, .x))) %>%
      # replace NA values with 0
      mutate(across(.cols = starts_with("ZVWK"),
                    .fns = ~ ifelse(is.na(.x), 0, .x))
      ) %>%
      ungroup() %>%
      mutate(
        pharma             = ifelse(ZVWKFARMACIE > 0, 1, 0),
             basic_mhc          = ifelse((ZVWKGENBASGGZ > 0 | ZVWKSPECGGZ > 0), 1, 0),
             specialist_mhc     = ifelse(ZVWKSPECGGZ > 0, 1, 0),
             hospital           = ifelse(ZVWKZIEKENHUIS > 0, 1, 0),
             total_health_costs = rowSums(across(c(ZVWKHUISARTS, ZVWKMULTIDISC, ZVWKFARMACIE, 
                                        ZVWKMONDZORG, ZVWKZIEKENHUIS, ZVWKEERSTELIJNSVERBLIJF, 
                                        ZVWKPARAMEDISCH, ZVWKHULPMIDDEL, ZVWKZIEKENVERVOER, 
                                        ZVWKBUITENLAND, ZVWKOVERIG, ZVWKGERIATRISCH, 
                                        ZVWKWYKVERPLEGING))), 
             total_health_costs = (total_health_costs - (ZVWKGEBOORTEZORG + NOPZVWKHUISARTSINSCHRIJF))
      ) %>%
      select(c("RINPERSOONS", "RINPERSOON", "pharma", "basic_mhc", "specialist_mhc",
               "hospital", "total_health_costs", "year")) %>%
      bind_rows(health_tab, .)
    
  }
}

cohort_dat <- left_join(cohort_dat, health_tab,
                        by = c("RINPERSOONS", "RINPERSOON", "year"))

# not merged
not_merged_tab <- not_merged_func('total_health_costs')

# free up memory
rm(health_tab)


# replace NA with 0 (for those who are not merged with the zvwzorgkostentab)
cohort_dat <- cohort_dat %>%
  mutate(pharma             = ifelse(is.na(pharma), 0, pharma),
         basic_mhc          = ifelse(is.na(basic_mhc), 0, basic_mhc),
         specialist_mhc     = ifelse(is.na(specialist_mhc), 0, specialist_mhc),
         hospital           = ifelse(is.na(hospital), 0, hospital),
         total_health_costs = ifelse(is.na(total_health_costs), 0, total_health_costs))



#### WEALTH ####
# VEHW1000VERH = Vermogen van het huishouden.
# VEHWVEREXEWH = Vermogen van het huishouden exclusief eigen woning.


# get wealth data from each requested year into single data frame
get_veh_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/VEHTAB"),
    pattern = paste0("VEH", year),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}



wealth_child <- tibble(RINPERSOONS = factor(), RINPERSOON = character(),
                     wealth = double(), wealth_no_home =  double(),
                     year = integer())
for (year in seq(as.integer(cfg$child_outcome_year_min), as.integer(cfg$child_outcome_year_max))) {
  
  if (year <= 2010) {
    
    # load koppel data person to household
    koppel_hh <- read_sav(get_koppeltabel_filename1(year)) %>%
      # select only incomes of children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>%
      mutate(RINPERSOONSKERN = as_factor(RINPERSOONSKERN, levels = "value"),
             RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
      rename(RINPERSOONSHKW = RINPERSOONSKERN,
             RINPERSOONHKW = RINPERSOONKERN)
    
  } else {
    
    # load koppel data person to household
    koppel_hh <- read_sav(get_koppeltabel_filename2(year)) %>%
      # select only incomes of children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>% 
      mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"), 
             RINPERSOONS = as_factor(RINPERSOONS, levels = "value"))
  }
  
  # use VEH tab
  veh_child <-
    # read file from disk
    read_sav(get_veh_filename(year), col_select = c("RINPERSOONSHKW", "RINPERSOONHKW",
                                                    "VEHW1000VERH", "VEHWVEREXEWH")) %>%
    rename(wealth = VEHW1000VERH, 
           wealth_no_home = VEHWVEREXEWH) %>%
    mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value")) %>%
    # add year
    mutate(year = year) %>%
    inner_join(koppel_hh, by = c("RINPERSOONSHKW", "RINPERSOONHKW")) %>%
    select(-c(RINPERSOONSHKW, RINPERSOONHKW))
  
  wealth_child <- bind_rows(wealth_child, veh_child)
}
rm(veh_child, koppel_hh)


# remove NA incomes
wealth_child <-
  wealth_child %>%
  mutate(wealth = ifelse(wealth == 99999999999, NA, wealth), 
         wealth_no_home = ifelse(wealth_no_home == 99999999999, NA, wealth_no_home))


# deflate
wealth_child <-
  wealth_child %>%
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>%
  mutate(wealth         = wealth / (cpi / 100), 
         wealth_no_home = wealth_no_home / (cpi / 100)) %>%
  select(-cpi)


cohort_dat <- 
  cohort_dat %>% 
  left_join(wealth_child, by = c("RINPERSOONS", "RINPERSOON", "year"))

rm(wealth_child)


# not merged
not_merged_tab <- not_merged_func(c('wealth'))


# create debt and home value outcome
cohort_dat <- 
  cohort_dat %>%
  mutate(
    debt        = ifelse(wealth >= 0, 0, abs(wealth)),
    debt        = ifelse(is.na(wealth), NA, debt), 
    home_wealth = wealth - wealth_no_home)


# create wealth rank
cohort_dat <- 
  cohort_dat %>%
  group_by(birth_year) %>%
  mutate(
    wealth_rank = rank(wealth, ties.method = "average", na.last = 'keep'),
    wealth_perc = wealth_rank / max(wealth_rank, na.rm = T))


sample_size <- sample_size %>% mutate(n_8_child_wealth = nrow(cohort_dat))



#### HOMEOWNER: PROPERTY BOUGHT OR RENT ####

# find home address
adres_path <- file.path(loc$data_folder, loc$gbaao_data)
adres_dat  <- read_sav(adres_path) %>% 
  mutate(
    RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
    SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"),
    GBADATUMAANVANGADRESHOUDING = ymd(GBADATUMAANVANGADRESHOUDING),
    GBADATUMEINDEADRESHOUDING = ymd(GBADATUMEINDEADRESHOUDING)
  ) 

adres_tab <- 
  adres_dat %>%
  # take addresses that are still open on a specific date
  filter(GBADATUMAANVANGADRESHOUDING <= ymd(paste0(cfg$child_outcome_year_max, "-01-01")) &
           GBADATUMEINDEADRESHOUDING >= ymd(paste0(cfg$child_outcome_year_min, "-01-01"))) 


# keep children at a specific date
adres_tab <- adres_tab %>%
  left_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, year), 
            by = c("RINPERSOONS", "RINPERSOON")) %>%
  # data at which we measure property
  mutate(date = ymd(paste0(year, "-01-01"))) %>%
  # only keep address at specific data
  filter(date %within% interval(GBADATUMAANVANGADRESHOUDING, GBADATUMEINDEADRESHOUDING))
  


# function for property data
get_property_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "BouwenWonen/EIGENDOMTAB"),
    pattern = paste0("EIGENDOM", year), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


property_dat <- tibble(SOORTOBJECTNUMMER = factor(), RINOBJECTNUMMER = character(), 
                       TypeEigenaar = factor(), year = integer())
for (year in seq(as.integer(cfg$child_outcome_year_min), as.integer(cfg$child_outcome_year_max))) {
  # load property data
  property_dat <- read_sav(get_property_filename(year), 
                           col_select = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER", "TypeEigenaar")) %>%
    mutate(SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"), 
           TypeEigenaar = as_factor(TypeEigenaar, levels = "values"), 
           year = year) %>%
    bind_rows(property_dat, .)
  }
 
adres_tab <- left_join(adres_tab, property_dat,
                       by = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER", "year"))


cohort_dat <- 
  cohort_dat %>% 
  left_join(adres_tab %>% select(RINPERSOONS, RINPERSOON, TypeEigenaar), 
            by = c("RINPERSOONS", "RINPERSOON"))


# not merged
not_merged_tab <- not_merged_func('TypeEigenaar')


# create home owner dummy
cohort_dat <- 
  cohort_dat %>%
  mutate(
    homeowner = ifelse(TypeEigenaar == "E", 1, 0),
    homeowner = ifelse(is.na(homeowner), NA, homeowner)
  ) %>%
  select(-TypeEigenaar)

rm(adres_tab, property_dat)



#### GIFTS RECEIVED ####


get_schtab_filename <- function(year) {
    # function to get latest version of specified year
    fl <- list.files(
      path = file.path(loc$data_folder, "InkomenBestedingen/SCHTAB", year),
      pattern = "SCH", year,
      full.names = TRUE
    )
    # return only the latest version
    sort(fl, decreasing = TRUE)[1]
}

gifts_tab <- tibble(RINPERSOONSONTVANGER = factor(), RINPERSOONONTVANGER = character(), 
                        SCHBRUTSCHK = double(), year = integer())
for (year in seq(as.integer(cfg$gifts_dat_start), as.integer(cfg$gifts_dat_end))) {
 
  gifts_tab <- read_sav(get_schtab_filename(year), 
                            col_select = c("RINPERSOONSONTVANGER", "RINPERSOONONTVANGER", 
                                           "SCHTYPEVRIJ", "SCHBRUTSCHK")) %>%
    mutate(RINPERSOONSONTVANGER = as_factor(RINPERSOONSONTVANGER, levels = "values"), 
           SCHTYPEVRIJ = as_factor(SCHTYPEVRIJ, levels = "values"), 
           year = year) %>%
    filter(
      RINPERSOONONTVANGER %in% cohort_dat$RINPERSOON,
      SCHTYPEVRIJ %in% c("1", "2", "3")
           ) %>%
    select(-SCHTYPEVRIJ) %>%
    # add to tibble
    bind_rows(gifts_tab, .)
}


gifts_tab <-
  # add birth year to data
  left_join(gifts_tab, cohort_dat %>% select(RINPERSOONS, RINPERSOON, birth_year), 
            by = c("RINPERSOONSONTVANGER" = "RINPERSOONS", 
                   "RINPERSOONONTVANGER" = "RINPERSOON")
            ) %>%
  # ages at which we measure when the child has received a gifts from the parents
  mutate(
    gifts_start = birth_year + cfg$gifts_year_start, 
    gifts_end = birth_year + cfg$gifts_year_end
    ) %>%
  # only keep gifts that are between the gifts age year range
  filter(year > gifts_start & year < gifts_end) 


# deflate 
gifts_tab <-
  gifts_tab %>%
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>%
  mutate(SCHBRUTSCHK = SCHBRUTSCHK / (cpi / 100)) %>%
  select(-cpi)


# sum all gifts received
gifts_tab <-
  gifts_tab %>%
  group_by(RINPERSOONSONTVANGER, RINPERSOONONTVANGER) %>%
  summarize(sum_gifts = sum(SCHBRUTSCHK))


cohort_dat <-
  left_join(cohort_dat, gifts_tab,
            by = c("RINPERSOONS" = "RINPERSOONSONTVANGER",
                   "RINPERSOON" = "RINPERSOONONTVANGER"))

rm(gifts_tab)

# not merged
not_merged_tab <- not_merged_func('sum_gifts')


# create binary gifts outcome
cohort_dat <-
  cohort_dat %>%
  mutate(gifts_received = ifelse(!is.na(sum_gifts), 1, 0), 
         sum_gifts = ifelse(is.na(sum_gifts), 0, sum_gifts)) # code NA to 0



#### LIVING SPACE PER HOUSEHOLD MEMBER ####


# LIVING SPACE PP AT AGE 35
   
# select home addresses
adres_tab <- 
  adres_dat %>% 
  filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>%
  filter(!(GBADATUMEINDEADRESHOUDING < ymd(paste0(cfg$child_outcome_year_min, "0101"))),
         !(GBADATUMAANVANGADRESHOUDING > ymd(paste0(cfg$child_outcome_year_max, "0101"))))


# create 1 january variable for current year the child is age 35 
cohort_dat <- cohort_dat %>%
  mutate(january = ymd(paste0(year, "-01-01")))


# add 1 january data to adres tab to filter for home addresses at 1 jan
adres_tab <- 
  adres_tab %>%
  left_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, january)) %>%
  filter(january %within% interval(GBADATUMAANVANGADRESHOUDING, GBADATUMEINDEADRESHOUDING)) 

# add home addresses to data
cohort_dat <- left_join(cohort_dat, adres_tab %>% 
                          select(RINPERSOONS, RINPERSOON, 
                                 SOORTOBJECTNUMMER, RINOBJECTNUMMER),
                        by = c("RINPERSOONS", "RINPERSOON"))
rm(adres_tab) 

# not merged
not_merged_tab <- not_merged_func('SOORTOBJECTNUMMER')


# LIVING SPACE 

woon_dat <-
  read_sav(file.path(loc$data_folder, loc$woon_data),
           col_select = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER", "VBOOPPERVLAKTE", 
                          "AANVLEVCYCLWOONNIETWOON", "EINDLEVCYCLWOONNIETWOON")
           ) %>%
  mutate(SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"), 
         AANVLEVCYCLWOONNIETWOON = ymd(AANVLEVCYCLWOONNIETWOON))

woon_tab <-
  woon_dat %>%
  mutate(EINDLEVCYCLWOONNIETWOON = ifelse(EINDLEVCYCLWOONNIETWOON == "88888888",
                                          paste0(cfg$child_outcome_year_max, "1231"),
                                          EINDLEVCYCLWOONNIETWOON),
         EINDLEVCYCLWOONNIETWOON = ymd(EINDLEVCYCLWOONNIETWOON)
  ) %>%
  filter(!(EINDLEVCYCLWOONNIETWOON < ymd(paste0(cfg$child_outcome_year_min, "-01-01"))),
         !(AANVLEVCYCLWOONNIETWOON > ymd(paste0(cfg$child_outcome_year_max, "-01-01"))))


# add 1 january data to woon tab to filter for home addresses at 1 jan
woon_tab <- 
  woon_tab %>%
  left_join(cohort_dat %>% select(SOORTOBJECTNUMMER, RINOBJECTNUMMER, january), 
            by = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER")) %>%
  filter(january %within% interval(AANVLEVCYCLWOONNIETWOON, EINDLEVCYCLWOONNIETWOON)) %>%
  unique()


# add living space to data
cohort_dat <- left_join(cohort_dat,
                        woon_tab %>% select(SOORTOBJECTNUMMER, RINOBJECTNUMMER, VBOOPPERVLAKTE, january),
                        by = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER", "january"))  %>%
  mutate(VBOOPPERVLAKTE = as.numeric(VBOOPPERVLAKTE))

rm(woon_tab)



# NUMBER OF HOUSEHOLD MEMBERS

# function to get latest eigendom version of specified year
get_eigendom_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "BouwenWonen/EIGENDOMTAB"),
    pattern = paste0("EIGENDOM", year), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


household_members <- tibble(SOORTOBJECTNUMMER = factor(), RINOBJECTNUMMER = character(), 
                            AantalBewoners = double(), year = integer())
for (year in seq(as.integer(cfg$child_outcome_year_min), as.integer(cfg$child_outcome_year_max))) {
  
  household_members <- read_sav(get_eigendom_filename(year), 
                                col_select = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER",
                                               "AantalBewoners")) %>%
    mutate(SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"),
           AantalBewoners = as.numeric(AantalBewoners),
           year = year) %>%
    # add to household member dat
    bind_rows(household_members, .)
}


cohort_dat <- 
  cohort_dat %>%
  left_join(household_members, by = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER", "year")) %>%
  mutate(AantalBewoners = as.numeric(AantalBewoners),
         AantalBewoners = ifelse(AantalBewoners == 0, NA, AantalBewoners)) 


# not merged
not_merged_tab <- not_merged_func(c('VBOOPPERVLAKTE', 'AantalBewoners'))


# create living space per household member outcome at age 35
cohort_dat <- 
  cohort_dat %>%
  mutate(living_space_pp = VBOOPPERVLAKTE / AantalBewoners) %>%
  select(-c(january, SOORTOBJECTNUMMER, RINOBJECTNUMMER, 
            VBOOPPERVLAKTE, AantalBewoners))


# not merged
not_merged_tab <- not_merged_func(c('living_space_pp'))

# change name
not_merged_tab <-
  not_merged_tab %>%
  mutate(outcome = recode(outcome, 
                          'SOORTOBJECTNUMMER' = 'home address - age 35',
                          'VBOOPPERVLAKTE' = 'VBOOPPERVLAKTE - age 35',
                          'AantalBewoners' = 'AantalBewoners - age 35',
                          'living_space_pp' = 'living_space_pp - age 35'))


rm(household_members)


# LIVING SPACE PP AT AGE 34 
# create living space pp at age 34 for those who are NA at age 35 (NA = 12.899)

# LIVING SPACE
adres_tab <- 
  adres_dat %>% 
  # select only children
  filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>%
  filter(!(GBADATUMEINDEADRESHOUDING < ymd(paste0((cfg$child_outcome_year_min - 1), "0101"))),
         !(GBADATUMAANVANGADRESHOUDING > ymd(paste0((cfg$child_outcome_year_max - 1), "0101"))))


# create 1 january variable for current year the child is age 34 
cohort_dat <- cohort_dat %>%
  mutate(january = ymd(paste0((year-1), "-01-01")))


# add 1 january data to adres tab to filter for home addresses at 1 jan
adres_tab <- 
  adres_tab %>%
  left_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, january)) %>%
  filter(january %within% interval(GBADATUMAANVANGADRESHOUDING, GBADATUMEINDEADRESHOUDING)) 


# add home addresses to data
cohort_dat <- left_join(cohort_dat, adres_tab %>% 
                          select(RINPERSOONS, RINPERSOON, SOORTOBJECTNUMMER, RINOBJECTNUMMER),
                        by = c("RINPERSOONS", "RINPERSOON"))
rm(adres_tab)

# not merged
not_merged_tab <- not_merged_func('SOORTOBJECTNUMMER')


# living space at age 34
woon_tab <-
  woon_dat %>%
  mutate(EINDLEVCYCLWOONNIETWOON = ifelse(EINDLEVCYCLWOONNIETWOON == "88888888",
                                          paste0(cfg$child_outcome_year_max, "1231"),
                                          EINDLEVCYCLWOONNIETWOON),
         EINDLEVCYCLWOONNIETWOON = ymd(EINDLEVCYCLWOONNIETWOON)
  ) %>%
  filter(!(EINDLEVCYCLWOONNIETWOON < ymd(paste0((cfg$child_outcome_year_min - 1), "-01-01"))),
         !(AANVLEVCYCLWOONNIETWOON > ymd(paste0((cfg$child_outcome_year_max - 1), "-01-01"))))


# add 1 january data to woon tab to filter for home addresses at 1 jan
woon_tab <- 
  woon_tab %>%
  left_join(cohort_dat %>% select(SOORTOBJECTNUMMER, RINOBJECTNUMMER, january), 
            by = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER")) %>%
  filter(january %within% interval(AANVLEVCYCLWOONNIETWOON, EINDLEVCYCLWOONNIETWOON)) %>%
  unique()


# add living space to data
cohort_dat <- left_join(cohort_dat,
                        woon_tab %>% select(SOORTOBJECTNUMMER, RINOBJECTNUMMER, VBOOPPERVLAKTE, january),
                        by = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER", "january"))  %>%
  mutate(VBOOPPERVLAKTE = as.numeric(VBOOPPERVLAKTE))

rm(woon_tab)


# NUMBER OF HOUSEHOLD MEMBERS
household_members <- tibble(SOORTOBJECTNUMMER = factor(), RINOBJECTNUMMER = character(), 
                            AantalBewoners = double(), year = integer())
for (year in seq(as.integer(cfg$child_outcome_year_min - 1), 
                 as.integer(cfg$child_outcome_year_max - 1))) {
  
  household_members <- read_sav(get_eigendom_filename(year), 
                                col_select = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER",
                                               "AantalBewoners")) %>%
    mutate(SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"),
           AantalBewoners = as.numeric(AantalBewoners),
           year = year) %>%
    # add to household member dat
    bind_rows(household_members, .)
}


cohort_dat <- 
  cohort_dat %>%
  left_join(household_members, by = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER", "year")) %>%
  mutate(AantalBewoners = as.numeric(AantalBewoners),
         AantalBewoners = ifelse(AantalBewoners == 0, NA, AantalBewoners)) 

# not merged
not_merged_tab <- not_merged_func(c('VBOOPPERVLAKTE', 'AantalBewoners'))


# create living space per household member outcome at age 34
cohort_dat <- 
  cohort_dat %>%
  mutate(living_space_pp = ifelse(is.na(living_space_pp), 
                                  VBOOPPERVLAKTE / AantalBewoners, living_space_pp)) %>%
  select(-c(january, VBOOPPERVLAKTE, AantalBewoners))

# not merged
not_merged_tab <- not_merged_func(c('living_space_pp'))

# change name
not_merged_tab <-
  not_merged_tab %>%
  mutate(outcome = recode(outcome, 
                          'SOORTOBJECTNUMMER' = 'home address - age 34',
                          'VBOOPPERVLAKTE' = 'VBOOPPERVLAKTE - age 34',
                          'AantalBewoners' = 'AantalBewoners - age 34',
                          'living_space_pp' = 'living_space_pp - age 34'))

rm(household_members, adres_dat, woon_dat)


#### AGE LEFT PARENTS ####
# load household data
household_dat <-
  read_sav(file.path(loc$data_folder, loc$household_data),
           col_select = c("RINPERSOONS", "RINPERSOON", "DATUMAANVANGHH",
                          "DATUMEINDEHH", "PLHH")) %>%
  filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>%
  mutate(
    RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
    DATUMAANVANGHH = as.numeric(DATUMAANVANGHH),
    DATUMEINDEHH = as.numeric(DATUMEINDEHH),
    PLHH = as_factor(PLHH, levels = "value")
  ) %>%
  mutate(
    DATUMAANVANGHH = ymd(DATUMAANVANGHH),
    DATUMEINDEHH = ymd(DATUMEINDEHH)
  )                                                                                 

# select the age at which children left their parents home
hh_dat <- tibble(RINPERSOONS = cohort_dat$RINPERSOONS, 
                 RINPERSOON = cohort_dat$RINPERSOON) %>%
  mutate (age = 35)

for(year in seq(cfg$outcome_age, cfg$childhood_home_age)) {
  age_tab <- cohort_dat %>%
    select(RINPERSOONS, RINPERSOON, birthdate) %>%
    mutate(home_address_date = birthdate %m+% years(year)) %>%
    select(-birthdate)
  
  hh_tab <- 
    household_dat %>%
    left_join(age_tab, by = c("RINPERSOONS", "RINPERSOON")) %>%
    group_by(RINPERSOONS, RINPERSOON) %>% 
    filter(DATUMAANVANGHH <= home_address_date & 
             DATUMEINDEHH >= home_address_date) %>%
    summarize(PLHH = as.numeric(PLHH[1]))
  
  # for children with missing observation on birthday, find the next available registration within one year 
  hh_tab_missing <-
    household_dat %>%
    left_join(age_tab, by = c("RINPERSOONS", "RINPERSOON")) %>%
    mutate(pl_missing = if_else((RINPERSOON %in% hh_tab$RINPERSOON), 0, 1)) %>%
    filter (pl_missing == 1) %>% 
    group_by(RINPERSOONS, RINPERSOON) %>%
    filter(DATUMAANVANGHH > home_address_date) %>%
    summarise(
      PLHH = as.numeric(PLHH[1]),
      birthday = home_address_date[1],
      date_PLHH = DATUMAANVANGHH[1]) %>%
    mutate(missing = if_else((date_PLHH-birthday > 365),1,0)) %>%
    filter(missing == 0) %>%
    select(RINPERSOON, RINPERSOONS, PLHH)
  
  hh_tab <- rbind (hh_tab, hh_tab_missing)
  
  hh_dat <- 
    hh_dat %>%
    left_join (hh_tab, by = c("RINPERSOONS", "RINPERSOON")) 
  
  hh_dat <- 
    hh_dat %>%
    mutate (living_at_home = if_else(PLHH == (1 | 10)  | is.na(PLHH), 1, 0)) %>% 
    mutate (age = if_else(living_at_home == 1 | is.na(living_at_home), age, year)) %>%
    select (-c(PLHH))
}  


cohort_dat <- left_join(cohort_dat, hh_dat %>% 
                          select(-living_at_home)%>%
                          rename (age_left_parents = age),
                        by = c("RINPERSOONS", "RINPERSOON")) %>%
  select(-c(SOORTOBJECTNUMMER, RINOBJECTNUMMER))

# not merged
not_merged_tab <- not_merged_func(c('age_left_parents'))



# free up memory
rm(hh_tab, hh_tab_missing, age_tab, household_dat, hh_dat)




#### PREFIX ####

# add prefix to outcomes
outcomes <- c("income", "income_perc", "hbo_attained", "wo_attained", "hourly_wage", 
              "hrs_work_pw", "permanent_contract", "hourly_wage_max_11", 
              "hourly_wage_max_14", "employed", "social_assistance", "disability", 
              "total_health_costs", "basic_mhc", "specialist_mhc", "hospital", 
              "pharma", "debt", "wealth", "wealth_perc", 
              "wealth_no_home", "home_wealth", "homeowner",
              "gifts_received", "household_income", "household_income_perc", 
              "living_space_pp", "sum_gifts", "age_left_parents", 
              "disposable_household_income_perc", "disposable_household_income", 
              "primary_household_income_perc", "primary_household_income", 
              "earnings", "earnings_perc", "top_20_household_income", 
              "bottom_20_household_income", "top_20_income", "bottom_20_income"
              )
suffix <- "c35_"


# rename outcomes
cohort_dat <- 
  cohort_dat %>%
  rename_with(~str_c(suffix, .), .cols = all_of(outcomes)) %>% 
  ungroup() 



#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))

#write sample size reduction table to scratch
sample_size <- sample_size %>% mutate(cohort_name = cohort)
write_rds(sample_size, file.path(loc$scratch_folder, "03_sample_size.rds"))


#--------------------------------------------------------

# save table

missings_tab <- missings_tab %>% filter(!is.na(missing))
negatives_tab <- negatives_tab %>% filter(!is.na(negative))
zeros_tab <- zeros_tab %>% filter(!is.na(zero))

write.xlsx(
  list('missings' = missings_tab,
       'negatives' = negatives_tab,
       'zeros' = zeros_tab,
       'not_merged' = not_merged_tab),
  file.path(excel_path, 'child_zeros_negatives_missings.xlsx'), 
  overwrite = T)

