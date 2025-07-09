# Kansenkaart data preparation pipeline
#
# 2. Predictor creation.
#    - Adding parent income and income percentile to the cohort.
#    - Adding migration background information to the cohort.
#    - Writing `scratch/02_predictor.rds`
#
# (c) ODISSEI Social Data Science team 2025



#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(openxlsx)
library(readxl)


#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "01_cohort.rds")) %>%
  mutate(birth_year = year(birthdate))

sample_size <- read_rds(file.path(loc$scratch_folder, "01_sample_size.rds"))


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


# residency requirement for parents between the years parental income and wealth are measured
residency_tab <-
  cohort_dat %>%
  select(RINPERSOONSMa, RINPERSOONMa, RINPERSOONSpa, RINPERSOONpa) %>%
  mutate(start_date = ymd(paste0(cfg$parent_income_year_min, "-01-01")),
         end_date = ymd(paste0(cfg$parent_wealth_year_max, "-12-31")),
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

# record sample size
sample_size <- sample_size %>% mutate(n_3_parent_residency = nrow(cohort_dat))

# select parents
parents_dat <- 
  cohort_dat %>% 
  select(c("RINPERSOONSpa", "RINPERSOONpa", "RINPERSOONSMa", "RINPERSOONMa"))



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

parents_dat <- 
  cohort_dat %>% 
  select(c("RINPERSOONSpa", "RINPERSOONpa", "RINPERSOONSMa", "RINPERSOONMa"))

parents <- c(cohort_dat$RINPERSOONMa, cohort_dat$RINPERSOONSMa, 
             cohort_dat$RINPERSOONpa, cohort_dat$RINPERSOONSpa)

income_parents <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                         income = double(), year = integer())
for (year in seq(as.integer(cfg$parent_income_year_min), as.integer(cfg$parent_income_year_max))) {
  if (year < 2011) {
    # use IPI tab
    income_parents <- 
      # read file from disk
      read_sav(get_ipi_filename(year), col_select = c("RINPERSOONS", "RINPERSOON", 
                                                      "PERSBRUT", "PERSPRIM")) %>% 
      rename(income = PERSBRUT, 
             earnings = PERSPRIM) %>% 
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
      read_sav(get_inpa_filename(year), 
               col_select = c("RINPERSOONS", "RINPERSOON", 
                              "INPPERSBRUT", "INPPERSPRIM")) %>% 
      rename(income = INPPERSBRUT,
             earnings = INPPERSPRIM) %>% 
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
  mutate(income = ifelse(income == 9999999999 | income == 999999999, NA, income), 
         earnings = ifelse(earnings == 9999999999 | earnings == 999999999, NA, earnings)) 


#--------------------------------------------------------------

# number of missings
missings <- missings_function(income_parents, c('income', 'earnings'))
missings_tab <- bind_rows(missings_tab, parents_funct(missings, 'missing'))

# number of negatives
negatives <- negatives_function(income_parents, c('income', 'earnings'))
negatives_tab <- bind_rows(negatives_tab, parents_funct(negatives, 'negative'))

# number of zeros
zeros <- zeros_function(income_parents, c('income', 'earnings'))
zeros_tab <- bind_rows(zeros_tab, parents_funct(zeros, 'zero'))
rm(missings, negatives, zeros)

#--------------------------------------------------------------

# if negative then NA
income_parents <-
  income_parents %>% 
  mutate(income = ifelse(income < 0, NA, income), 
         earnings = ifelse(earnings < 0, NA, earnings))


# create data for parents that have missing incomes for all 5 years
missings <- income_parents %>% 
  group_by(RINPERSOONS, RINPERSOON) %>%
  summarize(missing_income = sum(is.na(income)),
            missing_earnings = sum(is.na(earnings))) %>% ungroup() %>%
  filter(missing_income == 5 | missing_earnings == 5) 


# deflate
income_parents <- 
  income_parents %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(income = income / (cpi / 100), 
         earnings = earnings / (cpi / 100)) %>% 
  select(-cpi)

# compute mean
income_parents <- 
  income_parents %>% 
  group_by(RINPERSOON, RINPERSOONS) %>% 
  summarize(income = mean(income, na.rm = TRUE),
            earnings = mean(earnings, na.rm = TRUE))


# add to data
# mothers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = income_parents %>% rename(income_ma = income, earnings_ma = earnings), 
  by = c("RINPERSOONMa" = "RINPERSOON", "RINPERSOONSMa" = "RINPERSOONS")
) 
# fathers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = income_parents %>% rename(income_pa = income, earnings_pa = earnings), 
  by = c("RINPERSOONpa" = "RINPERSOON", "RINPERSOONSpa" = "RINPERSOONS")
) 

# free up memory
rm(income_parents)


# if income is missing for all 5 years for one parents
# then income is also missing for the other parent
# mothers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = missings %>% rename(missing_earnings_ma = missing_earnings, 
                          missing_income_ma = missing_income), 
  by = c("RINPERSOONMa" = "RINPERSOON", "RINPERSOONSMa" = "RINPERSOONS")
) 
# fathers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = missings %>% rename(missing_earnings_pa = missing_earnings, 
                          missing_income_pa = missing_income), 
  by = c("RINPERSOONpa" = "RINPERSOON", "RINPERSOONSpa" = "RINPERSOONS")
) 


# remove missing parents
cohort_dat <-
  cohort_dat %>%
  mutate(across(c(starts_with("income_") | starts_with("earnings_")), 
                ~ ifelse(is.nan(.), NA, .))) %>%
  mutate(income_ma = ifelse(missing_income_pa == 5 & !is.na(missing_income_pa), NA, income_ma),
         income_pa = ifelse(missing_income_ma == 5 & !is.na(missing_income_ma), NA, income_pa),
         earnings_ma = ifelse(missing_earnings_pa == 5 & !is.na(missing_earnings_pa), NA, earnings_ma),
         earnings_pa = ifelse(missing_earnings_ma == 5 & !is.na(missing_earnings_ma), NA, earnings_pa)) %>%
  select(-c(starts_with('missing_')))


# sum parents income
cohort_dat <- 
  cohort_dat %>% 
  rowwise() %>%
  mutate(income_parents =  sum(income_ma, income_pa, na.rm = TRUE),
         income_parents = ifelse((is.na(income_ma) & is.na(income_pa)), 
                                 NA, income_parents),
         earnings_parents = sum(earnings_ma, earnings_pa, na.rm = TRUE),
         earnings_parents = ifelse((is.na(earnings_ma) & is.na(earnings_pa)), 
                                   NA, earnings_parents))


# not merged
not_merged_tab <- not_merged_func(c('income_parents', 'earnings_parents'))


# remove income if income_parents is NA (parents income is NA for all years)
cohort_dat <- 
  cohort_dat %>%
  filter(!is.na(income_parents))


# compute income transformations
cohort_dat <- 
  cohort_dat %>%
  group_by(birth_year) %>%
  mutate(
    income_parents_rank = rank(income_parents, ties.method = "average", na.last = 'keep'),
    income_parents_perc = income_parents_rank / max(income_parents_rank, na.rm = T),
    
    earnings_parents_rank = rank(earnings_parents, ties.method = "average", na.last = 'keep'),
    earnings_parents_perc = earnings_parents_rank / max(earnings_parents_rank, na.rm = T),
  ) %>%
  select(-c(income_parents_rank, income_ma, income_pa, 
            earnings_parents_rank, earnings_ma, earnings_pa)) %>%
  ungroup()



#### PARENT HOUSEHOLD INCOME ####


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

parents_dat <- cohort_dat %>%
  select(RINPERSOONS, RINPERSOON, RINPERSOONMa, 
         RINPERSOONSMa, RINPERSOONpa, RINPERSOONSpa)

parents <- c(cohort_dat$RINPERSOONMa, cohort_dat$RINPERSOONSMa,
             cohort_dat$RINPERSOONpa, cohort_dat$RINPERSOONSpa)


income_household <- tibble(RINPERSOONS = factor(), RINPERSOON = character(),
                           household_income = double(), primary_household_income = double(),
                           disposable_household_income = double(), year = integer())
for (year in seq(as.integer(cfg$parent_income_year_min), as.integer(cfg$parent_income_year_max))) {
  
  if (year <= 2010) {
    
    # load koppel data person to household
    koppel_hh <- read_sav(get_koppeltabel_filename1(year)) %>%
      # select only incomes of parents
      filter(RINPERSOON %in% parents) %>%
      mutate(RINPERSOONSKERN = as_factor(RINPERSOONSKERN, levels = "value"),
             RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
      rename(RINPERSOONSHKW = RINPERSOONSKERN,
             RINPERSOONHKW = RINPERSOONKERN)
    
  } else {
    
    # load koppel data person to household
    koppel_hh <- read_sav(get_koppeltabel_filename2(year)) %>%
      # select only incomes of parents
      filter(RINPERSOON %in% parents) %>%
      mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"),
             RINPERSOONS = as_factor(RINPERSOONS, levels = "value"))
  }
  
  if (year < 2011) {
    # use tab
    income_parents <-
      # read file from disk
      read_sav(get_ihi_filename(year),
               col_select = c("RINPERSOONSKERN", "RINPERSOONKERN", "BVRBRUTINKH", 
                              "BVRPRIMINKH", "BVRBESTINKH")) %>%
      rename(household_income = BVRBRUTINKH,
             primary_household_income = BVRPRIMINKH,
             disposable_household_income = BVRBESTINKH, 
             RINPERSOONSHKW = RINPERSOONSKERN,
             RINPERSOONHKW = RINPERSOONKERN) %>%
      mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"),
             year = year)
    
  } else {
    # use tab
    income_parents <-
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
  
  income_parents <- inner_join(income_parents, koppel_hh,
                               by = c("RINPERSOONSHKW", "RINPERSOONHKW")) %>%
    select(-c(RINPERSOONSHKW, RINPERSOONHKW))
  
  income_household <- bind_rows(income_household, income_parents) 
  
}
rm(koppel_hh, income_parents)


# remove NA incomes
income_household <-
  income_household %>%
  mutate(
    household_income = ifelse(household_income == 999999999 | household_income == 9999999999, NA, household_income),
    primary_household_income = ifelse(primary_household_income == 999999999 | primary_household_income == 9999999999, NA, primary_household_income),
    disposable_household_income = ifelse(disposable_household_income == 999999999 | disposable_household_income == 9999999999, NA, disposable_household_income))


#--------------------------------------------------------------

# number of missings
missings <- missings_function(income_household, c('household_income', 'primary_household_income', 'disposable_household_income'))
missings_tab <- bind_rows(missings_tab, parents_funct(missings, 'missing'))

# number of negatives
negatives <- negatives_function(income_household, c('household_income', 'primary_household_income', 'disposable_household_income'))
negatives_tab <- bind_rows(negatives_tab, parents_funct(negatives, 'negative'))

# number of zeros
zeros <- zeros_function(income_household, c('household_income', 'primary_household_income', 'disposable_household_income'))
zeros_tab <- bind_rows(zeros_tab, parents_funct(zeros, 'zero'))
rm(missings, negatives, zeros)

#--------------------------------------------------------------

# if negative then NA
income_household <-
  income_household %>%
  mutate(
    household_income = ifelse(household_income < 0, NA, household_income), 
    primary_household_income = ifelse(primary_household_income < 0, NA, primary_household_income),
    disposable_household_income = ifelse(disposable_household_income < 0, NA, disposable_household_income))


# create data for parents that have missing incomes for all 5 years
missings <- 
  income_household %>% 
  group_by(RINPERSOONS, RINPERSOON) %>%
  summarize(
    missing_household_income = sum(is.na(household_income)),
    missing_primary_household_income = sum(is.na(primary_household_income)),
    missing_disposable_household_income = sum(is.na(disposable_household_income))) %>% 
  ungroup() %>%
  filter(missing_household_income == 5 | missing_primary_household_income == 5 |
           missing_disposable_household_income == 5) 


# deflate
income_household <-
  income_household %>%
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>%
  mutate(household_income = household_income / (cpi/100), 
         disposable_household_income = disposable_household_income / (cpi/100), 
         primary_household_income = primary_household_income / (cpi/100)) %>%
  select(-cpi)


# compute mean
income_household <-
  income_household %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  summarize(
    household_income = mean(household_income, na.rm = TRUE),
    disposable_household_income = mean(disposable_household_income, na.rm = TRUE),
    primary_household_income = mean(primary_household_income, na.rm = TRUE))


# add to data
# mothers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = income_household %>% rename(household_income_ma = household_income, 
                                  disposable_household_income_ma = disposable_household_income,
                                  primary_household_income_ma = primary_household_income), 
  by = c("RINPERSOONMa" = "RINPERSOON", "RINPERSOONSMa" = "RINPERSOONS")
) 
# fathers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = income_household %>% rename(household_income_pa = household_income, 
                                  disposable_household_income_pa = disposable_household_income,
                                  primary_household_income_pa = primary_household_income), 
  by = c("RINPERSOONpa" = "RINPERSOON", "RINPERSOONSpa" = "RINPERSOONS")
) 

# free up memory
rm(income_household)



# if income is missing for all 5 years for one parents
# then income is also missing for the other parent
# mothers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = missings %>% 
    rename(missing_household_income_ma = missing_household_income, 
           missing_disposable_household_income_ma = missing_disposable_household_income,
           missing_primary_household_income_ma = missing_primary_household_income), 
  by = c("RINPERSOONMa" = "RINPERSOON", "RINPERSOONSMa" = "RINPERSOONS")
) 
# fathers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = missings %>% 
    rename(missing_household_income_pa = missing_household_income, 
           missing_disposable_household_income_pa = missing_disposable_household_income,
           missing_primary_household_income_pa = missing_primary_household_income), 
  by = c("RINPERSOONpa" = "RINPERSOON", "RINPERSOONSpa" = "RINPERSOONS")
)

# remove missing parents
cohort_dat <-
  cohort_dat %>%
  mutate(across(c(starts_with("household_") | starts_with("disposable_") |
                    starts_with("primary_")), ~ ifelse(is.nan(.), NA, .))) %>%
  mutate(household_income_ma = ifelse(missing_household_income_pa == 5 & !is.na(missing_household_income_pa), NA, household_income_ma),
         household_income_pa = ifelse(missing_household_income_ma == 5 & !is.na(missing_household_income_ma), NA, household_income_pa),
         disposable_household_income_ma = ifelse(missing_disposable_household_income_pa == 5 & !is.na(missing_disposable_household_income_pa), NA, disposable_household_income_ma),
         disposable_household_income_pa = ifelse(missing_disposable_household_income_ma == 5 & !is.na(missing_disposable_household_income_ma), NA, disposable_household_income_pa),
         primary_household_income_ma = ifelse(missing_primary_household_income_pa == 5 & !is.na(missing_primary_household_income_pa), NA, primary_household_income_ma),
         primary_household_income_pa = ifelse(missing_primary_household_income_ma == 5 & !is.na(missing_primary_household_income_ma), NA, primary_household_income_pa)) %>%
  select(-c(starts_with('missing_')))


# how many parents do not live in the same household?
diff_hh <-
  cohort_dat %>%
  filter(household_income_ma != household_income_pa) %>%
  summarize(n = n()) %>%
  mutate(parents = "household income",
         household = 'different household')

same_hh <-
  cohort_dat %>%
  filter(household_income_ma == household_income_pa) %>%
  summarize(n = n()) %>%
  mutate(parents = "household income",
         household = 'same household')

parents_diff_hh <- bind_rows(parents_diff_hh, diff_hh)
parents_diff_hh <- bind_rows(parents_diff_hh, same_hh)


# average household income of the parents
cohort_dat <-
  cohort_dat %>%
  rowwise() %>%
  mutate(
    household_income_parents = ifelse(
      household_income_ma == household_income_pa, 
      mean(c(household_income_ma, household_income_pa), na.rm = T),
      sum(c(household_income_ma, household_income_pa), na.rm=T)),
    household_income_parents = ifelse(is.na(household_income_parents), household_income_pa, household_income_parents),
    household_income_parents = ifelse(is.na(household_income_parents), household_income_ma, household_income_parents),
    
    disposable_household_income_parents = ifelse(
      disposable_household_income_ma == disposable_household_income_pa, 
      mean(c(disposable_household_income_ma, disposable_household_income_pa), na.rm = T),
      sum(c(disposable_household_income_ma, disposable_household_income_pa), na.rm=T)),
    disposable_household_income_parents = ifelse(is.na(disposable_household_income_parents), disposable_household_income_pa, disposable_household_income_parents),
    disposable_household_income_parents = ifelse(is.na(disposable_household_income_parents), disposable_household_income_ma, disposable_household_income_parents),
    
    primary_household_income_parents = ifelse(
      primary_household_income_ma == primary_household_income_pa, 
      mean(c(primary_household_income_ma, primary_household_income_pa), na.rm = T),
      sum(c(primary_household_income_ma, primary_household_income_pa), na.rm=T)),
    primary_household_income_parents = ifelse(is.na(primary_household_income_parents), primary_household_income_pa, primary_household_income_parents),
    primary_household_income_parents = ifelse(is.na(primary_household_income_parents), primary_household_income_ma, primary_household_income_parents)) %>%
  select(-c(disposable_household_income_ma, disposable_household_income_pa, 
            household_income_ma, household_income_pa, 
            primary_household_income_pa, primary_household_income_ma))


# not merged
not_merged_tab <- 
  not_merged_func(c('household_income_parents', 'disposable_household_income_parents', 
                    'primary_household_income_parents'))


# compute income transformations
cohort_dat <- 
  cohort_dat %>%
  group_by(birth_year) %>%
  mutate(
    household_income_parents_rank = rank(household_income_parents, ties.method = "average", na.last = 'keep'),
    household_income_parents_perc = household_income_parents_rank / max(household_income_parents_rank, na.rm = T),
    
    disposable_household_income_parents_rank = rank(disposable_household_income_parents, ties.method = "average", na.last = 'keep'),
    disposable_household_income_parents_perc = disposable_household_income_parents_rank / 
      max(disposable_household_income_parents_rank, na.rm = T),
    
    primary_household_income_parents_rank = rank(primary_household_income_parents, ties.method = "average", na.last = 'keep'),
    primary_household_income_parents_perc = primary_household_income_parents_rank / max(primary_household_income_parents_rank, na.rm = T)
  ) %>%
  select(-c(household_income_parents_rank, disposable_household_income_parents_rank, 
            primary_household_income_parents_rank)) %>%
  ungroup()


#### PARENT WEALTH ####


# get wealth data from each requested year into single data frame
get_veh_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/VEHTAB"),
    pattern = paste0("VEH", year, "TAB"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


wealth_parents <- data.frame()
for (year in seq(as.integer(cfg$parent_wealth_year_min), as.integer(cfg$parent_wealth_year_max))) {
  
  if (year <= 2010) {
    
    # load koppel data person to household
    koppel_hh <- read_sav(get_koppeltabel_filename1(year)) %>%
      # select only incomes of parents
      filter(RINPERSOON %in% parents) %>%
      mutate(RINPERSOONSKERN = as_factor(RINPERSOONSKERN, levels = "value"),
             RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
      rename(RINPERSOONSHKW = RINPERSOONSKERN,
             RINPERSOONHKW = RINPERSOONKERN)
    
  } else {
    
    # load koppel data person to household
    koppel_hh <- read_sav(get_koppeltabel_filename2(year)) %>%
      # select only incomes of parents
      filter(RINPERSOON %in% parents) %>% 
      mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"), 
             RINPERSOONS = as_factor(RINPERSOONS, levels = "value"))
  }
  
  # use VEH tab
  veh_parents <-
    # read file from disk
    read_sav(get_veh_filename(year), col_select = c("RINPERSOONSHKW", "RINPERSOONHKW",
                                                    "VEHW1000VERH")) %>%
    rename(wealth = VEHW1000VERH) %>%
    mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"), 
           year = year) %>%
    inner_join(koppel_hh, by = c("RINPERSOONSHKW", "RINPERSOONHKW")
    ) %>%
    # remove NA incomes
    mutate(wealth = ifelse(wealth == 99999999999, NA, wealth)) %>%
    left_join(cpi_tab %>% select(year, cpi), by = "year") %>%
    mutate(wealth = wealth / (cpi / 100)) %>%
    select(-c(cpi, year))
  
  
  # add to data
  # mothers
  wealth_dat <- left_join(parents_dat, veh_parents,
                          by = c("RINPERSOONSMa" = "RINPERSOONS",
                                 "RINPERSOONMa" = "RINPERSOON")
  ) %>%
    rename(wealth_ma = wealth, 
           RINPERSOONSHKW_ma = RINPERSOONSHKW,
           RINPERSOONHKW_ma = RINPERSOONHKW)
  
  # fathers
  wealth_dat <- left_join(wealth_dat, veh_parents,
                          by = c("RINPERSOONSpa" = "RINPERSOONS",
                                 "RINPERSOONpa" = "RINPERSOON")
  ) %>%
    rename(wealth_pa = wealth, 
           RINPERSOONSHKW_pa = RINPERSOONSHKW,
           RINPERSOONHKW_pa = RINPERSOONHKW)
  
  # add to data if mother and father are not in the same household
  
  
  # if RINPERSOONHKW is the same for mother and father, keep the same wealth
  # if RINPERSOONHKW is different for mother and father, sum wealth_ma and wealth_pa
  wealth_dat <- 
    wealth_dat %>%
    rowwise() %>%
    mutate(wealth_parents = ifelse(RINPERSOONHKW_ma == RINPERSOONHKW_pa, wealth_pa, 
                                   sum(c(wealth_ma, wealth_pa), na.rm = TRUE)), 
           wealth_parents = ifelse(is.na(wealth_pa), wealth_ma, wealth_parents), 
           wealth_parents = ifelse(is.na(wealth_ma), wealth_pa, wealth_parents),
           diff_household = ifelse(RINPERSOONHKW_ma != RINPERSOONHKW_pa, 1, 0),
           diff_household = ifelse(is.na(diff_household), 0, diff_household),
    ) %>%
    # add year
    mutate(year = year) %>%
    select(RINPERSOONS, RINPERSOON, wealth_parents, year, diff_household)
  
  wealth_parents <- bind_rows(wealth_parents, wealth_dat)
}
rm(veh_parents, koppel_hh, wealth_dat, parents_dat)


# add year of child specific age at which we measure parental wealth
wealth_age <- abs(min(cohort_dat$birth_year) - 
                    as.integer(cfg$parent_wealth_year_min))
cohort_dat <- 
  cohort_dat %>%
  mutate(wealth_age = birth_year + wealth_age)


cohort_dat <- 
  cohort_dat %>%
  left_join(wealth_parents, by = c("RINPERSOONS" = "RINPERSOONS", 
                                   "RINPERSOON" = "RINPERSOON", 
                                   "wealth_age"  = "year")) %>% select(-wealth_age) 

# not merged
not_merged_tab <- 
  not_merged_func(c('wealth_parents'))

not_merged_tab <-
  not_merged_tab %>%
  mutate(outcome = recode(outcome, 
                          'wealth_parents' = 'wealth_parents 2006-2010'))



# for those who are not matched to a wealth, take year after 
# add year of child specific age at which we measure parental wealth
cohort_dat <- 
  cohort_dat %>%
  mutate(wealth_age = birth_year + wealth_age + 1)


cohort_dat <- 
  cohort_dat %>%
  left_join(wealth_parents, by = c("RINPERSOONS" = "RINPERSOONS", 
                                   "RINPERSOON" = "RINPERSOON", 
                                   "wealth_age"  = "year"), 
            suffix = c('', '_after')) %>%
  select(-wealth_age)


# number of NA (not merged)
tmp_dat <- cohort_dat %>% filter(is.na(wealth_parents))

tmp <- 
  tibble(
    outcome = 'wealth_parents 2007-2011', 
    not_merged = sum(is.na(tmp_dat$wealth_parents_after)),
    merged = sum(!is.na(tmp_dat$wealth_parents_after)))

not_merged_tab <- bind_rows(not_merged_tab, tmp)
rm(tmp, tmp_dat)


# if wealth parents not available, use wealth parents of the next year
cohort_dat <-
  cohort_dat %>%
  mutate(wealth_parents = ifelse(is.na(wealth_parents), 
                                 wealth_parents_after, wealth_parents), 
         diff_household = ifelse(is.na(diff_household),
                                 diff_household_after, diff_household)) %>%
  select(-wealth_parents_after, -diff_household_after)


# compute income transformations
cohort_dat <- 
  cohort_dat %>% 
  group_by(birth_year) %>%
  mutate(
    wealth_parents_rank = rank(wealth_parents, ties.method = "average", na.last = 'keep'),
    wealth_parents_perc = wealth_parents_rank / max(wealth_parents_rank, na.rm = T)
  ) %>%
  select(-wealth_parents_rank) %>%
  ungroup()

rm(wealth_parents)

# how many parents do not live in the same household?
diff_hh <-
  cohort_dat %>%
  filter(diff_household == 1) %>%
  summarize(n = n()) %>%
  mutate(parents = "wealth parents",
         household = 'diffent household')

same_hh <-
  cohort_dat %>%
  filter(diff_household == 0) %>%
  summarize(n = n()) %>%
  mutate(parents = "wealth parents",
         household = 'same household')

parents_diff_hh <- bind_rows(parents_diff_hh, diff_hh)
parents_diff_hh <- bind_rows(parents_diff_hh, same_hh)
rm(diff_hh)

cohort_dat <- cohort_dat %>% select(-diff_household)





#### THIRD GENERATION ####

# import gba for parents generation and origin
gba_path <- file.path(loc$data_folder, loc$gba_data)
gba_dat <-  
  read_sav(gba_path, col_select = c("RINPERSOONS", "RINPERSOON",
                                    "GBAGENERATIE", "GBAHERKOMSTGROEPERING")) %>% 
  mutate(RINPERSOONS  = as_factor(RINPERSOONS,  levels = "values"),
         GBAGENERATIE = as_factor(GBAGENERATIE, levels = "label"),
         GBAHERKOMSTGROEPERING = as_factor(GBAHERKOMSTGROEPERING, levels = "labels"))

# add parents generation to cohort
cohort_dat <- cohort_dat %>% 
  left_join(gba_dat,
            by = c("RINPERSOONpa" = "RINPERSOON", "RINPERSOONSpa" = "RINPERSOONS"), 
            suffix = c("", "_pa")) %>%
  left_join(gba_dat,
            by = c("RINPERSOONMa" = "RINPERSOON", "RINPERSOONSMa" = "RINPERSOONS"),
            suffix = c("", "_ma"))



# replace autochtoon to third generation if one of the parents were second generation  (2) 
# and the child is native (0)
cohort_dat <- 
  cohort_dat %>%
  mutate(across(c("GBAGENERATIE", "GBAGENERATIE_pa", "GBAGENERATIE_ma"), as.character)) %>% 
  mutate(
    generation = as.character(GBAGENERATIE),
    generation = ifelse(
      GBAGENERATIE == "autochtoon" & GBAGENERATIE_ma == "tweede generatie allochtoon" & !is.na(GBAGENERATIE_ma), 
      "derde generatie allochtoon", 
      generation
    ),
    generation = ifelse(
      GBAGENERATIE == "autochtoon" & GBAGENERATIE_pa == "tweede generatie allochtoon" & !is.na(GBAGENERATIE_pa), 
      "derde generatie allochtoon", 
      generation
    )
  )

# replace gbaherkomstgroepering if the child is third generation
cohort_dat <- 
  cohort_dat %>%
  mutate(
    across(c(GBAHERKOMSTGROEPERING, GBAHERKOMSTGROEPERING_pa, GBAHERKOMSTGROEPERING_ma), 
           as.character)
  ) %>%
  mutate(
    # third generation child gets mom's origin
    origin_group = ifelse(
      generation == "derde generatie allochtoon" & !is.na(GBAHERKOMSTGROEPERING_ma),
      GBAHERKOMSTGROEPERING_ma, 
      GBAHERKOMSTGROEPERING
    ),
    # except when mom is not second generation, then dad's origin
    origin_group = ifelse(
      generation == "derde generatie allochtoon" & (GBAGENERATIE_ma != "tweede generatie allochtoon" | is.na(GBAGENERATIE_ma)),
      GBAHERKOMSTGROEPERING_pa, 
      origin_group
    )
  )

#### MIGRATION BACKGROUND ####


# create migration variable: origin with third generation
cohort_dat <- 
  cohort_dat %>%
  mutate(
    migration_background = as.character(origin_group), 
    migration_background = 
      ifelse(!(migration_background %in% 
                 c("Nederland", "Turkije", "Marokko", "Suriname", 
                   'Aruba', 'Bonaire', 'Curaçao', 'Saba', 'Sint Eustatius', 
                   'Sint Maarten', 'Nederlandse Antillen (oud)')), 
             "Other", migration_background)) %>%
  # create migration antilles
  mutate(migration_background = ifelse(migration_background %in% 
                                         c('Aruba', 'Bonaire', 'Curaçao', 'Saba', 'Sint Eustatius',
                                           'Nederlandse Antillen (oud)', 'Sint Maarten'), 'Dutch Caribbean',
                                       migration_background),
         migration_background = as.factor(migration_background)) %>%
  # recode migration background
  mutate(migration_background = recode(migration_background, 
                                       'Nederland' = 'No Migration Background', 
                                       'Turkije' = 'Turkey', 
                                       'Marokko' = 'Morocco'))



# remove unnecessary outcomes
cohort_dat <- 
  cohort_dat %>%
  select(-c(GBAHERKOMSTGROEPERING_pa, GBAGENERATIE_pa, 
            GBAHERKOMSTGROEPERING_ma, GBAGENERATIE_ma, GBAHERKOMSTGROEPERING, 
            GBAGENERATIE))


# create variable for individuals with a migration background
cohort_dat <-
  cohort_dat %>%
  mutate(has_migration = ifelse(migration_background == "No Migration Background", 0, 1))


# free up memory
rm(gba_dat)


#### TYPE HOUSEHOLD ####


# import household  data
household_dat <-
  read_sav(file.path(loc$data_folder, loc$household_data),
           col_select = c("RINPERSOONS", "RINPERSOON", "DATUMAANVANGHH",
                          "DATUMEINDEHH", "TYPHH")) %>%
  filter(RINPERSOON %in% cohort_dat$RINPERSOON)


household_dat <- 
  household_dat %>%
  mutate(
    RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
    DATUMAANVANGHH = as.numeric(DATUMAANVANGHH),
    DATUMEINDEHH = as.numeric(DATUMEINDEHH),
    TYPHH = as_factor(TYPHH, levels = "value")
  ) %>%
  # mutate_all(na_if, "") %>%
  mutate(
    DATUMAANVANGHH = ymd(DATUMAANVANGHH),
    DATUMEINDEHH = ymd(DATUMEINDEHH)
  ) 


# take date at which the child is a specific age
age_tab <- cohort_dat %>%
  select(RINPERSOONS, RINPERSOON, birthdate) %>%
  mutate(home_address_date = birthdate %m+% years(cfg$childhood_home_age)) %>%
  select(-birthdate)


hh_tab <- 
  household_dat %>%
  left_join(age_tab, by = c("RINPERSOONS", "RINPERSOON")) %>%
  # take addresses that are still open on a specific date
  filter(
    DATUMAANVANGHH <= home_address_date & 
      DATUMEINDEHH >= home_address_date
  ) %>%
  group_by(RINPERSOONS, RINPERSOON) %>% 
  summarize(TYPHH = TYPHH[1])  

rm(age_tab)


# create type household variable
hh_tab <- 
  hh_tab %>%
  mutate(type_household = ifelse(TYPHH %in% "6", "Single Parent", "Other"),
         type_household = ifelse(TYPHH %in% c("2", "3", "4", "5"), "Two Parents", type_household)) %>%
  select(-TYPHH)

cohort_dat <- 
  cohort_dat %>%
  left_join(hh_tab, by = c("RINPERSOONS", "RINPERSOON"))



# if child cannot be merged to type_household (NA) then remove from the sample
cohort_dat <- 
  cohort_dat %>%
  filter(!is.na(type_household)) %>%
  mutate(type_household = as.factor(type_household))



rm(household_dat, hh_tab)

# recording sample size
sample_size <- sample_size %>% mutate(n_4_parent_characteristics = nrow(cohort_dat))


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "02_predictors.rds"))

write_rds(sample_size, file.path(loc$scratch_folder, "02_sample_size.rds"))


#--------------------------------------------------------

# save table

missings_tab <- missings_tab %>% filter(!is.na(missing))
negatives_tab <- negatives_tab %>% filter(!is.na(negative))
zeros_tab <- zeros_tab %>% filter(!is.na(zero))

write.xlsx(
  list('missings' = missings_tab,
       'negatives' = negatives_tab,
       'zeros' = zeros_tab,
       'not_merged' = not_merged_tab,
       'different_hh' = parents_diff_hh),
  file.path(excel_path, 'parents_zeros_negatives_missings.xlsx'), 
  overwrite = T)


