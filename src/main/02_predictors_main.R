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
  mutate(birth_year = year(birthdate))

sample_size <- read_rds(file.path(loc$scratch_folder, "01_sample_size.rds"))
  

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
for (year in seq(as.integer(cfg$parent_income_year_min), as.integer(cfg$parent_income_year_max))) {
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
      read_sav(get_inpa_filename(year), 
               col_select = c("RINPERSOONS", "RINPERSOON", "INPPERSBRUT")) %>% 
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


# compute mean
income_parents <- 
  income_parents %>% 
  group_by(RINPERSOON, RINPERSOONS) %>% 
  summarize(income_n = sum(!is.na(income)),
            income   = mean(income, na.rm = TRUE)) 

# table of the number of years the mean income is based on
print(table(`income years` = income_parents$income_n))

# add to data
# mothers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = income_parents %>% rename(income_ma = income, income_n_ma = income_n), 
  by = c("RINPERSOONMa" = "RINPERSOON", "RINPERSOONSMa" = "RINPERSOONS")
) %>%
  select(-income_n_ma)
# fathers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = income_parents %>% rename(income_pa = income, income_n_pa = income_n), 
  by = c("RINPERSOONpa" = "RINPERSOON", "RINPERSOONSpa" = "RINPERSOONS")
) %>%
  select(-income_n_pa)

# free up memory
rm(income_parents)

# parents
cohort_dat <- cohort_dat %>% 
  rowwise() %>%
  mutate(income_parents =  sum(income_ma, income_pa, na.rm = TRUE),
         income_parents = ifelse((is.na(income_ma) & is.na(income_pa)), 
                                 NA, income_parents))


# if income_parents is negative then income_parents becomes NA
cohort_dat <- 
  cohort_dat %>%
  mutate(income_parents = ifelse(income_parents < 0, NA, income_parents)) %>%
  # remove income if income_parents is NA (parents income is NA for all years)
  filter(!is.na(income_parents))


# compute income transformations
cohort_dat <- 
  cohort_dat %>%
  group_by(birth_year) %>%
  mutate(
    income_parents_rank = rank(income_parents, ties.method = "average"),
    income_parents_perc = income_parents_rank / max(income_parents_rank)
  ) %>%
  select(-c(income_parents_rank, income_ma, income_pa)) %>%
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


get_koppelpersoon_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INHATAB"),
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


parents_dat <- cohort_dat %>%
  select(RINPERSOONS, RINPERSOON, RINPERSOONMa, 
         RINPERSOONSMa, RINPERSOONpa, RINPERSOONSpa)

parents <- c(cohort_dat$RINPERSOONMa, cohort_dat$RINPERSOONSMa,
             cohort_dat$RINPERSOONpa, cohort_dat$RINPERSOONSpa)

wealth_parents <- tibble(RINPERSOONS = factor(), RINPERSOON = character(),
                         wealth_parents = double(), year = integer())
for (year in seq(as.integer(cfg$parent_wealth_year_min), as.integer(cfg$parent_wealth_year_max))) {
  
  if (year <= 2010) {
    
    # load koppel data person to household
    koppel_hh <- read_sav(get_koppeltabel_filename(year)) %>%
      # select only incomes of parents
      filter(RINPERSOON %in% parents) %>% 
      mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"), 
             RINPERSOONS = as_factor(RINPERSOONS, levels = "value"))
    
  } else {
    
    # load koppel data person to household
    koppel_hh <- read_sav(get_koppelpersoon_filename(year)) %>%
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
  
  # if RINPERSOONHKW is the same for mother and father, keep the same wealth
  # if RINPERSOONHKW is different for mother and father, sum wealth_ma and wealth_pa
  wealth_dat <- 
    wealth_dat %>%
    rowwise() %>%
    mutate(wealth_parents = ifelse(RINPERSOONHKW_ma == RINPERSOONHKW_pa, wealth_pa, 
                                   sum(c(wealth_ma, wealth_pa), na.rm = TRUE)), 
           wealth_parents = ifelse(is.na(wealth_pa), wealth_ma, wealth_parents), 
           wealth_parents = ifelse(is.na(wealth_ma), wealth_pa, wealth_parents)
    ) %>%
    # add year
    mutate(year = year) %>%
    select(RINPERSOONS, RINPERSOON, wealth_parents, year)
  
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
                                   "wealth_age"  = "year")) %>%
  select(-wealth_age)



# if wealth_parents is negative then wealth_parents becomes NA
cohort_dat <- 
  cohort_dat %>%
  # remove income if wealth_parents is NA (parents income is NA for all years)
  filter(!is.na(wealth_parents))


# compute income transformations
cohort_dat <- 
  cohort_dat %>% 
  group_by(birth_year) %>%
  mutate(
    wealth_parents_rank = rank(wealth_parents, ties.method = "average"),
    wealth_parents_perc = wealth_parents_rank / max(wealth_parents_rank)
  ) %>%
  select(-wealth_parents_rank) %>%
  ungroup()

rm(wealth_parents)



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
    GBAGENERATIE_third = as.character(GBAGENERATIE),
    GBAGENERATIE_third = ifelse(
      GBAGENERATIE == "autochtoon" & GBAGENERATIE_ma == "tweede generatie allochtoon" & !is.na(GBAGENERATIE_ma), 
      "derde generatie allochtoon", 
      GBAGENERATIE_third
    ),
    GBAGENERATIE_third = ifelse(
      GBAGENERATIE == "autochtoon" & GBAGENERATIE_pa == "tweede generatie allochtoon" & !is.na(GBAGENERATIE_pa), 
      "derde generatie allochtoon", 
      GBAGENERATIE_third
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
    GBAHERKOMSTGROEPERING_third = ifelse(
      GBAGENERATIE_third == "derde generatie allochtoon" & !is.na(GBAHERKOMSTGROEPERING_ma),
      GBAHERKOMSTGROEPERING_ma, 
      GBAHERKOMSTGROEPERING
    ),
    # except when mom is not second generation, then dad's origin
    GBAHERKOMSTGROEPERING_third = ifelse(
      GBAGENERATIE_third == "derde generatie allochtoon" & (GBAGENERATIE_ma != "tweede generatie allochtoon" | is.na(GBAGENERATIE_ma)),
      GBAHERKOMSTGROEPERING_pa, 
      GBAHERKOMSTGROEPERING_third
    )
  )

#### MIGRATION BACKGROUND ####


# create migration variable: origin with third generation
cohort_dat <- 
  cohort_dat %>%
  mutate(
    migration_third = as.character(GBAHERKOMSTGROEPERING_third), 
    migration_third = ifelse(!(migration_third %in% c("Nederland", "Turkije", "Marokko", "Suriname",
                                                     "Nederlandse Antillen (oud)")), 
                             "Overig", migration_third)) %>%
  mutate(migration_third = as.factor(migration_third))


# remove unnecessary outcomes
cohort_dat <- 
  cohort_dat %>%
  select(-c(GBAHERKOMSTGROEPERING_pa, GBAGENERATIE_pa, 
            GBAHERKOMSTGROEPERING_ma, GBAGENERATIE_ma, GBAHERKOMSTGROEPERING, 
            GBAGENERATIE, GBAGENERATIE_third))


# create variable for individuals with a migration background
cohort_dat <-
  cohort_dat %>%
  mutate(has_migration = ifelse(migration_third == "Nederland", 0, 1))


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
   mutate(type_hh = ifelse(TYPHH %in% "6", "single parent", "other"),
          type_hh = ifelse(TYPHH %in% c("2", "3", "4", "5"), "two parents", type_hh)) %>%
   select(-TYPHH)
 
 cohort_dat <- 
   cohort_dat %>%
   left_join(hh_tab, by = c("RINPERSOONS", "RINPERSOON"))
 


# if child cannot be merged to type_hh (NA) then remove from the sample
cohort_dat <- 
  cohort_dat %>%
  filter(!is.na(type_hh)) %>%
  mutate(type_hh = as.factor(type_hh))
  
 

rm(household_dat, hh_tab)

# recording sample size
sample_size <- sample_size %>% mutate(n_4_parent_characteristics = nrow(cohort_dat))

 
#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "02_predictors.rds"))
 
write_rds(sample_size, file.path(loc$scratch_folder, "02_sample_size.rds"))

