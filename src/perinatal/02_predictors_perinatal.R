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


#### HOUSEHOLD INCOME ####


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


# LINK MOTHER RINPERSOON TO HOUSEHOLD RINPERSOON

mothers <- c(cohort_dat$CBKSoortNr_Moeder, cohort_dat$Rinpersoon_Moeder)
mothers_rinpersoon <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                             RINPERSOONSHKW = factor(), RINPERSOONHKW = character(),
                             year = double())
for (year in seq(as.integer(cfg$parent_income_year_min), as.integer(cfg$parent_income_year_max))) {
  if (year < 2011) {
    # use IPI tab
    mothers_rinpersoon <- 
      # read file from disk
      read_sav(get_ipi_filename(year), col_select = c("RINPERSOONS", "RINPERSOON",
                                                      "RINPERSOONSKERN", "RINPERSOONKERN")) %>% 
      rename(RINPERSOONSHKW = RINPERSOONSKERN,
             RINPERSOONHKW = RINPERSOONKERN) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
             RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"),
             year = year) %>%
      # select only incomes of mothers
      filter(RINPERSOON %in% mothers) %>% 
      # add to mothers rinpersoon
      bind_rows(mothers_rinpersoon, .)
  } else {
    # use INPA tab
    mothers_rinpersoon <- 
      # read file from disk
      read_sav(get_inpa_filename(year), col_select = c("RINPERSOONS", "RINPERSOON", 
                                                       "RINPERSOONSHKW", "RINPERSOONHKW")) %>% 
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
             RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"),
             year = year) %>%
      # select only mothers
      filter(RINPERSOON %in% mothers) %>% 
      # add to mothers
      bind_rows(mothers_rinpersoon, .)
  }
}



#### HOUSEHOLD INCOME ####
# get income data from each requested year into single data frame
get_ihi_filename <- function(year) {
  # function to get latest ihi version of specified year
  # get all ihi files with the specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INTEGRAAL HUISHOUDENS INKOMEN", year),
    pattern = paste0(year, "TABV[0-9]+(?i)(.sav)"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

get_inha_filename <- function(year) {
  # function to get latest inha version of specified year
  # get all inha files with the specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INHATAB"),
    pattern = paste0(year, "TABV[0-9]+(?i)(.sav)"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


income_household <- tibble(RINPERSOONSHKW = factor(), RINPERSOONHKW = character(), 
                           household_income = double(), year = integer())
for (year in seq(as.integer(cfg$parent_income_year_min), as.integer(cfg$parent_income_year_max))) {
  if (year < 2011) {
    # use IHI tab
    income_household <- 
      # read file from disk
      read_sav(get_ihi_filename(year), col_select = c("RINPERSOONSKERN", "RINPERSOONKERN", 
                                                      "BVRBRUTINKH")) %>% 
      rename(household_income = BVRBRUTINKH,
             RINPERSOONSHKW = RINPERSOONSKERN,
             RINPERSOONHKW = RINPERSOONKERN) %>% 
      mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"),
             year = year) %>%
      # add to income household
      bind_rows(income_household, .)
  } else {
    # use INHA tab
    income_household <- 
      # read file from disk
      read_sav(get_inha_filename(year), col_select = c("RINPERSOONSHKW", "RINPERSOONHKW", 
                                                       "INHBRUTINKH")) %>% 
      rename(household_income = INHBRUTINKH) %>% 
      mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"),
             year = year) %>%
      # add to income household
      bind_rows(income_household, .)
  }
}


# remove negative and NA incomes
income_household <-
  income_household %>% 
  mutate(household_income = ifelse(household_income == 999999999 | 
                                     household_income == 9999999999, NA,
                                   household_income)) 

# deflate
income_household <- 
  income_household %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(household_income = household_income / (cpi / 100)) %>% 
  select(-cpi)


income_household <- 
  income_household %>%
  inner_join(mothers_rinpersoon, by = c("RINPERSOONSHKW", "RINPERSOONHKW", "year"))

# free up memory
rm(mothers_rinpersoon)


# compute mean
income_household <- 
  income_household %>% 
  group_by(RINPERSOON, RINPERSOONS) %>% 
  summarize(household_income_n = n(),
            household_income   = mean(household_income, na.rm = TRUE)) %>%
  filter(!is.na(household_income))

# table of the number of years the mean income is based on
print(table(`income years` = income_household$household_income_n))


cohort_dat <- 
  cohort_dat %>%
  inner_join(income_household, 
             by = c("CBKSoortNr_Moeder" = "RINPERSOONS",
                    "Rinpersoon_Moeder" = "RINPERSOON"))

rm(income_household)


# if household_income is negative then household_income is NA
cohort_dat <- 
  cohort_dat %>%
  mutate(household_income = ifelse(household_income < 0, NA, household_income)) %>%
  # remove income if household_income is NA 
  filter(!is.na(household_income))


# compute income transformations
cohort_dat <- 
  cohort_dat %>% 
  group_by(birth_year) %>%
  mutate(
    income_household_rank = rank(household_income, ties.method = "average"),
    income_household_perc = income_household_rank / max(income_household_rank)
  ) %>%
  select(-c(income_household_rank, household_income_n)) %>%
  ungroup()


# remove parents income and replace with household income
cohort_dat <- 
  cohort_dat %>%
  rename( 
         income_parents      = household_income,
         income_parents_perc = income_household_perc)



#### MOTHERS WEALTH ####


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

parents_dat <-
  cohort_dat %>%
  select(RINPERSOONS, RINPERSOON, Rinpersoon_Moeder, CBKSoortNr_Moeder)

parents <- c(cohort_dat$Rinpersoon_Moeder, cohort_dat$CBKSoortNr_Moeder)

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
  wealth_dat <- parents_dat %>%
    left_join(veh_parents, 
              by = c("CBKSoortNr_Moeder" = "RINPERSOONS",
                     "Rinpersoon_Moeder" = "RINPERSOON")
              ) %>%
    # add year
    mutate(year = year) %>% 
    rename('wealth_parents' = 'wealth') %>%
    select(RINPERSOONS, RINPERSOON, wealth_parents, year)
  
  
  wealth_parents <- bind_rows(wealth_parents, wealth_dat)
}
rm(veh_parents, koppel_hh, wealth_dat, parents_dat)


# add year of child specific age at which we measure parental wealth
wealth_age <- abs(min(cohort_dat$birth_year) - 
                    as.integer(cfg$parent_wealth_year_min))

cohort_dat <- 
  cohort_dat %>%
  mutate(wealth_age = as.numeric(birth_year + wealth_age))

cohort_dat <- 
  cohort_dat %>%
  left_join(wealth_parents, by = c("RINPERSOONS" = "RINPERSOONS",
                                   "RINPERSOON" = "RINPERSOON",
                                   "wealth_age"  = "year")) %>%
  select(-wealth_age)

rm(wealth_parents)


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



#### THIRD GENERATION ####

# import gba for mothers generation and origin
gba_path <- file.path(loc$data_folder, loc$gba_data)
gba_dat <-  
  read_sav(gba_path, col_select = c("RINPERSOONS", "RINPERSOON",
                                    "GBAGENERATIE", "GBAHERKOMSTGROEPERING")) %>% 
  mutate(RINPERSOONS  = as_factor(RINPERSOONS,  levels = "values"),
         GBAGENERATIE = as_factor(GBAGENERATIE, levels = "label"),
         GBAHERKOMSTGROEPERING = as_factor(GBAHERKOMSTGROEPERING, levels = "labels"))

# add mothers generation to cohort
cohort_dat <- 
  cohort_dat %>% 
  left_join(gba_dat,
            by = c("Rinpersoon_Moeder" = "RINPERSOON", "CBKSoortNr_Moeder" = "RINPERSOONS"),
            suffix = c("", "_ma"))


# replace autochtoon to third generation if the mother is second generation  (2) 
# and the child is native (0)
cohort_dat <- 
  cohort_dat %>%
  mutate(across(c("GBAGENERATIE", "GBAGENERATIE_ma"), as.character)) %>% 
  mutate(
    GBAGENERATIE_third = as.character(GBAGENERATIE),
    GBAGENERATIE_third = ifelse(
      GBAGENERATIE == "autochtoon" & GBAGENERATIE_ma == "tweede generatie allochtoon" & !is.na(GBAGENERATIE_ma), 
      "derde generatie allochtoon", 
      GBAGENERATIE_third
    )
  )

# replace gbaherkomstgroepering if the child is third generation
cohort_dat <- 
  cohort_dat %>%
  mutate(
    across(c(GBAHERKOMSTGROEPERING, GBAHERKOMSTGROEPERING_ma), 
           as.character)
  ) %>%
  mutate(
    # third generation child gets mom's origin
    GBAHERKOMSTGROEPERING_third = ifelse(
      GBAGENERATIE_third == "derde generatie allochtoon" & !is.na(GBAHERKOMSTGROEPERING_ma),
      GBAHERKOMSTGROEPERING_ma, 
      GBAHERKOMSTGROEPERING
    )
  )


#### MIGRATION BACKGROUND ####


# create migration variable: origin with third generation
cohort_dat <- 
  cohort_dat %>%
  mutate(
    migration_third = as.character(GBAHERKOMSTGROEPERING_third), 
    migration_third = 
      ifelse(!(migration_third %in% 
                 c("Nederland", "Turkije", "Marokko", "Suriname", 
                   # "Nederlandse Antillen (oud)"
                   'Aruba', 'Bonaire', 'Curaçao', 'Saba', 'Sint Eustatius', 
                   'Sint Maarten')), 
             "Overig", migration_third)) %>%
  # create migration antilles
  mutate(migration_third = ifelse(migration_third %in% 
                                    c('Aruba', 'Bonaire', 'Curaçao', 'Saba', 'Sint Eustatius', 
                                      'Sint Maarten'), 'Nederlandse Antillen (oud)', migration_third),
    migration_third = as.factor(migration_third)) 


# remove unnecessary outcomes
cohort_dat <- 
  cohort_dat %>%
  select(-c(GBAHERKOMSTGROEPERING_ma, GBAGENERATIE_ma, 
            GBAHERKOMSTGROEPERING, GBAGENERATIE, 
            GBAGENERATIE_third, GBAHERKOMSTGROEPERING_third))


# create variable for individuals with a migration background
cohort_dat <-
  cohort_dat %>%
  mutate(has_migration = ifelse(migration_third == "Nederland", 0, 1))


# free up memory
rm(gba_dat)



#### TYPE HOUSEHOLDS ####

cohort_rinpersoons <- 
  c(cohort_dat$RINPERSOONS, cohort_dat$RINPERSOON, 
    cohort_dat$CBKSoortNr_Moeder, cohort_dat$Rinpersoon_Moeder)

# import household  data
household_dat <-
  read_sav(file.path(loc$data_folder, loc$household_data),
           col_select = c("RINPERSOONS", "RINPERSOON", "DATUMAANVANGHH",
                          "DATUMEINDEHH", "TYPHH")) %>%
  filter(RINPERSOON %in% cohort_rinpersoons) %>%
  mutate(
    RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
    DATUMAANVANGHH = as.numeric(DATUMAANVANGHH),
    DATUMEINDEHH = as.numeric(DATUMEINDEHH),
    TYPHH = as_factor(TYPHH, levels = "value")
  ) %>%
  mutate(
    DATUMAANVANGHH = ymd(DATUMAANVANGHH),
    DATUMEINDEHH = ymd(DATUMEINDEHH)
  ) 


# keep the first known record that we observe for the child
hh_tab <- 
  household_dat %>%
  group_by(RINPERSOONS, RINPERSOON) %>% 
  filter(row_number() == 1)

# create type household outcome for child
hh_tab <- 
  hh_tab %>%
  mutate(type_hh = ifelse(TYPHH %in% "6", "single parent", "other"),
         type_hh = ifelse(TYPHH %in% c("2", "3", "4", "5"), "two parents", type_hh)) %>%
  select(-c(TYPHH, DATUMAANVANGHH, DATUMEINDEHH))


# if not observed use household type of the mother
hh_tab_mom <- 
  inner_join(household_dat, 
             cohort_dat %>% select(c(CBKSoortNr_Moeder, Rinpersoon_Moeder, birthdate)), 
             by = c("RINPERSOONS" = "CBKSoortNr_Moeder",
                    "RINPERSOON" = "Rinpersoon_Moeder"), 
             relationship = "many-to-many") %>%
  filter(birthdate %within% interval(DATUMAANVANGHH, 
                                     DATUMEINDEHH)) %>%
  group_by(RINPERSOONS, RINPERSOON) %>% 
  summarise(TYPHH_mom = TYPHH[1])


# create type household outcome for mother
hh_tab_mom <- 
  hh_tab_mom %>%
  mutate(type_hh_mom = ifelse(TYPHH_mom %in% "6", "single parent", "other"),
         type_hh_mom = ifelse(TYPHH_mom %in% c("2", "3", "4", "5"), 
                              "two parents", type_hh_mom)) %>%
  select(-c(TYPHH_mom))


# join to cohort
cohort_dat <- 
  cohort_dat %>%
  left_join(hh_tab, by = c("RINPERSOONS", "RINPERSOON"))


cohort_dat <- 
  cohort_dat %>%
  left_join(hh_tab_mom, by = c("CBKSoortNr_Moeder" = "RINPERSOONS",
                               "Rinpersoon_Moeder" = "RINPERSOON"))



# if child type_hh is NA use type_hh_mom
cohort_dat <-
  cohort_dat %>%
  mutate(
    type_hh = ifelse(is.na(type_hh), type_hh_mom, type_hh),
    type_hh = as.factor(type_hh)     
  )  %>%
  select(-type_hh_mom) %>%
  filter(!is.na(type_hh))

rm(household_dat, hh_tab, hh_tab_mom)



#### EDUCATION PARENTS ####

get_hoogstopl_filename <- function(year) {
  # get all HOOGSTEOPLTAB files with the specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "Onderwijs/HOOGSTEOPLTAB", year),
    pattern = "(?i)(.sav)", 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


# load data for linking education numbers to education levels
edu_link <- read_sav(loc$opleiding_data) %>%
  select(OPLNR, CTO2021V) %>%
  left_join(read_sav(loc$cto_data) %>%
              select(CTO, OPLNIVSOI2016AGG4HB), by = c("CTO2021V" = "CTO")) 

parents_education <- tibble(RINPERSOONS = factor(), RINPERSOON = character(),
                            education = integer(), year = integer())
for (year in seq((as.integer(format(dmy(cfg$child_birth_date_min), "%Y")) + cfg$childhood_home_age),
                 (as.integer(format(dmy(cfg$child_birth_date_max), "%Y")) + cfg$childhood_home_age))) {
  
  if (year < 2013) {
    parents_education <- read_sav(get_hoogstopl_filename(year), 
                                  col_select = c("RINPERSOONS", "RINPERSOON", 
                                                 "OPLNRHB")) %>%
      rename(education_number = OPLNRHB) %>%
      mutate(
        RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
        education_number = ifelse(education_number == "----", NA, education_number),
        year = year
      ) %>%
      # add education numbers to education data
      left_join(edu_link, by = c("education_number" = "OPLNR")) %>%
      select(RINPERSOONS, RINPERSOON, OPLNIVSOI2016AGG4HB, year) %>%
      rename(education = OPLNIVSOI2016AGG4HB) %>%
      mutate(education = ifelse(education == "----", NA, education), 
             education = as.numeric(education)) %>%
      # add to data 
      bind_rows(parents_education, .)
    
  }  else if (year >= 2013) {
    parents_education <- read_sav(get_hoogstopl_filename(year), 
                                  col_select = c("RINPERSOONS", "RINPERSOON", 
                                                 "OPLNIVSOI2016AGG4HBMETNIRWO")) %>%
      rename(education = OPLNIVSOI2016AGG4HBMETNIRWO) %>%
      mutate(
        RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
        education = ifelse(education == "----", NA, education),
        education = as.numeric(education),
        year = year
      ) %>%
      # add to data 
      bind_rows(parents_education, .)
  }
}

# create parents education variable
parents_education <- 
  parents_education %>%
  mutate(
    parents_edu = ifelse(education %in% c(3110, 3111, 3112, 3210, 3211), "hbo", "other"), 
    parents_edu = ifelse(education %in% c(3113, 3212, 3213), "wo", parents_edu)
  ) %>%
  select(-education)


# determine parents education of child at a specific age
cohort_dat <- 
  cohort_dat %>%
  mutate(year = as.numeric(format(birthdate, "%Y")) + cfg$childhood_home_age)


# add parents education to cohort
cohort_dat <- 
  cohort_dat %>% 
  left_join(parents_education,
            by = c("Rinpersoon_Moeder" = "RINPERSOON", "CBKSoortNr_Moeder" = "RINPERSOONS", "year")) %>%
  select(-year)

# convert NA to other category
cohort_dat <-
  cohort_dat %>%
  mutate(parents_edu = ifelse(is.na(parents_edu), "other", parents_edu)) %>%
  rename('parents_education' = 'parents_edu')


rm(parents_education, edu_link)

# record sample size
sample_size <- sample_size %>% mutate(n_3_parent_characteristics = nrow(cohort_dat))

#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "02_predictors.rds"))

write_rds(sample_size, file.path(loc$scratch_folder, "02_sample_size.rds"))
