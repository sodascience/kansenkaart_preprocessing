# Kansenkaart data preparation pipeline
#
# 2. Predictor creation.
#    - Adding household income and income percentile to the cohort.
#    - Adding migration background information to the cohort.
#    - Writing `scratch/02_predictor.rds`
#
# (c) ODISSEI Social Data Science team 2021



#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)

#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "01_cohort.rds"))


#### PARENT INCOME ####
# create a table with incomes at the cpi_base_year level
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


#### LINK MOTHER RINPERSOON TO HOUSEHOLD RINPERSOON ####
# get income data from each requested year into single data frame
get_ipi_filename <- function(year) {
  # function to get latest ipi version of specified year
  # get all ipi files with the specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INTEGRAAL PERSOONLIJK INKOMEN", year),
    pattern = paste0("PERSOONINK", year), 
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
    pattern = paste0("INPA", year), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

mothers <- c(cohort_dat$rinpersoons_moeder, cohort_dat$rinpersoon_moeder)
mothers_rinpersoon <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                             RINPERSOONSHKW = factor(), RINPERSOONHKW = character(),
                             year = double())
for (year in seq(as.integer(cfg$parent_income_year_min), 
                 as.integer(cfg$parent_income_year_max))) {
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
             year = year
      ) %>%
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
             year = year
      ) %>%
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

for (year in seq(as.integer(cfg$parent_income_year_min), 
                 as.integer(cfg$parent_income_year_max))) {
  
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
if (as.integer(cfg$parent_income_year_max) < 2011) {
  income_household <-
    income_household %>% 
    mutate(household_income = ifelse(household_income == 999999999, NA, household_income)) 
  
} else {
  income_household <-
    income_household %>% 
    mutate(household_income = ifelse(household_income == 9999999999,
                                     NA, household_income)) 
} 

# censor income above a certain value
income_household <-
  income_household %>%
  mutate(household_income = ifelse(household_income > cfg$income_censoring_value, 
                                   cfg$income_censoring_value, household_income))

# deflate
income_household <- 
  income_household %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(household_income = household_income / (cpi / 100)) %>% 
  select(-cpi)


income_household <- income_household %>%
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


cohort_dat <- cohort_dat %>%
  inner_join(income_household, by = c("rinpersoons_moeder" = "RINPERSOONS",
                                      "rinpersoon_moeder" = "RINPERSOON"))

rm(income_household)

# if household_income is negative then household_income is NA
cohort_dat <- 
  cohort_dat %>%
  mutate(household_income = ifelse(household_income < 0, NA, household_income))


# remove income if household_income is NA 
cohort_dat <- 
  cohort_dat %>%
  filter(!is.na(household_income))


# compute income transformations
cohort_dat <- 
  cohort_dat %>% 
  group_by(year) %>%
  # ungroup() %>%
  mutate(
    income_household_rank = rank(household_income, ties.method = "average"),
    income_household_perc = income_household_rank / max(income_household_rank),
    household_rank_100 = ntile(household_income, 100)
  ) 

cohort_dat <- 
  cohort_dat %>% 
  rename(parents_income_n    = household_income_n,
         income_parents      = household_income,
         income_parents_rank = income_household_rank,
         income_parents_perc = income_household_perc,
         parents_rank_100    = household_rank_100)



#### THIRD GENERATION ####

#### ADD ORIGIN AND GENERATION OF THE CHILD ####
gba_path <- file.path(loc$data_folder, loc$gba_data)
gba_dat <-  
  read_sav(gba_path, col_select = c("RINPERSOONS", "RINPERSOON",
                                    "GBAGEBOORTELAND", "GBAGENERATIE", 
                                    "GBAHERKOMSTGROEPERING")) %>% 
  mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
         GBAGEBOORTELAND = as_factor(GBAGEBOORTELAND, levels = "labels"),
         GBAHERKOMSTGROEPERING = as_factor(GBAHERKOMSTGROEPERING, levels = "labels"),
         GBAGENERATIE = as_factor(GBAGENERATIE, levels = "labels"))


# merge to child
cohort_dat <- cohort_dat %>%
  left_join(gba_dat, by = c("RINPERSOONS", "RINPERSOON"))


# merge to mother
cohort_dat <- cohort_dat %>%
  left_join(gba_dat, by = c("rinpersoons_moeder" = "RINPERSOONS", 
                            "rinpersoon_moeder" = "RINPERSOON"),
            suffix = c("", "_ma"))


# replace child generation, birth country, and origin to mom if missing
cohort_dat <- cohort_dat %>%
  mutate(across(c("GBAGENERATIE", "GBAGENERATIE_ma", "GBAHERKOMSTGROEPERING", 
                  "GBAHERKOMSTGROEPERING_ma", "GBAGEBOORTELAND", 
                  "GBAGEBOORTELAND_ma"), as.character)) %>% 
  mutate(
    GBAGENERATIE = ifelse(is.na(GBAGENERATIE), GBAGENERATIE_ma, GBAGENERATIE),
    GBAHERKOMSTGROEPERING = ifelse(is.na(GBAHERKOMSTGROEPERING), 
                                   GBAHERKOMSTGROEPERING_ma, GBAHERKOMSTGROEPERING),
    GBAGEBOORTELAND = ifelse(is.na(GBAGEBOORTELAND), GBAGEBOORTELAND_ma, GBAGEBOORTELAND)
  )


# replace autochtoon to third generation if mom was second generation  (2) 
# and the child is native (0)
cohort_dat <- 
  cohort_dat %>%
  mutate(
    GBAGENERATIE_third = as.character(GBAGENERATIE),
    GBAGENERATIE_third = ifelse(
      GBAGENERATIE == "autochtoon" & GBAGENERATIE_ma == "tweede generatie allochtoon" &
        !is.na(GBAGENERATIE_ma), 
      "derde generatie allochtoon", 
      GBAGENERATIE_third
    )
  )

# replace gbaherkomstgroepering if the child is third generation
cohort_dat <- 
  cohort_dat %>%
  mutate(
    # third generation child gets mom's origin
    GBAHERKOMSTGROEPERING_third = ifelse(
      GBAGENERATIE_third == "derde generatie allochtoon" & 
        !is.na(GBAHERKOMSTGROEPERING_ma),
      GBAHERKOMSTGROEPERING_ma, 
      GBAHERKOMSTGROEPERING
    )
  )


#### MIGRATION BACKGROUND ####
western_tab <- read_sav(loc$migration_data, 
                        col_select = c("LAND", "LANDTYPE")) %>%
  mutate(
    LAND = as_factor(LAND, levels = "labels"),
    LANDTYPE = as_factor(LANDTYPE, levels = "labels")
  )

# create migration variable with origin without third generation
cohort_dat <- cohort_dat %>%
  left_join(western_tab, by = c("GBAHERKOMSTGROEPERING" = "LAND")) %>%
  mutate(
    migration_third = as.character(LANDTYPE),
    migration_third = ifelse(GBAHERKOMSTGROEPERING_third == "Nederland", "Nederland", migration_third),
    migration_third = ifelse(GBAHERKOMSTGROEPERING_third == "Turkije", "Turkije", migration_third),
    migration_third = ifelse(GBAHERKOMSTGROEPERING_third == "Marokko", "Marokko", migration_third),
    migration_third = ifelse(GBAHERKOMSTGROEPERING_third == "Suriname", "Suriname", migration_third),
    migration_third = ifelse(GBAHERKOMSTGROEPERING_third == "Nederlandse Antillen (oud)", "Nederlandse Antillen (oud)", migration_third),
    total_non_western_third = ifelse(migration_third == "NietWesters" |  migration_third == "Turkije" |
                                       migration_third == "Marokko" | migration_third == "Suriname" |
                                       migration_third == "Nederlandse Antillen (oud)", 1, 0)) %>%
  select(-LANDTYPE)

# free up memory
rm(gba_dat, western_tab)


#### SINGLE PARENTS HOUSEHOLDS ####

# import household  data
household_dat <-
  read_sav(file.path(loc$data_folder, loc$household_data),
           col_select = c("RINPERSOONS", "RINPERSOON", "DATUMAANVANGHH",
                          "DATUMEINDEHH", "TYPHH")) %>%
  filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>%
  mutate(
    RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
    DATUMAANVANGHH = as.numeric(DATUMAANVANGHH),
    DATUMEINDEHH = as.numeric(DATUMEINDEHH),
    TYPHH = as_factor(TYPHH, levels = "value")
  ) %>%
  mutate_all(na_if, "") %>%
  mutate(
    DATUMAANVANGHH = ymd(DATUMAANVANGHH),
    DATUMEINDEHH = ymd(DATUMEINDEHH)
  ) 

  
# onyl keep children within an time interval
  hh_tab <- 
    household_dat %>% 
    left_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, datumkind), 
              by = c("RINPERSOONS", "RINPERSOON")) %>%
    filter(datumkind %within% interval(DATUMAANVANGHH, DATUMEINDEHH)) %>%
    group_by(RINPERSOONS, RINPERSOON) %>% 
    summarize(type_hh = TYPHH)  
  

# create single parents dummy
  hh_tab <- 
    hh_tab %>%
    mutate(single_parents = ifelse(type_hh == "6", 1, 0), 
           two_parents = ifelse(type_hh %in% c("2", "3", "4", "5"), 1, 0))

cohort_dat <- 
  cohort_dat %>%
  left_join(hh_tab, by = c("RINPERSOONS", "RINPERSOON"))

# convert NA to 0
cohort_dat <- 
  cohort_dat %>%
  mutate(single_parents = ifelse(is.na(single_parents), 0, single_parents), 
         two_parents = ifelse(is.na(two_parents), 0, two_parents))

rm(household_dat, hh_tab)



#### EDUCATION PARENTS ####

if (cfg$education_parents) {
  
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
    mutate(year = as.numeric(format(datumkind, "%Y")) + cfg$childhood_home_age)
  
  
  # add parents education to cohort
  cohort_dat <- 
    cohort_dat %>% 
    left_join(parents_education,
              by = c("rinpersoons_moeder" = "RINPERSOONS", "rinpersoon_moeder" = "RINPERSOON", "year")) %>%
    rename(parents_education = parents_edu) %>%
    select(-year)
  
  # convert NA to other category
  cohort_dat <-
    cohort_dat %>%
    mutate(
      parents_education = ifelse(is.na(parents_education), "other", parents_education)
    )
  
  rm(parents_education, edu_link)
}


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "02_predictors.rds"))

