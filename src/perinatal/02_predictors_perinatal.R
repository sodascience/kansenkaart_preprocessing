# Kansenkaart data preparation pipeline
#
# 2. Predictor creation.
#    - Adding parent income and income percentile to the cohort.
#    - Adding migration background information to the cohort.
#    - Writing `scratch/02_predictor.rds`
#
# (c) ODISSEI Social Data Science team 2022



#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(readxl)


#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "01_cohort.rds")) %>%
  mutate(birth_year = year(birthdate))


# create a table with incomes at the cpi_base_year level
# first, load consumer price index data (2015 = 100)
# source: CBS statline
cpi_tab <- read_excel(loc$cpi_index_data) %>% 
  mutate(year = as.numeric(year))

# set cpi_base_year = 100
cpi_tab <- cpi_tab %>%
  mutate(
    cpi = cpi / cpi_tab %>% filter(year == cfg$cpi_base_year) %>% pull(cpi) * 100)



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

# parents <- c(cohort_dat$RINPERSOONMa, cohort_dat$RINPERSOONSMa, 
#              cohort_dat$RINPERSOONpa, cohort_dat$RINPERSOONSpa)
# 
# income_parents <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
#                          income = double(), year = integer())
# for (year in seq(as.integer(cfg$parent_income_year_min), as.integer(cfg$parent_income_year_max))) {
#   if (year < 2011) {
#     # use IPI tab
#     income_parents <- 
#       # read file from disk
#       read_sav(get_ipi_filename(year), col_select = c("RINPERSOONS", "RINPERSOON", "PERSBRUT")) %>% 
#       rename(income = PERSBRUT) %>% 
#       mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
#       # select only incomes of parents
#       filter(RINPERSOON %in% parents) %>% 
#       # add year
#       mutate(year = year) %>% 
#       # add to income parents
#       bind_rows(income_parents, .)
#   } else {
#     # use INPA tab
#     income_parents <- 
#       # read file from disk
#       read_sav(get_inpa_filename(year), col_select = c("RINPERSOONS", "RINPERSOON", "INPPERSBRUT")) %>% 
#       rename(income = INPPERSBRUT) %>% 
#       mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
#       # select only incomes of parents
#       filter(RINPERSOON %in% parents) %>% 
#       # add year
#       mutate(year = year) %>% 
#       # add to income parents
#       bind_rows(income_parents, .)
#   }
# }
# 
# # remove NA incomes
# income_parents <-
#   income_parents %>% 
#   mutate(
#     income = ifelse(income == 999999999 | income == 9999999999, NA, income))
# 
# # deflate
# income_parents <- 
#   income_parents %>% 
#   left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
#   mutate(income = income / (cpi / 100)) %>% 
#   select(-cpi)
# 
# 
# 
# # compute mean
# income_parents <- 
#   income_parents %>% 
#   group_by(RINPERSOON, RINPERSOONS) %>% 
#   summarize(income_n = sum(!is.na(income)),
#             income   = mean(income, na.rm = TRUE)) 
# 
# # table of the number of years the mean income is based on
# print(table(`income years` = income_parents$income_n))
# 
# # add to data
# # mothers
# cohort_dat <- left_join(
#   x = cohort_dat, 
#   y = income_parents %>% rename(income_ma = income, income_n_ma = income_n), 
#   by = c("RINPERSOONMa" = "RINPERSOON", "RINPERSOONSMa" = "RINPERSOONS")
# ) %>%
#   select(-income_n_ma)
# # fathers
# cohort_dat <- left_join(
#   x = cohort_dat, 
#   y = income_parents %>% rename(income_pa = income, income_n_pa = income_n), 
#   by = c("RINPERSOONpa" = "RINPERSOON", "RINPERSOONSpa" = "RINPERSOONS")
# ) %>%
#   select(-income_n_pa)
# 
# # free up memory
# rm(income_parents)
# 
# # parents
# cohort_dat <- cohort_dat %>% 
#   rowwise() %>%
#   mutate(income_parents =  sum(income_ma, income_pa, na.rm = TRUE),
#          income_parents = ifelse((is.na(income_ma) & is.na(income_pa)), 
#                                  NA, income_parents))
# 
# 
# # if income_parents is negative then income_parents becomes NA
# cohort_dat <- 
#   cohort_dat %>%
#   mutate(income_parents = ifelse(income_parents < 0, NA, income_parents))%>%
#   # remove income if income_parents is NA (parents income is NA is all years)
#   filter(!is.na(income_parents))
# 
# 
# # compute income transformations
# cohort_dat <- 
#   cohort_dat %>%
#   group_by(birth_year) %>%
#   mutate(
#     income_parents_rank = rank(income_parents, ties.method = "average"),
#     income_parents_perc = income_parents_rank / max(income_parents_rank)
#   ) %>%
#   select(-c(income_parents_rank, income_ma, income_pa)) %>%
#   ungroup()
# 
# 

#### HOUSEHOLD INCOME ####


# LINK MOTHER RINPERSOON TO HOUSEHOLD RINPERSOON

mothers <- c(cohort_dat$RINPERSOONSMa, cohort_dat$RINPERSOONMa)
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
  inner_join(income_household, by = c("RINPERSOONSMa" = "RINPERSOONS",
                                      "RINPERSOONMa" = "RINPERSOON"))

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
  select(-c(GBAHERKOMSTGROEPERING_pa, GBAGENERATIE_pa, GBAHERKOMSTGROEPERING_ma, 
            GBAGENERATIE_ma, GBAHERKOMSTGROEPERING, GBAGENERATIE, 
            GBAGENERATIE_third, GBAHERKOMSTGROEPERING_third))


# create variable for individuals with a migration background
cohort_dat <-
  cohort_dat %>%
  mutate(has_migration_background = ifelse(migration_third == "Nederland", 0, 1))



# free up memory
rm(gba_dat)


#### TYPE HOUSEHOLDS ####

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


# keep the first known record that we observe for the child
hh_tab <- 
  household_dat %>%
  group_by(RINPERSOONS, RINPERSOON) %>% 
  filter(row_number() == 1)

hh_tab <- 
  hh_tab %>%
  mutate(type_hh = ifelse(TYPHH %in% "6", "single parent", "other"),
         type_hh = ifelse(TYPHH %in% c("2", "3", "4", "5"), "two parents", type_hh)) %>%
  select(-c(TYPHH, DATUMAANVANGHH, DATUMEINDEHH))


cohort_dat <- 
  cohort_dat %>%
  left_join(hh_tab, by = c("RINPERSOONS", "RINPERSOON"))


# convert NA to other
cohort_dat <- 
  cohort_dat %>%
  mutate(type_hh = ifelse(is.na(type_hh), "other", type_hh))


rm(household_dat, hh_tab)



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
            by = c("RINPERSOONpa" = "RINPERSOON", "RINPERSOONSpa" = "RINPERSOONS", "year")) %>%
  rename(parents_edu_pa = parents_edu) %>%
  left_join(parents_education,
            by = c("RINPERSOONMa" = "RINPERSOON", "RINPERSOONSMa" = "RINPERSOONS", "year")) %>%
  rename(parents_edu_ma = parents_edu) %>%
  select(-year)

# convert NA to other category
cohort_dat <-
  cohort_dat %>%
  mutate(
    parents_edu_pa = ifelse(is.na(parents_edu_pa), "other", parents_edu_pa),
    parents_edu_ma = ifelse(is.na(parents_edu_ma), "other", parents_edu_ma)
  )


# create parents education
cohort_dat <- 
  cohort_dat %>%
  mutate(
    parents_education = ifelse(parents_edu_pa == "hbo" | parents_edu_ma == "hbo",
                               "hbo", "other"),
    parents_education = ifelse(parents_edu_pa == "wo" | parents_edu_ma == "wo",
                               "wo", parents_education)
  ) %>%
  select(-c(parents_edu_pa, parents_edu_ma))


rm(parents_education, edu_link)



#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "02_predictors.rds"))
