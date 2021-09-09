# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding perinatal outcomes to the cohort.
#   - Writing `scratch/03_outcomes.rds`.
#
# (c) ODISSEI Social Data Science team 2021


#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(readxl)


#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "02_predictors.rds"))


# import percentile weight boys & girls
boys_weight_tab <- read_excel(loc$birthweight_data, sheet = loc$boys_sheet, skip = 2) %>%
  rename(
    gestational_age = "Totaal aantal dagen",
    p10_boys = "p10...6"
  ) %>%
  select(c(gestational_age, p10_boys))

girls_weight_tab <- read_excel(loc$birthweight_data, sheet = loc$girls_sheet, skip = 2) %>%
  rename(
    gestational_age = "Totaal aantal dagen",
    p10_girls = "p10...6"
  ) %>%
  select(c(gestational_age, p10_girls))


#### PERINED ####

# function to get latest perined version of specified year
get_prnl_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "GezondheidWelzijn/PRNL/", year, "/"), 
    pattern = paste0(year, "V[0-9]+(?i)(.sav)"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


# create perinatal data
perined_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                      rinpersoons_moeder = factor(), rinpersoon_moeder = character(), 
                      amddd = double(), datumkind = double(), geslachtkind = factor(), 
                      gewichtkind_ruw = double(), meerling = factor(), 
                      sterfte = factor(), year = integer())
for (year in seq(format(dmy(cfg$child_birth_date_min), "%Y"), 
                 format(dmy(cfg$child_birth_date_max), "%Y"))){
  
  perined_dat <- read_sav(get_prnl_filename(year), col_select = 
                            c("rinpersoons_kind_uitgebreid", "rinpersoon_kind", 
                              "rinpersoons_moeder", "rinpersoon_moeder",
                              "amddd", "datumkind", "geslachtkind", "gewichtkind_ruw", 
                              "meerling", "sterfte")) %>% 
    rename(RINPERSOONS = rinpersoons_kind_uitgebreid,
           RINPERSOON = rinpersoon_kind) %>%
    mutate(
      RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
      rinpersoons_moeder = as_factor(rinpersoons_moeder, levels = "values"),
      geslachtkind = tolower(as_factor(geslachtkind, levels = "labels")),
      meerling = tolower(as_factor(meerling, levels = "labels")),
      sterfte = tolower(as_factor(sterfte, levels = "labels")), 
                          year = year) %>%
  select(c("RINPERSOONS", "RINPERSOON", "rinpersoons_moeder", "rinpersoon_moeder",
           "amddd", "datumkind", "geslachtkind",
           "gewichtkind_ruw", "meerling", "sterfte", "year")) %>%
  bind_rows(perined_dat, .)

}

# remove gestational age below given days
perined_dat <- perined_dat %>%
  unique() %>%
  filter(!(amddd < cfg$cut_off_days_min),
         !(amddd > cfg$cut_off_days_max))


# merge percentile to perined data
perined_dat <- left_join(perined_dat, boys_weight_tab, by = c("amddd" = "gestational_age"))
perined_dat <- left_join(perined_dat, girls_weight_tab, by = c("amddd" = "gestational_age"))

rm(boys_weight_tab, girls_weight_tab)

# post-processing
perined_dat <- perined_dat %>% 
  mutate(datumkind = dmy(datumkind))


#### DO AND DOODOORZTAB ####

## CLEAN DO ##

# function to get latest do version of specified year 
get_do_filename <- function(year) {
  fl <- list.files(
    path = file.path("G:/GezondheidWelzijn/DO/"), 
    pattern = paste0("DO", year, "V[0-9]+(?i)(.sav)"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

# function to get latest do version of specified year 
get_do_map_filename <- function(year) {
  fl <- list.files(
    path = file.path("G:/GezondheidWelzijn/DO/", year, "/"), 
    pattern = paste0("DO ", year, "V[0-9]+(?i)(.sav)"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

death_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                    date_of_death = character(), year = as.numeric())
for (year in seq(format(dmy(cfg$child_birth_date_min), "%Y"),
                 format(dmy(cfg$child_birth_date_max), "%Y"))) {
  
  if (year <= 2011) {
    death_dat <- read_sav(get_do_filename(year), 
                          col_select = c("rinpersoons", "rinpersoon",
                                         "ovljr", "ovlmnd", "ovldag")) %>%
      # convert to uppercase
      rename_all(toupper) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
             date_of_death = paste(OVLJR, OVLMND, OVLDAG, sep = "-"),
             year = year, 
             RINPERSOON = ifelse(RINPERSOON == "", NA, RINPERSOON)) %>%
      select(-c(OVLJR, OVLMND, OVLDAG)) %>%
      # add to death data
      bind_rows(death_dat, .)
    
  } else if (year == 2012) {
    death_dat <- read_sav(get_do_map_filename(year), 
                          col_select = c("rinpersoons", "rinpersoon",
                                         "ovljr", "ovlmnd", "ovldag")) %>%
      # convert to uppercase
      rename_all(toupper) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
             date_of_death = paste(OVLJR, OVLMND, OVLDAG, sep = "-"),
             year = year, 
             RINPERSOON = ifelse(RINPERSOON == "", NA, RINPERSOON)) %>%
      select(-c(OVLJR, OVLMND, OVLDAG)) %>%
      # add to death data
      bind_rows(death_dat, .)
  }
}

# post_processing
death_dat <- death_dat %>%
  mutate(date_of_death = ymd(date_of_death))


## CLEAN DOODOORZTAB ##

# function to get latest doodoorztab version of specified year 
get_dood_filename <- function(year) {
  fl <- list.files(
    path = file.path("G:/GezondheidWelzijn/DOODOORZTAB/", year, "/"),
    pattern = paste0("DOODOORZ", year, "TABV[0-9]+(?i)(.sav)"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

# function to get latest gbaoverlijdenstab version of specified year 
get_gba_filename <- function(year) {
  fl <- list.files(
    path = file.path("G:/Bevolking/GBAOVERLIJDENTAB/", year, "/"),
    pattern = "(?i)(.sav)",
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


dood_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), year = numeric())
gba_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), date_of_death = character())
for (year in seq(2013, format(dmy(cfg$child_birth_date_max), "%Y"))) {
  
  dood_dat <- read_sav(get_dood_filename(year), 
                       col_select = c("RINPERSOONS", "RINPERSOON")) %>%
    mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
           year = year) %>%
    # add to death data
    bind_rows(dood_dat, .)
  
  gba_dat <- read_sav(get_gba_filename(year)) %>%
    rename(date_of_death = GBADatumOverlijden) %>%
    mutate(
      RINPERSOONS = as_factor(RINPERSOONS, levels = "value")
    ) %>%
    # add to death data
    bind_rows(gba_dat, .)
}

# post-processing
dood_dat <- inner_join(dood_dat, gba_dat) %>%
  mutate(date_of_death = ymd(date_of_death))


# create one death data
death_dat <- rbind(death_dat, dood_dat) %>%
  select(-year) %>%
  unique() %>%
  distinct(RINPERSOONS, RINPERSOON, .keep_all = TRUE)
rm(dood_dat, gba_dat)

perined_dat <- left_join(perined_dat, death_dat) 

rm(death_dat)

# filter
perined_dat <- perined_dat %>%
  mutate(
    diff_days = as.numeric(difftime(date_of_death, datumkind, units = "days"))
    ) %>%
  # remove all birth below certain days
  filter(diff_days >= cfg$cut_off_mortality_day | is.na(date_of_death))


#### OUTCOMES ####

# low birth weight & premature birth cohort
perined_dat <- perined_dat %>%
  mutate(
    # premature birth for infants with < 259 gestational age
    premature_birth = ifelse(amddd < 259, 1, 0),
    
    # create low birth weight outcome
    low_birthweight = ifelse((geslachtkind == "jongen" & gewichtkind_ruw < p10_boys) |
                               (geslachtkind == "meisje" & gewichtkind_ruw < p10_girls),
                             1, 0)
  )

cohort_dat <- inner_join(cohort_dat, perined_dat)

# free up memory
rm(perined_dat)


#### HOUSEHOLD INCOME ####
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
    path = file.path("G:/InkomenBestedingen/INTEGRAAL PERSOONLIJK INKOMEN/", year),
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
    path = file.path("G:/InkomenBestedingen/INPATAB/"),
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
    path = file.path("G:/InkomenBestedingen/INTEGRAAL HUISHOUDENS INKOMEN/", year),
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
    path = file.path("G:/InkomenBestedingen/INHATAB/"),
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
if (as.integer(cfg$parent_income_year_max) < 2011) {
  income_household <-
    income_household %>% 
    mutate(household_income = ifelse(household_income == 999999999 | 
                                       household_income < 0, NA, household_income)) 
  
} else {
  income_household <-
    income_household %>% 
    mutate(household_income = ifelse(household_income == 9999999999 | 
                                       household_income < 0, NA, household_income)) 
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



#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))

