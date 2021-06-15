# Kansenkaart data preparation pipeline
#
# 2. Predictor creation.
#    - Adding parent income and income percentile to the cohort.
#    - Adding migration background information to the cohort.
#    - Adding gender information to the cohort.
#    - Writing `scratch/02_predictor.rds`
#
# (c) ODISSEI Social Data Science team 2021

#### PACKAGES ####
library(tidyverse)
library(haven)

#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "01_cohort.rds"))

#### PARENT INCOME ####
# create a table with incomes at the cpi_base_year€ level
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


# get income data from each requested year into single data frame
get_ipi_filename <- function(year) {
  # function to get latest ipi version of specified year
  # get all ipi files with the specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INTEGRAAL PERSOONLIJK INKOMEN/"),
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
    path = file.path(loc$data_folder, "InkomenBestedingen/INPATAB/"),
    pattern = paste0("INPA", year, "TABV[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

parents <- c(cohort_dat$RINPERSOONMa, cohort_dat$RINPERSOONSMa, 
             cohort_dat$RINPERSOONpa, cohort_dat$RINPERSOONSpa)

income_parents <- tibble(RINPERSOONS = factor(), RINPERSOON = integer(), 
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

# remove negative and NA incomes
income_parents <-
  income_parents %>% 
  mutate(income = ifelse(income == 9999999999 | income < 0, NA, income)) 

# censor income above a certain value
income_parents <-
  income_parents %>%
  mutate(income = ifelse(income > cfg$income_censoring_value, cfg$income_censoring_value, income))

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
)
# fathers
cohort_dat <- left_join(
  x = cohort_dat, 
  y = income_parents %>% rename(income_pa = income, income_n_pa = income_n), 
  by = c("RINPERSOONpa" = "RINPERSOON", "RINPERSOONSpa" = "RINPERSOONS")
)

# parents
cohort_dat <- cohort_dat %>% mutate(income_parents = income_ma + income_pa)

# free up memory
rm(income_parents)

# remove income if income_parents is NA (parents income is NA is all years)
cohort_dat <- cohort_dat %>%
  filter(!is.na(income_parents))

# compute income transformations
cohort_dat <- 
  cohort_dat %>% 
  mutate(
    income_parents_1log = log1p(income_parents),
    income_parents_rank = rank(income_parents, na.last = "keep", ties.method = "average"),
    income_parents_perc = income_parents_rank / max(income_parents_rank)
  )



#### THIRD GENERATION ####

# import gba for parents generation and origin
gba_path <- file.path(loc$data_folder, loc$gba_data)
gba_dat <-  
  read_sav(gba_path, col_select = c("RINPERSOONS", "RINPERSOON", "GBAGEBOORTELAND", 
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
    GBAGENERATIE_third = ifelse(
      GBAGENERATIE == "autochtoon" & (GBAGENERATIE_pa == "tweede generatie allochtoon" | GBAGENERATIE_ma == "tweede generatie allochtoon"), 
      "derde generatie allochtoon", 
      GBAGENERATIE
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
      GBAGENERATIE_third == "derde generatie allochtoon", 
      GBAHERKOMSTGROEPERING_ma, 
      GBAHERKOMSTGROEPERING
    ),
    # except when mom is not second generation, then dad's origin
    GBAHERKOMSTGROEPERING_third = ifelse(
      GBAGENERATIE_third == "derde generatie allochtoon" & GBAGENERATIE_ma != "tweede generatie allochtoon", 
      GBAHERKOMSTGROEPERING_pa, 
      GBAHERKOMSTGROEPERING
    )
  )

# #### MIGRATION BACKGROUND ####
western_tab <- read_sav(loc$migration_data)

# create migration variable with origin without third generation
cohort_dat <- cohort_dat %>%
  left_join(western_tab, by = c("GBAHERKOMSTGROEPERING" = "LANDEN")) %>%
  rename(migration = LANDTYPE) %>%
  mutate(
    migration = ifelse(GBAHERKOMSTGROEPERING == "Nederland", "Nederland", migration),
    migration = ifelse(GBAHERKOMSTGROEPERING == "Turkije", "Turkije", migration),
    migration = ifelse(GBAHERKOMSTGROEPERING == "Marokko", "Marokko", migration),
    migration = ifelse(GBAHERKOMSTGROEPERING == "Suriname", "Suriname", migration),
    migration = ifelse(GBAHERKOMSTGROEPERING == "Nederlandse Antillen (oud)", "Nederlandse Antillen (oud)", migration),
    
    # create total non western dummy
    total_non_western = ifelse(migration == "NietWesters" |  migration == "Turkije" |
                                 migration == "Marokko" | migration == "Suriname" |
                                 migration == "Nederlandse Antillen (oud)", 1, 0)
  ) %>%
  mutate(
    migration_third = ifelse(GBAHERKOMSTGROEPERING_third == "Nederland", "Nederland", migration),
    migration_third = ifelse(GBAHERKOMSTGROEPERING_third == "Turkije", "Turkije", migration),
    migration_third = ifelse(GBAHERKOMSTGROEPERING_third == "Marokko", "Marokko", migration),
    migration_third = ifelse(GBAHERKOMSTGROEPERING_third == "Suriname", "Suriname", migration),
    migration_third = ifelse(GBAHERKOMSTGROEPERING_third == "Nederlandse Antillen (oud)", "Nederlandse Antillen (oud)", migration),
    total_non_western_third = ifelse(migration == "NietWesters" |  migration == "Turkije" |
                                       migration == "Marokko" | migration == "Suriname" |
                                       migration == "Nederlandse Antillen (oud)", 1, 0)) # create total non western dummy



#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "02_predictors.rds"))
