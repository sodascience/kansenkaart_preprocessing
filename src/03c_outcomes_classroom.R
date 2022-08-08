# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding classroom outcomes to the cohort.
#
# (c) ODISSEI Social Data Science team 2021


#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(lubridate)
library(readxl)


#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "02_predictors.rds"))


#### CLASSROOM OUTCOMES ####

# function to get latest inschrwpo version of specified year
get_inschrwpo_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "Onderwijs/INSCHRWPOTAB"),
    pattern = paste0("INSCHRWPOTAB", year, "V[0-9]+(?i)(.sav)"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


school_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), WPOLEERJAAR = character(), 
                     WPOBRIN_crypt = character(), WPOBRINVEST = character(), WPOGROEPSGROOTTE = character(),
                     WPOREKENEN = character(), WPOTAALLV = character(), WPOTAALTV = character(),
                     WPOTOETSADVIES = character(), WPOADVIESVO = character(), 
                     WPOADVIESHERZ = character())
for (year in seq(as.integer(cfg$classroom_year_min), as.integer(cfg$classroom_year_max))) {
  school_dat <- 
    # read file from disk
    read_sav(get_inschrwpo_filename(year), 
             col_select = c("RINPERSOONS", "RINPERSOON", "WPOLEERJAAR", "WPOGROEPSGROOTTE",
                           "WPOTOETSADVIES","WPOADVIESVO", "WPOADVIESHERZ", 
                           "WPOREKENEN", "WPOTAALLV", "WPOTAALTV",
                           "WPOBRIN_crypt", "WPOBRINVEST")) %>% 
    mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
    # add year
    mutate(year = year) %>% 
    # add to income children
    bind_rows(school_dat, .)
}


# keep group 8 pupils
school_dat <- school_dat %>%
  filter(RINPERSOONS == "R") %>%
  mutate(WPOLEERJAAR = trimws(as.character(WPOLEERJAAR))) %>%
  filter(WPOLEERJAAR == "8")


# keep unique observations
school_dat <- school_dat %>%
  # when duplicated pick the last year
  arrange(desc(year)) %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  filter(row_number() == 1)


#### SCHOOL ID ####

school_dat <-
  school_dat %>%
  mutate(across(c("WPOBRIN_crypt", "WPOBRINVEST"), as.character)) %>%
  mutate(school_ID = paste0(WPOBRIN_crypt, WPOBRINVEST)) %>%
  select(-c(WPOBRIN_crypt, WPOBRINVEST))



#### INDIVIDUAL OUTCOMES ####
# add individual havo advice test to the cohort
# add individual reading to the cohort

test_score_tab <- school_dat %>%
  select(RINPERSOONS, RINPERSOON, WPOTOETSADVIES, WPOTAALTV) %>%
  mutate(
    WPOTOETSADVIES = as.character(WPOTOETSADVIES),
    WPOTOETSADVIES = as.numeric(WPOTOETSADVIES),
    havo_test = ifelse(WPOTOETSADVIES %in% c(60, 61, 70), 1, 0),
    havo_test = ifelse(is.na(WPOTOETSADVIES), NA, havo_test),
    
    WPOTAALTV = as.character(WPOTAALTV),
    WPOTAALTV = as.numeric(WPOTAALTV),
    language = ifelse(WPOTAALTV == 4, 1, 0),
    language = ifelse(is.na(WPOTAALTV), NA, language)
    ) %>%
  select(-WPOTOETSADVIES, -WPOTAALTV)


cohort_dat <- cohort_dat %>%
  left_join(test_score_tab, by = c("RINPERSOONS", "RINPERSOON"))
rm(test_score_tab)


#### GBA TO GET ORIGIN ####
gba_path <- file.path(loc$data_folder, loc$gba_data)
gba_dat <-  
  read_sav(gba_path, col_select = c("RINPERSOONS", "RINPERSOON", "GBAGEBOORTELAND", "GBAGESLACHT", 
                                    "GBAGEBOORTEJAAR", "GBAGEBOORTEMAAND", "GBAGEBOORTEDAG", "GBAGENERATIE", 
                                    "GBAHERKOMSTGROEPERING", "GBAGEBOORTELANDMOEDER", "GBAGEBOORTELANDVADER",
                                    "GBAGEBOORTEJAARMOEDER", "GBAGEBOORTEMAANDMOEDER", "GBAGEBOORTEDAGMOEDER",
                                    "GBAGEBOORTEJAARVADER", "GBAGEBOORTEMAANDVADER", "GBAGEBOORTEDAGVADER" )) %>% 
  mutate(birthdate = dmy(paste(GBAGEBOORTEDAG, GBAGEBOORTEMAAND, GBAGEBOORTEJAAR, sep = "-")),
         birthdate_pa = dmy(paste(GBAGEBOORTEDAGVADER, GBAGEBOORTEMAANDVADER, GBAGEBOORTEJAARVADER, sep = "-")),
         birthdate_ma = dmy(paste(GBAGEBOORTEDAGMOEDER, GBAGEBOORTEMAANDMOEDER, GBAGEBOORTEJAARMOEDER, sep = "-"))) %>% 
  select(-c(GBAGEBOORTEJAAR, GBAGEBOORTEMAAND, GBAGEBOORTEDAG,
           GBAGEBOORTEDAGVADER, GBAGEBOORTEMAANDVADER, GBAGEBOORTEJAARVADER,
           GBAGEBOORTEDAGMOEDER, GBAGEBOORTEMAANDMOEDER, GBAGEBOORTEJAARMOEDER)) %>%
  mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
         # GBAGEBOORTELAND = as_factor(GBAGEBOORTELAND, levels = "labels"),
         # GBAGEBOORTELANDMOEDER = as_factor(GBAGEBOORTELANDMOEDER, levels = "labels"),
         # GBAGEBOORTELANDVADER = as_factor(GBAGEBOORTELANDVADER, levels = "labels"),
         # GBAHERKOMSTGROEPERING = as_factor(GBAHERKOMSTGROEPERING, levels = "labels"),
         GBAGENERATIE = as_factor(GBAGENERATIE, levels = "labels"),
         GBAGESLACHT = as_factor(GBAGESLACHT, levels = "labels"))


# filter out children with too old or too young parents
gba_dat <- 
  gba_dat %>% 
  mutate(
    age_at_birth_ma = interval(birthdate_ma, birthdate) / years(1),
    age_at_birth_pa = interval(birthdate_pa, birthdate) / years(1)
  ) %>% 
  filter(
    (is.na(age_at_birth_ma) | (age_at_birth_ma >= cfg$parent_min_age & age_at_birth_ma <= cfg$parent_max_age)),
    (is.na(age_at_birth_pa) | (age_at_birth_pa >= cfg$parent_min_age & age_at_birth_pa <= cfg$parent_max_age))
  )



school_dat <- school_dat %>% inner_join(gba_dat, by = c("RINPERSOONS", "RINPERSOON"))



#### PARENT LINK ####
# add parent id to cohort
kindouder_path <- file.path(loc$data_folder, loc$kind_data)
school_dat <- inner_join(
  x = school_dat, 
  y = read_sav(kindouder_path) %>% select(-(XKOPPELNUMMER)) %>% 
    as_factor(only_labelled = TRUE, levels = "values"),
  by = c("RINPERSOONS", "RINPERSOON")
)


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

parents <- c(school_dat$RINPERSOONMa, school_dat$RINPERSOONSMa, 
             school_dat$RINPERSOONpa, school_dat$RINPERSOONSpa)

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
if (as.integer(cfg$parent_income_year_max) < 2011) {
  income_parents <-
    income_parents %>% 
    mutate(income = ifelse(income == 999999999, NA, income)) 
  
} else {
  income_parents <-
    income_parents %>% 
    mutate(income = ifelse(income == 9999999999, NA, income)) 
}

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
school_dat <- left_join(
  x = school_dat, 
  y = income_parents %>% rename(income_ma = income, income_n_ma = income_n), 
  by = c("RINPERSOONMa" = "RINPERSOON", "RINPERSOONSMa" = "RINPERSOONS")
)
# fathers
school_dat <- left_join(
  x = school_dat, 
  y = income_parents %>% rename(income_pa = income, income_n_pa = income_n), 
  by = c("RINPERSOONpa" = "RINPERSOON", "RINPERSOONSpa" = "RINPERSOONS")
)

# parents
school_dat <- school_dat %>% 
  rowwise() %>%
  mutate(income_parents =  sum(income_ma, income_pa, na.rm = TRUE),
         income_parents = ifelse((is.na(income_ma) & is.na(income_pa)), 
                                 NA, income_parents))

# free up memory
rm(income_parents)

# if income_parents is negative then income_parents is NA
cohort_dat <- 
  cohort_dat %>%
  mutate(income_parents = ifelse(income_parents < 0, NA, income_parents))


# remove income if income_parents is NA (parents income is NA is all years)
school_dat <- 
  school_dat %>%
  filter(!is.na(income_parents))

# compute income transformations
school_dat <- 
  school_dat %>%
  ungroup() %>%
  mutate(
    income_parents_rank = rank(income_parents, ties.method = "average"),
    income_parents_perc = income_parents_rank / max(income_parents_rank),
    parents_rank_100 = ntile(income_parents, 100)
  )



#### SCHOOL outcomes ####


# convert NA
school_dat <-
  school_dat %>%
  mutate(across(c("WPOGROEPSGROOTTE", "WPOTOETSADVIES", "WPOADVIESVO", 
                  "WPOREKENEN", "WPOTAALLV", "WPOTAALTV",
                  "WPOADVIESHERZ"), as.character)) %>%
  mutate(across(c("WPOGROEPSGROOTTE", "WPOTOETSADVIES", 
                  "WPOREKENEN", "WPOTAALLV", "WPOTAALTV",
                  "WPOADVIESVO", "WPOADVIESHERZ"), as.numeric))


# create outcome variables
school_dat <-
  school_dat %>%
  mutate(
    # rekenen, lezen & taalverzorging
    math     = ifelse((WPOREKENEN == 3 | WPOREKENEN == 4), 1, 0),
    reading  = ifelse(WPOTAALLV == 4, 1, 0),
    language = ifelse(WPOTAALTV == 4, 1, 0),
    
    # cito test outcomes
    vmbo_gl_test = ifelse(WPOTOETSADVIES %in% c(42, 44, 60, 61, 70), 1, 0),
    havo_test      = ifelse(WPOTOETSADVIES %in% c(60, 61, 70), 1, 0),
    vwo_test       = ifelse(WPOTOETSADVIES == 70, 1, 0),
    
    # final high school advice 
    # replace 80 (= Geen specifiek advies mogelijk) with NA
    WPOADVIESVO = ifelse(WPOADVIESVO == 80, NA, WPOADVIESVO),
    # replace final school advice (wpoadviesvo) with wpoadviesherz if wpoadviesherz is not missing
    final_school_advice = ifelse(!is.na(WPOADVIESHERZ), WPOADVIESHERZ, WPOADVIESVO),
    
    vmbo_gl_final = ifelse(final_school_advice %in% c(40, 41, 42, 43, 44, 45, 50, 
                                                             51, 52, 53, 60, 61, 70), 1, 0),
    havo_final      = ifelse(final_school_advice %in% c(60, 61, 70), 1, 0),
    vwo_final       = ifelse(final_school_advice == 70, 1, 0)
  )


# replace NA
school_dat <-
  school_dat %>%
  mutate(
    math     = ifelse(is.na(WPOREKENEN), NA, math),
    reading  = ifelse(is.na(WPOTAALLV), NA, reading),
    language = ifelse(is.na(WPOTAALTV), NA, language),
    
    # cito test outcomes
    vmbo_gl_test = ifelse(is.na(WPOTOETSADVIES), NA, vmbo_gl_test),
    havo_test      = ifelse(is.na(WPOTOETSADVIES), NA, havo_test),
    vwo_test       = ifelse(is.na(WPOTOETSADVIES), NA, vwo_test)
  )


# create parents rank income outcomes
school_dat <-
  school_dat %>%
  mutate(
    # create dummy for below 25th 
    income_below_25th = ifelse(income_parents_perc < 0.25, 1, 0),
    
    # create dummy for above 75th
    income_above_75th = ifelse(income_parents_perc > 0.75, 1, 0)
  )


# create outcome for children with both parents born in a foreign country
school_dat <-
  school_dat %>%
  mutate(
    GBAGEBOORTELANDMOEDER = as_factor(GBAGEBOORTELANDMOEDER),
    GBAGEBOORTELANDVADER = as_factor(GBAGEBOORTELANDVADER)
  ) %>%
  mutate(
    foreign_born_parents = 
      ifelse((GBAGEBOORTELANDMOEDER != "Nederland" & 
                GBAGEBOORTELANDVADER != "Nederland"),  1, 0))


#### CLASSROOM OUTCOMES ####

# 1.  class_vmbo_gl_test
# 2.  class_havo_test
# 3.  class_vwo_test
# 4.  class_foreign_born_parents
# 5.  class_parents_below_25
# 6.  class_parents_above_75
# 7. class_size (2014 - 2016)
# 8. class_math
# 9. class_language
# 10. class_reading


# only keep classes with more than one student per class
school_dat <-
  school_dat %>%
  group_by(school_ID, year) %>%
  mutate(n = n()) %>%
  filter(n > 1)


# hold out mean function
hold_out_means <- function(x) {
  hold <- ((sum(x, na.rm = TRUE) - x) / (length(x) - 1))
  return(hold)
}


# hold out means = mean of the class without the child him/herself
school_dat <-
  school_dat %>%
  group_by(school_ID, year) %>%
  mutate(
    N_students_per_school = n(),
    
    class_math = hold_out_means(math),
    class_reading = hold_out_means(reading),
    class_language = hold_out_means(language),
    
    class_foreign_born_parents = hold_out_means(foreign_born_parents),
    
    class_vmbo_gl_test = hold_out_means(vmbo_gl_test),
    class_havo_test = hold_out_means(havo_test),
    class_vwo_test = hold_out_means(vwo_test),

    class_income_below_25th = hold_out_means(income_below_25th),
    class_income_above_75th = hold_out_means(income_above_75th)
) %>% 
  rename(class_size = WPOGROEPSGROOTTE)
  

# select relevant variables
school_dat <-
  school_dat %>%
  select(RINPERSOONS, RINPERSOON, school_ID, year, 
         N_students_per_school, class_foreign_born_parents,  
         class_vmbo_gl_test, class_havo_test, class_vwo_test, 
         class_income_below_25th, class_income_above_75th, class_size,
         class_math, class_reading, class_language)


cohort_dat <- inner_join(cohort_dat, school_dat, 
                         by = c("RINPERSOONS", "RINPERSOON"))
   

#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))


