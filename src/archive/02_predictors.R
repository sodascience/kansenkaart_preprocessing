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



#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "01_cohort.rds"))


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



#### PARENT INCOME ####

# get income data from each requested year into single data frame
get_ipi_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INTEGRAAL PERSOONLIJK INKOMEN", year),
    pattern = paste0("PERSOONINK", year, "TABV[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

get_inpa_filename <- function(year) {
  # function to get latest version of specified year
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


# censor income above a certain value
income_parents <-
  income_parents %>%
  mutate(income = ifelse(income > cfg$income_censoring_value, cfg$income_censoring_value, income))


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
  mutate(income_parents = ifelse(income_parents < 0, NA, income_parents))


# remove income if income_parents is NA (parents income is NA is all years)
cohort_dat <- 
  cohort_dat %>%
  filter(!is.na(income_parents))

# compute income transformations
cohort_dat <- 
  cohort_dat %>% 
  ungroup() %>%
  mutate(
    income_parents_rank = rank(income_parents, ties.method = "average"),
    income_parents_perc = income_parents_rank / max(income_parents_rank)
  )



#### PARENT WEALTH ####


# get income data from each requested year into single data frame
get_ihi_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INTEGRAAL HUISHOUDENS INKOMEN", year),
    pattern = "(?i)(.sav)",
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

get_inha_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INHATAB"),
    pattern = paste0("INHA", year, "TABV[0-9]+(?i)(.sav)"),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

get_koppel_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INHATAB"),
    pattern = paste0("KOPPELPERSOONHUISHOUDEN", year),
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


#TODO: change inha tab to vehtab (variable: VEHW1000VERH)
parents <- c(cohort_dat$RINPERSOONMa, cohort_dat$RINPERSOONSMa,
             cohort_dat$RINPERSOONpa, cohort_dat$RINPERSOONSpa)

wealth_parents <- tibble(RINPERSOONS = factor(), RINPERSOON = character(),
                         wealth = double(), year = integer())
for (year in seq(as.integer(cfg$parent_wealth_year_min), as.integer(cfg$parent_wealth_year_max))) {
  if (year < 2011) {

    # use ihi tab
    ihi_parents <-
      # read file from disk
      read_sav(get_ihi_filename(year), col_select = c("RINPERSOONSKERN", "RINPERSOONKERN",
                                                      "BVRPRIMINKH")) %>%
      rename(wealth = BVRPRIMINKH) %>%
      mutate(RINPERSOONSKERN = as_factor(RINPERSOONSKERN, levels = "value")) %>%
      # add year
      mutate(year = year)


    # import ipi for merging individuals to households
    ipi_parents <-
      # read file from disk
      read_sav(get_ipi_filename(year), col_select = c("RINPERSOONS", "RINPERSOON",
                                                      "RINPERSOONSKERN", "RINPERSOONKERN")) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
             RINPERSOONSKERN = as_factor(RINPERSOONSKERN, levels = "value")) %>%
      # select only incomes of parents
      filter(RINPERSOON %in% parents) %>%
      inner_join(ihi_parents, by = c("RINPERSOONSKERN", "RINPERSOONKERN")) %>%
      select(-c(RINPERSOONSKERN, RINPERSOONKERN))

    wealth_parents <- bind_rows(wealth_parents, ipi_parents)
    rm(ipi_parents, ihi_parents)

  } else {
    # load koppel data person to household
    koppel_hh <- read_sav(get_koppel_filename(year)) %>%
      # select only incomes of parents
      filter(RINPERSOON %in% parents) %>% 
      mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value"), 
             RINPERSOONS = as_factor(RINPERSOONS, levels = "value"))

    # use INHA tab
    inha_parents <-
      # read file from disk
      read_sav(get_inha_filename(year), col_select = c("RINPERSOONSHKW", "RINPERSOONHKW",
                                                       "INHPRIMINKH")) %>%
      rename(wealth = INHPRIMINKH) %>%
      mutate(RINPERSOONSHKW = as_factor(RINPERSOONSHKW, levels = "value")) %>%
      # add year
      mutate(year = year) %>%
      inner_join(koppel_hh, by = c("RINPERSOONSHKW", "RINPERSOONHKW")) %>%
      select(-c(RINPERSOONSHKW, RINPERSOONHKW))

    wealth_parents <- bind_rows(wealth_parents, inha_parents)
    rm(inha_parents)

  }
}

# remove NA incomes
wealth_parents <-
  wealth_parents %>%
  mutate(
    wealth = ifelse(wealth == 999999999 | wealth == 9999999999, NA, wealth))

# deflate
wealth_parents <-
  wealth_parents %>%
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>%
  mutate(wealth = wealth / (cpi / 100)) %>%
  select(-cpi)

# censor income above a certain value
wealth_parents <-
  wealth_parents %>%
  mutate(wealth = ifelse(wealth > cfg$income_censoring_value, cfg$income_censoring_value, wealth))


# compute mean
wealth_parents <-
  wealth_parents %>%
  group_by(RINPERSOON, RINPERSOONS) %>%
  summarize(wealth_n = sum(!is.na(wealth)),
            wealth   = mean(wealth, na.rm = TRUE)) %>%
  mutate(wealth = ifelse(is.nan(wealth), NA, wealth))

# table of the number of years the mean income is based on
print(table(`wealth years` = wealth_parents$wealth_n))


# add to data
# mothers
cohort_dat <- left_join(
  x = cohort_dat,
  y = wealth_parents %>% rename(wealth_ma = wealth, wealth_n_ma = wealth_n),
  by = c("RINPERSOONMa" = "RINPERSOON", "RINPERSOONSMa" = "RINPERSOONS")
)
# fathers
cohort_dat <- left_join(
  x = cohort_dat,
  y = wealth_parents %>% rename(wealth_pa = wealth, wealth_n_pa = wealth_n),
  by = c("RINPERSOONpa" = "RINPERSOON", "RINPERSOONSpa" = "RINPERSOONS")
)

# free up memory
rm(wealth_parents)


# parents
cohort_dat <- cohort_dat %>%
  rowwise() %>%
  mutate(wealth_parents =  mean(c(wealth_ma, wealth_pa), na.rm = TRUE),
         wealth_parents = ifelse((is.na(wealth_ma) & is.na(wealth_pa)),
                                 NA, wealth_parents))


# if wealth_parents is negative then wealth_parents becomes NA
cohort_dat <- 
  cohort_dat %>%
  mutate(wealth_parents = ifelse(wealth_parents < 0, NA, wealth_parents))

# remove income if wealth_parents is NA 
cohort_dat <-
  cohort_dat %>%
  filter(!is.na(wealth_parents))


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
      GBAGENERATIE_third == "derde generatie allochtoon" & (GBAGENERATIE_ma != "tweede generatie allochtoon" | 
                                                              is.na(GBAGENERATIE_ma)),
      GBAHERKOMSTGROEPERING_pa, 
      GBAHERKOMSTGROEPERING_third
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


if (cfg$childhood_home_first) {
  # take the first address registration to be their childhood home
  
  hh_tab <- 
    household_dat %>%
    arrange(DATUMAANVANGHH) %>% 
  group_by(RINPERSOONS, RINPERSOON) %>% 
  summarize(type_hh = TYPHH[1]) 
  
} else if (cfg$childhood_home_date) {
  # take the address registration on a specific date
  
hh_tab <- 
  household_dat %>%
  filter(
    DATUMAANVANGHH <= cfg$childhood_home_year & 
      DATUMEINDEHH >= cfg$childhood_home_year
  ) %>%
  group_by(RINPERSOONS, RINPERSOON) %>% 
  summarize(type_hh = TYPHH)  

} else if (cfg$childhood_home_age_date) {

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
    summarize(type_hh = TYPHH)  
  
  rm(age_tab)
  
  # take the address registration to be their childhood home at date of birth
} else if (cfg$childhood_home_birthyear) {
  
  # function to get latest perined version of specified year
  get_prnl_filename <- function(year) {
    fl <- list.files(
      path = file.path(loc$data_folder, "GezondheidWelzijn/PRNL", year), 
      pattern = paste0(year, "V[0-9]+(?i)(.sav)"),
      full.names = TRUE
    )
    # return only the latest version
    sort(fl, decreasing = TRUE)[1]
  }
  
  # create perinatal data to obtain date of birth 
  perined_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(),
                        datumkind = double())
  for (year in seq(format(dmy(cfg$child_birth_date_min), "%Y"), 
                   format(dmy(cfg$child_birth_date_max), "%Y"))){
    
    perined_dat <- read_sav(get_prnl_filename(year), col_select = 
                              c("rinpersoons_kind_uitgebreid", "rinpersoon_kind", 
                                "datumkind")) %>% 
      rename(RINPERSOONS = rinpersoons_kind_uitgebreid,
             RINPERSOON = rinpersoon_kind) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values")) %>%
      filter(RINPERSOON %in% cohort_dat$RINPERSOON & RINPERSOONS %in% cohort_dat$RINPERSOONS) %>%
      select(c("RINPERSOONS", "RINPERSOON", "datumkind")) %>%
      bind_rows(perined_dat, .)
  }
  
  # post-processing
  perined_dat <- perined_dat %>% mutate(datumkind = ymd(datumkind))
  
  hh_tab <- 
    household_dat %>% 
    left_join(perined_dat, by = c("RINPERSOONS", "RINPERSOON")) %>%
    filter(datumkind %within% interval(DATUMAANVANGHH, DATUMEINDEHH)) %>%
    group_by(RINPERSOONS, RINPERSOON) %>% 
    summarize(type_hh = TYPHH)  
  
  rm(perined_dat)
  
} 
 
 # create single parents dummy
 hh_tab <- 
   hh_tab %>%
   mutate(single_parents = ifelse(type_hh == "6", 1, 0), 
          two_parents = ifelse(type_hh %in% c("2", "3", "4", "5"), 1, 0)) %>%
   select(-type_hh)
   
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
     )
   
   
   rm(parents_education, edu_link)
 }
 
 
#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "02_predictors.rds"))
