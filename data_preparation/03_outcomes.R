# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding (socio)economic outcomes to the cohort.
#   - Adding education outcomes to the cohort.
#   - Adding health outcomes to the cohort.
#   - Writing `scratch/03_outcomes.rds`.
#
# (c) ODISSEI Social Data Science team 2021

#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)

#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds("scratch/02_predictors.rds")

# load the configuration
cfg <- config::get("data_preparation")
loc <- config::get("file_locations")

#### CHILD INCOME ####
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


income_children <- tibble(RINPERSOON = integer(), income = double(), year = integer())
for (year in seq(as.integer(cfg$child_income_year_min), as.integer(cfg$child_income_year_max))) {
  if (year < 2011) {
    # use IPI tab
    income_children <- 
      # read file from disk
      read_sav(get_ipi_filename(year), col_select = c("RINPERSOON", "PERSBRUT")) %>% 
      rename(income = PERSBRUT) %>% 
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
      read_sav(get_inpa_filename(year), col_select = c("RINPERSOON", "INPPERSBRUT")) %>% 
      rename(income = INPPERSBRUT) %>% 
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
  mutate(income = ifelse(income == 9999999999 | income < 0, NA, income)) 

# deflate
income_children <- 
  income_children %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(income = income / (cpi / 100)) %>% 
  select(-cpi)

# compute mean
income_children <- 
  income_children %>% 
  group_by(RINPERSOON) %>% 
  summarize(income_n = sum(!is.na(income)),
            income = mean(income, na.rm = TRUE))

# table of the number of years the mean income is based on
print(table(`income years` = income_children$income_n))

# add to data
cohort_dat <- left_join(cohort_dat, income_children, by = "RINPERSOON")

# free up memory
rm(income_children)

# compute income transformations
cohort_dat <- 
  cohort_dat %>% 
  mutate(
    income_1log = log(income + 1),
    income_rank = rank(income, na.last = "keep", ties.method = "average"),
    income_perc = income_rank / max(income_rank)
  )

#### HIGHER EDUCATION ####
hopl_tab <- 
  read_sav(file.path(loc$data_folder, loc$hoogste_opl_data)) %>% 
  mutate(RINPERSOONS = as_factor(RINPERSOONS))

hopl_tab <-
  hopl_tab %>% 
  rename(
    edu_followed = OPLNIVSOI2016AGG4HGMETNIRWO, 
    edu_attained = OPLNIVSOI2016AGG4HBMETNIRWO
  ) %>% 
  mutate(across(c(edu_followed, edu_attained), na_if, 9999)) %>% 
  mutate(
    hbo_attained = as.integer(edu_attained > 3000),
    wo_attained  = as.integer(edu_attained %in% c(3113, 3212, 3213))
    # hbo_followed     = as.integer(edu_followed > 3000), # codes above 3000 indicate hbo
    # wo_followed      = as.integer(edu_followed %in% c(3113, 3212, 3213)),
    # edu_lvl_followed = factor(floor(edu_followed/1000), levels = 1:3, labels = c("lower", "middle", "higher")),
    # edu_lvl_attained = factor(floor(edu_attained/1000), levels = 1:3, labels = c("lower", "middle", "higher"))
  ) %>% 
  select(-edu_attained, -edu_followed)

cohort_dat <- left_join(cohort_dat, hopl_tab)


#### SOCIOECONOMIC ####
secm_tab <- 
  read_sav(file.path(loc$data_folder, loc$secm_data), 
           col_select = c("RINPERSOONS", "RINPERSOON", "AANVSECM", "EINDSECM", "SECM")) %>% 
  mutate(RINPERSOONS = as_factor(RINPERSOONS))

secm_tab <- 
  secm_tab %>%
  mutate(
    AANVSECM = ymd(AANVSECM),
    EINDSECM = ymd(EINDSECM)
    ) %>%
  filter(
    AANVSECM <= cfg$secm_ref_date & EINDSECM >= cfg$secm_ref_date # entry that is still open on target date
         ) %>% 
  mutate(
    employed = as.integer(SECM %in% c(11, 12, 13, 14)),
    social.benefits = as.integer(SECM == 22),
    disability = as.integer(SECM == 24)
  ) %>%
  distinct() %>% # keep unique records
  select(-c(AANVSECM, EINDSECM, SECM))
  
cohort_dat <- left_join(cohort_dat, secm_tab)



#### HEALTH COSTS ####
health_tab <- 
  read_sav(file.path(loc$data_folder, loc$zvwzorgkosten_data),   
                     col_select =  c("RINPERSOONS", "RINPERSOON", "ZVWKFARMACIE", "ZVWKGENBASGGZ",        
                                      "ZVWKSPECGGZ", "ZVWKZIEKENHUIS", "ZVWKZIEKENVERVOER", "ZVWKEERSTELIJNSPSYCHO", 
                                      "ZVWKGERIATRISCH", "ZVWKOPHOOGFACTOR", "ZVWKGEBOORTEZORG", "ZVWKGGZ",              
                                      "ZVWKWYKVERPLEGING", "ZVWKHUISARTS", "ZVWKPARAMEDISCH", "ZVWKBUITENLAND",     
                                      "ZVWKHULPMIDDEL", "ZVWKOVERIG", "ZVWKMONDZORG")) %>% 
  mutate(RINPERSOONS = as_factor(RINPERSOONS)) 

health_tab <- 
  health_tab %>%
  mutate_at(names(health_tab %>% select(-c(RINPERSOONS, RINPERSOON))), 
            function(x) ifelse(x < 0, 0, x)  # replace negative values with 0
            ) %>%
  mutate(pharma.costs       = ifelse(ZVWKFARMACIE > 0, 1, 0),
         basis.ggz.costs    = ifelse(ZVWKGENBASGGZ > 0, 1, 0),
         specialist.costs   = ifelse(ZVWKSPECGGZ > 0, 1, 0),
         hospital.costs     = ifelse(ZVWKZIEKENHUIS > 0, 1, 0),
         total.health.costs = rowSums(health_tab %>% select(-c(RINPERSOONS, RINPERSOON))) # sum of all healthcare costs
         ) %>%
  select(RINPERSOONS, RINPERSOON, pharma.costs, basis.ggz.costs, specialist.costs, 
          hospital.costs, total.health.costs)
  

cohort_dat <- left_join(cohort_dat, health_tab)

# replace NA with 0 (for those who are not merged with the zvwzorgkostentab)
cohort_dat <- cohort_dat %>%
  mutate(pharma.costs       = ifelse(is.na(pharma.costs), 0, pharma.costs),
         basis.ggz.costs    = ifelse(is.na(basis.ggz.costs), 0, basis.ggz.costs),
         specialist.costs   = ifelse(is.na(specialist.costs), 0, specialist.costs),
         hospital.costs     = ifelse(is.na(hospital.costs), 0, hospital.costs),
         total.health.costs = ifelse(is.na(total.health.costs), 0, total.health.costs))
  
  
#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))