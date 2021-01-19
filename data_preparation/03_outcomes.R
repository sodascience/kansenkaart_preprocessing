# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding (socio)economic outcomes to the cohort.
#   - Adding education outcomes to the cohort.
#   - Adding health outcomes to the cohort.
#   - Writing `scratch/03_outcomes.rds`.
#
# (c) ODISSEI Social Data Science team 2021


##########################
#### OUTCOME FEATURES ####
##########################

#### CHILD INCOME 2017-2018 ####

# create income table for children
inpa_path_2017 <- file.path(loc$data_folder, "InkomenBestedingen/INPATAB/INPA2017TABV3.sav")
inpa_path_2018 <- file.path(loc$data_folder, "InkomenBestedingen/INPATAB/INPA2018TABV2.sav")
inpa_cohort <- bind_rows(
  read_sav(inpa_path_2017) %>% mutate(year = 2017),
  read_sav(inpa_path_2018) %>% mutate(year = 2018),
)

# deflate
inpa_cohort <- 
  inpa_cohort %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(INPPERSBRUT = INPPERSBRUT / (cpi / 100)) %>% 
  select(-cpi)

# compute mean
inpa_cohort <- 
  inpa_cohort %>% 
  group_by(RINPERSOON) %>% 
  summarize(income = mean(INPPERSBRUT))

# add to cohort and free up memory
cohort_dat <- left_join(cohort_dat, inpa_cohort)
rm(inpa_cohort)
