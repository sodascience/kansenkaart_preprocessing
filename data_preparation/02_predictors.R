# Kansenkaart data preparation pipeline
#
# 2. Predictor creation.
#    - Adding parent income and income percentile to the cohort.
#    - Adding ethnicity information to the cohort.
#    - Adding gender information to the cohort.
#    - Writing `scratch/02_predictor.rds`
#
# (c) ODISSEI Social Data Science team 2021

#### PARENT INCOME 2006-2010 ####
# create a table with incomes at the 2018€ level
# first, load consumer price index data
# source: https://opendata.cbs.nl/statline/#/CBS/nl/dataset/83131NED/table?ts=1610019128426
cpi_tab <- 
  read_delim("resources/Consumentenprijzen__prijsindex_2015_100_07012021_123946.csv", ";", skip = 5, 
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

# create income table for parents
inpa_path_2006 <- file.path(loc$data_folder, "InkomenBestedingen/INPATAB/INPA2006TABV3.sav")
inpa_path_2007 <- file.path(loc$data_folder, "InkomenBestedingen/INPATAB/INPA2007TABV3.sav")
inpa_path_2008 <- file.path(loc$data_folder, "InkomenBestedingen/INPATAB/INPA2008TABV3.sav")
inpa_path_2009 <- file.path(loc$data_folder, "InkomenBestedingen/INPATAB/INPA2009TABV2.sav")
inpa_path_2010 <- file.path(loc$data_folder, "InkomenBestedingen/INPATAB/INPA2010TABV3.sav")
inpa_parents <- bind_rows(
  read_sav(inpa_path_2006) %>% mutate(year = 2006),
  read_sav(inpa_path_2007) %>% mutate(year = 2007),
  read_sav(inpa_path_2008) %>% mutate(year = 2008),
  read_sav(inpa_path_2009) %>% mutate(year = 2009),
  read_sav(inpa_path_2010) %>% mutate(year = 2010),
)

# clean up
inpa_parents <-
  inpa_parents %>% 
  mutate(INPPERSBRUT = ifelse(INPPERSBRUT < 0, NA, INPPERSBRUT)) # remove negative incomes

# deflate
inpa_parents <- 
  inpa_parents %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(INPPERSBRUT = INPPERSBRUT / (cpi / 100)) %>% 
  select(-cpi)

# compute mean
inpa_parents <- 
  inpa_parents %>% 
  group_by(RINPERSOON) %>% 
  summarize(income = mean(INPPERSBRUT, na.rm = TRUE))

# add to data
cohort_dat <- left_join(cohort_dat, inpa_parents %>% rename(income_ma = income), by = c("RINPERSOONMa" = "RINPERSOON"))
cohort_dat <- left_join(cohort_dat, inpa_parents %>% rename(income_pa = income), by = c("RINPERSOONpa" = "RINPERSOON"))
cohort_dat <- cohort_dat %>% mutate(income_parents = mean(c(income_ma, income_pa), na.rm = TRUE))

# free up memory
rm(inpa_parents)
