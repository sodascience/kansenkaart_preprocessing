# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding students outcomes to the cohort.
#
# (c) ODISSEI Social Data Science team 2024


#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)


#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "02_predictors_lifecycle.rds"))



#### LIVE CONTINUOUSLY IN NL ####

# We only include children who live continuously in the Netherlands 
# children are only allowed to live up to 31 days not in the netherlands
adres_tab <- read_sav(file.path(loc$data_folder, loc$gbaao_data)) %>%
  # select only children
  filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>% 
  mutate(
    RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
    SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"),
    GBADATUMAANVANGADRESHOUDING = ymd(GBADATUMAANVANGADRESHOUDING),
    GBADATUMEINDEADRESHOUDING = ymd(GBADATUMEINDEADRESHOUDING)
  )

# add the year we use for residency requirement
child_adres_date <- 
  cohort_dat %>%
  mutate(adres_date = as.numeric(format(birthdate, "%Y")) + cfg$child_live_age)%>%
  select(RINPERSOONS, RINPERSOON, adres_date)


# remove addresses that end before or start after the residency year
adres_tab <- adres_tab %>%
  inner_join(child_adres_date, by = c("RINPERSOONS", "RINPERSOON")) %>%
  filter(!(format(GBADATUMEINDEADRESHOUDING, "%Y") < adres_date),
         !(format(GBADATUMAANVANGADRESHOUDING, "%Y") > adres_date))


# throw out anything with an end date before start_date, and anything with a start date after end_date
# then also set the start date of everything to start_date, and the end date of everything to end_date
# then compute the timespan of each record
adres_tab <- 
  adres_tab %>%
  mutate(
    start_date = dmy(paste0("0101", adres_date)),
    end_date = dmy(paste0("3112", adres_date)),
    cutoff_days = as.numeric(difftime(end_date, start_date, units = "days")) - 
      cfg$child_live_slack_days) %>%
  mutate(
    recordstart = as_date(ifelse(GBADATUMAANVANGADRESHOUDING < start_date, start_date, GBADATUMAANVANGADRESHOUDING)),
    recordend   = as_date(ifelse(GBADATUMEINDEADRESHOUDING > end_date, end_date, GBADATUMEINDEADRESHOUDING)),
    timespan    = difftime(recordend, recordstart, units = "days")
  )


# group by person and sum the total number of days
# then compute whether this person lived in the Netherlands continuously
days_tab <- 
  adres_tab %>% 
  select(RINPERSOONS, RINPERSOON, timespan, cutoff_days) %>% 
  mutate(timespan = as.numeric(timespan)) %>% 
  group_by(RINPERSOONS, RINPERSOON) %>% 
  summarize(total_days = sum(timespan),
            continuous_living = total_days >= cutoff_days) %>% 
  select(RINPERSOONS, RINPERSOON, continuous_living) %>%
  unique()


# add to cohort and filter
cohort_dat <- 
  left_join(cohort_dat, days_tab, by = c("RINPERSOONS", "RINPERSOON")) %>% 
  filter(continuous_living) %>% 
  select(-continuous_living)

rm(adres_tab, days_tab, child_adres_date)



#### EDUCATION ####
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


education_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(),
                        education_attained = factor(), education_followed = factor(), 
                        year = integer())
for (year in as.integer(cfg$education_year_max)) {
  
  if (year < 2019) {
    education_dat <- read_sav(get_hoogstopl_filename(year),
                              col_select = c("RINPERSOONS", "RINPERSOON", "OPLNIVSOI2016AGG4HGMETNIRWO", 
                                             "OPLNIVSOI2016AGG4HBMETNIRWO")) %>%
      rename(education_attained = OPLNIVSOI2016AGG4HBMETNIRWO, 
             education_followed = OPLNIVSOI2016AGG4HGMETNIRWO) %>%
      mutate(
        RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
        education_attained = ifelse(education_attained == "----", NA, education_attained), 
        education_followed = ifelse(education_followed == "----", NA, education_followed), 
        year = year
      ) %>%
      # add to data 
      bind_rows(education_dat, .)
    
  } else if (year >= 2019) {
    education_dat <- read_sav(get_hoogstopl_filename(year),
                              col_select = c("RINPERSOONS", "RINPERSOON", "OPLNIVSOI2021AGG4HBmetNIRWO", 
                                             "OPLNIVSOI2021AGG4HGmetNIRWO")) %>%
      rename(education_attained = OPLNIVSOI2021AGG4HBmetNIRWO, 
             education_followed = OPLNIVSOI2021AGG4HGmetNIRWO) %>%
      mutate(
        RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
        education_attained = ifelse(education_attained == "----", NA, education_attained), 
        education_followed = ifelse(education_followed == "----", NA, education_followed), 
        year = year
      ) %>%
      # add to data 
      bind_rows(education_dat, .)
    
  }
}



# join to cohort
cohort_dat <- 
  cohort_dat %>%
  left_join(education_dat, 
            by = c("RINPERSOONS", "RINPERSOON")) 


#### OUTCOMES ####

# create education outcomes
cohort_dat <- 
  cohort_dat %>%
  mutate(
    high_school_attained = ifelse(education_attained >= 2110, 1, 0),
    hbo_followed        = ifelse(education_followed >= 3110, 1, 0),
    uni_followed        = ifelse(education_followed %in% c(3113, 3212, 3213), 1, 0),
    
    high_school_attained = ifelse(is.na(high_school_attained), 0, high_school_attained),
    hbo_followed         = ifelse(is.na(hbo_followed), 0, hbo_followed),
    uni_followed         = ifelse(is.na(uni_followed), 0, uni_followed)
  ) %>%
  select(-c(education_attained, education_followed, year))

rm(education_dat)



#### PREFIX ####

# add prefix to outcomes
outcomes <- c("high_school_attained", "hbo_followed", "uni_followed")

suffix <- "c21_"


# rename outcomes
cohort_dat <- 
  cohort_dat %>%
  rename_with(~str_c(suffix, .), .cols = all_of(outcomes)) %>% 
  ungroup() %>%
  #remove parents birth country
  select(-c(GBAGEBOORTELANDMOEDER, GBAGEBOORTELANDVADER))


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes_lifecycle.rds"))

