# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding students outcomes to the cohort.
#
# (c) ODISSEI Social Data Science team 2025




#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)


#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "02_predictors.rds"))

sample_size <- read_rds(file.path(loc$scratch_folder, "02_sample_size.rds"))


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
for (year in seq(as.integer(cfg$education_year_min), as.integer(cfg$education_year_max))) {
  
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


# add year at which the child is a specific age to the cohort
cohort_dat <- 
  cohort_dat %>%
  mutate(
    year = year(birthdate) + cfg$outcome_age
  )

# join to cohort
cohort_dat <- 
  cohort_dat %>%
  left_join(education_dat, 
            by = c("RINPERSOONS", "RINPERSOON", "year")) 

# record sample size
sample_size <- sample_size %>% 
  mutate(n_5_child_residency = nrow(cohort_dat))
    

     
#### LIVING AT HOME ####
household_dat <-
  read_sav(file.path(loc$data_folder, loc$household_data),
           col_select = c("RINPERSOONS", "RINPERSOON", "DATUMAANVANGHH",
                          "DATUMEINDEHH", "PLHH")) %>%
  filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>%
  mutate(
    RINPERSOONS = as_factor(RINPERSOONS, levels = "value"),
    DATUMAANVANGHH = as.numeric(DATUMAANVANGHH),
    DATUMEINDEHH = as.numeric(DATUMEINDEHH),
    PLHH = as_factor(PLHH, levels = "value")
  ) %>%
  mutate(
    DATUMAANVANGHH = ymd(DATUMAANVANGHH),
    DATUMEINDEHH = ymd(DATUMEINDEHH)
  )                                                                                 

# take date at which the child is a specific age
age_tab <- 
  cohort_dat %>%
  select(RINPERSOONS, RINPERSOON, birthdate) %>%
  mutate(home_address_date = birthdate %m+% years(cfg$outcome_age)) %>%
  select(-birthdate)

# take household data at which child is a specific age
hh_tab <- 
  household_dat %>%
  left_join(age_tab, by = c("RINPERSOONS", "RINPERSOON")) %>%
  group_by(RINPERSOONS, RINPERSOON) %>% 
  filter(DATUMAANVANGHH <= home_address_date & 
           DATUMEINDEHH >= home_address_date) %>%
  summarize(PLHH = PLHH[1]) 

hh_tab_missing <-
  household_dat %>%
  left_join(age_tab, by = c("RINPERSOONS", "RINPERSOON")) %>%
  mutate(pl_missing = if_else((RINPERSOON %in% hh_tab$RINPERSOON), 0, 1)) %>%
  filter (pl_missing == 1) %>% 
  group_by(RINPERSOONS, RINPERSOON) %>%
  filter(DATUMAANVANGHH > home_address_date) %>%
  summarize(
    # date_21 is date we take the address of the child, it is the first 
    # available registered address of a child after their 21st birthday
    PLHH = PLHH[1],
    birthday = home_address_date[1],
    date_21 = DATUMAANVANGHH[1]) %>%
  mutate(missing = if_else((date_21 - birthday > 365), 1, 0)) %>%
  filter(missing == 0) %>%
  select(RINPERSOON, RINPERSOONS, PLHH)

hh_tab <- rbind (hh_tab, hh_tab_missing)

# free up memory 
rm(hh_tab_missing,age_tab, household_dat)

cohort_dat <- cohort_dat %>%
  inner_join (hh_tab)

rm(hh_tab)



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

# create living at home outcome
cohort_dat <- 
  cohort_dat %>%
  mutate (PLHH = as.numeric(PLHH)) %>%
  mutate (living_with_parents = if_else(PLHH == 1, 1, 0)) %>%
  select (-PLHH)


#### YOUNG PARENTS ####


# load kinderoudertab 
kindouder_dat <- read_sav(file.path(loc$data_folder, loc$kind_data),
                          col_select = -c("XKOPPELNUMMER")) %>%
  as_factor(only_labelled = TRUE, levels = "values") %>%
  filter((RINPERSOONMa %in% cohort_dat$RINPERSOON | RINPERSOONpa %in% cohort_dat$RINPERSOON),
         RINPERSOONS == "R", RINPERSOONSMa == "R", RINPERSOONSpa == "R") %>%
  rename(RINPERSOONS_infant = RINPERSOONS,
         RINPERSOON_infant = RINPERSOON) 

# load gba data
# voormalig birth_dat, changed to gba_dat to match cohort creation 
gba_path <- file.path(loc$data_folder, loc$gba_dat)
gba_dat <-
  read_sav(gba_path, col_select = c("RINPERSOONS", "RINPERSOON", "GBAGEBOORTEJAAR",
                                    "GBAGEBOORTEMAAND", "GBAGEBOORTEDAG")) %>%
  mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
         birthdate = dmy(paste(GBAGEBOORTEDAG, GBAGEBOORTEMAAND, GBAGEBOORTEJAAR, sep = "-"))) %>%
  select(-c(GBAGEBOORTEJAAR, GBAGEBOORTEMAAND, GBAGEBOORTEDAG))


# add infants birth date to cohort
kindouder_dat <-
  kindouder_dat %>%
  left_join(gba_dat, by = c("RINPERSOONS_infant" = "RINPERSOONS",
                            "RINPERSOON_infant" = "RINPERSOON")) %>%
  rename(birthdate_infant = birthdate) %>%
  select(-c(RINPERSOONS_infant, RINPERSOON_infant))

# add parental birth date
kindouder_dat <-
  kindouder_dat %>%
  left_join(gba_dat, by = c("RINPERSOONSMa" = "RINPERSOONS",
                            "RINPERSOONMa" = "RINPERSOON")) %>%
  rename(birthdate_ma = birthdate) %>%
  left_join(gba_dat, by = c("RINPERSOONSpa" = "RINPERSOONS",
                            "RINPERSOONpa" = "RINPERSOON")) %>%
  rename(birthdate_pa = birthdate)

# create one long df for each individual parent, their birthday and the birthdate of their child.
kindvader_dat <- 
  kindouder_dat %>% 
  select(RINPERSOONSpa, RINPERSOONpa, birthdate_pa, birthdate_infant) %>% 
  rename(RINPERSOONS = RINPERSOONSpa, RINPERSOON = RINPERSOONpa, 
         birthdate_parent = birthdate_pa)
kindmoeder_dat <- 
  kindouder_dat %>%
  select(RINPERSOONSMa, RINPERSOONMa, birthdate_ma, birthdate_infant) %>% 
  rename(RINPERSOONS = RINPERSOONSMa, RINPERSOON = RINPERSOONMa, 
         birthdate_parent = birthdate_ma)
kindouder_dat <- rbind(kindvader_dat, kindmoeder_dat)

# determine age at which person became a parent
kindouder_dat <-
  kindouder_dat %>%
  mutate(age_at_birth_parent = interval(birthdate_parent, birthdate_infant) / years(1)) %>%
  select(-c(birthdate_infant, birthdate_parent)) %>%
  arrange(RINPERSOON, age_at_birth_parent) %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  summarize(age_at_birth_parent = age_at_birth_parent[1])


# create young parent outcome and change to 0 if NA
cohort_dat <- cohort_dat %>%
  left_join(kindouder_dat, by = c("RINPERSOONS", "RINPERSOON")) %>% 
  mutate(young_parents = ifelse(age_at_birth_parent < 20, 1, 0),
         young_parents = ifelse(is.na(young_parents), 0, young_parents)) %>% 
  select(-age_at_birth_parent)

rm(gba_dat, kindouder_dat, kindvader_dat, kindmoeder_dat)


# #### PRIMARY SCHOOL CLASS COMPOSITION ####
# # load class cohort data
# class_cohort_dat <- read_rds(file.path(loc$scratch_folder, "class_cohort.rds"))
# 
# # combine the main sample and class sample
# class_cohort_dat <- bind_rows(
#   class_cohort_dat %>% select ("RINPERSOON", "RINPERSOONS", "GBAGEBOORTELANDMOEDER", "GBAGEBOORTELANDVADER", "income_parents_perc"),
#   cohort_dat %>% select ("RINPERSOON", "RINPERSOONS", "GBAGEBOORTELANDMOEDER", "GBAGEBOORTELANDVADER", "income_parents_perc"),
# )
# 
# 
# # function to get latest inschrwpo version of specified year
# get_inschrwpo_filename <- function(year) {
#   fl <- list.files(
#     path = file.path(loc$data_folder, "Onderwijs/INSCHRWPOTAB"),
#     pattern = paste0("INSCHRWPOTAB", year, "V[0-9]+(?i)(.sav)"),
#     full.names = TRUE
#   )
#   # return only the latest version
#   sort(fl, decreasing = TRUE)[1]
# }
# 
# school_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), WPOLEERJAAR = character(),
#                      WPOBRIN_crypt = character(), WPOBRINVEST = character(), WPOTYPEPO = character())
# 
# for (year in seq(as.integer(cfg$primary_classroom_year_min), as.integer(cfg$primary_classroom_year_max))) {
#   school_dat <-
#     # read file from disk
#     read_sav(get_inschrwpo_filename(year),
#              col_select = c("RINPERSOONS", "RINPERSOON", "WPOLEERJAAR",
#                             "WPOBRIN_crypt", "WPOBRINVEST", "WPOTYPEPO")) %>%
#     mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
#     # add year
#     mutate(year = year) %>%
#     # add to income children
#     bind_rows(school_dat, .)
# }
# 
# # keep group 8 pupils
# school_dat <- school_dat %>%
#   filter(RINPERSOONS == "R") %>%
#   mutate(WPOLEERJAAR = trimws(as.character(WPOLEERJAAR))) %>%
#   filter(WPOLEERJAAR == "8") %>%
#   select(-WPOLEERJAAR)
# 
# 
# # link to classroom sample
# school_dat <- school_dat %>%
#   left_join(class_cohort_dat,
#             by = c("RINPERSOON", "RINPERSOONS"))%>%
#   # drop all children who are not in the large classroom sample
#   filter(!is.na(income_parents_perc))
# 
# # primary school ID
# school_dat <- school_dat %>%
#   mutate(across(c("WPOBRIN_crypt", "WPOBRINVEST"), as.character)) %>%
#   mutate(school_ID = paste0(WPOBRIN_crypt, WPOBRINVEST)) %>%
#   select(-c(WPOBRIN_crypt, WPOBRINVEST))
# 
# 
# # create parents rank income outcomes
# school_dat <- school_dat %>%
#   mutate(
#     # create dummy for below 25th
#     income_below_25th = ifelse(income_parents_perc < 0.25, 1, 0),
#     # create dummy for below 50th
#     income_below_50th = ifelse(income_parents_perc < 0.50, 1, 0),
#     # create dummy for above 75th
#     income_above_75th = ifelse(income_parents_perc > 0.75, 1, 0)
#   )
# 
# 
# # create outcome for children with both parents born in a foreign country
# school_dat <- school_dat %>%
#   mutate(
#     GBAGEBOORTELANDMOEDER = as_factor(GBAGEBOORTELANDMOEDER),
#     GBAGEBOORTELANDVADER = as_factor(GBAGEBOORTELANDVADER)
#   ) %>%
#   mutate(
#     foreign_born_parents =
#       ifelse((GBAGEBOORTELANDMOEDER != "Nederland" &
#                 GBAGEBOORTELANDVADER != "Nederland"),  1, 0))
# 
# 
# # classroom outcomes: class_foreign_born_parents, class_parents_below_25, class_parents_below_50, class_parents_above_75
# 
# # only keep classes with more than one student per class
# school_dat <- school_dat %>%
#   group_by(school_ID, year) %>%
#   mutate(n = n()) %>%
#   filter(n > 1)
# 
# 
# # hold out mean function
# hold_out_means <- function(x) {
#   hold <- ((sum(x, na.rm = TRUE) - x) / (length(x) - 1))
#   return(hold)
# }
# 
# 
# 
# # hold out means = mean of the class without the child him/herself
# school_dat <- school_dat %>%
#   group_by(school_ID, year) %>%
#   mutate(
#     #    primary_N_students_per_school = n(),
#     primary_class_foreign_born_parents = hold_out_means(foreign_born_parents),
#     primary_class_income_below_25th = hold_out_means(income_below_25th),
#     primary_class_income_below_50th = hold_out_means(income_below_50th),
#     primary_class_income_above_75th = hold_out_means(income_above_75th)
#   )
# 
# 
# # keep unique observations,for duplicates select the last time the child is in 8th grade
# school_dat <- school_dat %>%
#   arrange(desc(year)) %>%
#   group_by(RINPERSOONS, RINPERSOON) %>%
#   filter(row_number() == 1)
# 
# # add to outcomes to cohort
# cohort_dat <- cohort_dat %>%
#   left_join (school_dat %>%
#                 select(RINPERSOONS, RINPERSOON, primary_class_foreign_born_parents, primary_class_income_below_25th, primary_class_income_below_50th, primary_class_income_above_75th),
#               by = c("RINPERSOONS", "RINPERSOON"))
# 
# rm(school_dat)
# 


#### SECONDARY SCHOOL CLASS COMPOSITION ####
#load class cohort data
class_cohort_dat <- read_rds(file.path(loc$scratch_folder, "class_cohort.rds"))

# combine the main sample and class sample
class_cohort_dat <- bind_rows(
  class_cohort_dat %>% select ("RINPERSOON", "RINPERSOONS", "GBAGEBOORTELANDMOEDER", "GBAGEBOORTELANDVADER", "income_parents_perc"),
  cohort_dat %>% select ("RINPERSOON", "RINPERSOONS", "GBAGEBOORTELANDMOEDER", "GBAGEBOORTELANDVADER", "income_parents_perc"),
)

# function to get latest ONDERWIJSINSCHRTAB version of specified year
get_school_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "Onderwijs/ONDERWIJSINSCHRTAB"),
    pattern = paste0("ONDERWIJSINSCHRTAB", year, "V[0-9]+(?i)(.sav)"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


school_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), OPLNR = character(), VOLEERJAAR = character(), 
                     BRIN_crypt = character(), VOBRINVEST = character())

for (year in seq(as.integer(cfg$secondary_classroom_year_min),as.integer(cfg$secondary_classroom_year_max))) {
  school_dat <- 
    # read file from disk
    read_sav(get_school_filename(year), 
             col_select = c("RINPERSOONS", "RINPERSOON","OPLNR", "VOLEERJAAR", 
                            "BRIN_crypt", "VOBRINVEST")) %>% 
    mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
    # add year
    mutate(year = year) %>% 
    # add to income children
    bind_rows(school_dat, .)
}

# keep those in grade 4 of secondary school 
school_dat <- school_dat %>%
  filter(VOLEERJAAR == "4", 
         RINPERSOONS == "R",
         !is.na(RINPERSOON)) 

# # find the level of secondary school 
# school_level <- read_sav(loc$opleiding_data) %>% 
#   select(OPLNR, ONDERWIJSSOORTVO)
# 
# school_dat <- school_dat %>%
#   left_join(school_level, by = "OPLNR") %>%
#   rename(school_level = ONDERWIJSSOORTVO )


# link to classroom sample
school_dat <- school_dat %>%
  left_join(class_cohort_dat, 
            by = c("RINPERSOON", "RINPERSOONS"))%>%
  # drop all children who are not in the classroom sample 
  filter(!is.na(income_parents_perc))


# generate secondary school ID
school_dat <- school_dat %>%
  mutate(across(c("BRIN_crypt", "VOBRINVEST"), as.character)) %>%
  mutate(school_ID = paste0(BRIN_crypt, VOBRINVEST)) %>%
  select(-c(BRIN_crypt, VOBRINVEST))



# create parents rank income outcomes
school_dat <- school_dat %>%
  mutate(
    # create dummy for below 25th 
    income_below_25th = ifelse(income_parents_perc < 0.25, 1, 0),
    # create dummy for below 50th 
    income_below_50th = ifelse(income_parents_perc < 0.50, 1, 0),
    # create dummy for above 75th
    income_above_75th = ifelse(income_parents_perc > 0.75, 1, 0)
  )


# create outcome for children with both parents born in a foreign country
school_dat <- school_dat %>%
  mutate(
    GBAGEBOORTELANDMOEDER = as_factor(GBAGEBOORTELANDMOEDER),
    GBAGEBOORTELANDVADER = as_factor(GBAGEBOORTELANDVADER)
  ) %>%
  mutate(
    foreign_born_parents = 
      ifelse((GBAGEBOORTELANDMOEDER != "Nederland" & 
                GBAGEBOORTELANDVADER != "Nederland"),  1, 0))


# classroom outcomes: class_foreign_born_parents, class_parents_below_25, class_parents_below_50, class_parents_above_75

#only keep classes with more than one student per class
school_dat <- school_dat %>%
  group_by(school_ID, year) %>%
  mutate(n = n()) %>%
  filter(n > 1)

# hold out mean function
hold_out_means <- function(x) {
  hold <- ((sum(x, na.rm = TRUE) - x) / (length(x) - 1))
  return(hold)
}

# hold out means = mean of the class without the child him/herself
school_dat <- school_dat %>%
  group_by(school_ID, year) %>%
  mutate(
    #    secondary_class_N_students_per_school = n(),
    secondary_class_foreign_born_parents = hold_out_means(foreign_born_parents),
    secondary_class_income_below_25th = hold_out_means(income_below_25th),
    secondary_class_income_below_50th = hold_out_means(income_below_50th),
    secondary_class_income_above_75th = hold_out_means(income_above_75th)
  ) 


# keep unique observations,for duplicates select the last time the child is in 8th grade
school_dat <- school_dat %>%
  arrange(desc(year)) %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  filter(row_number() == 1)



# add to outcomes to cohort
cohort_dat <- cohort_dat %>%
  left_join (school_dat %>% 
               select(RINPERSOONS, RINPERSOON, secondary_class_foreign_born_parents, secondary_class_income_below_25th, secondary_class_income_below_50th, secondary_class_income_above_75th),
             by = c("RINPERSOONS", "RINPERSOON"))

rm(school_dat, class_cohort_dat)



#### PREFIX ####

# add prefix to outcomes
outcomes <- c("high_school_attained", "hbo_followed", "uni_followed", 
              "living_with_parents", "young_parents",
              # "primary_class_foreign_born_parents", "primary_class_income_below_25th", "primary_class_income_below_50th", "primary_class_income_above_75th",
              "secondary_class_foreign_born_parents", "secondary_class_income_below_25th", "secondary_class_income_below_50th", "secondary_class_income_above_75th")

suffix <- "c21_"


# rename outcomes
cohort_dat <- 
  cohort_dat %>%
  rename_with(~str_c(suffix, .), .cols = all_of(outcomes)) %>% 
  ungroup() %>%
  #remove parents birth country
  select(-c(GBAGEBOORTELANDMOEDER, GBAGEBOORTELANDVADER))

# record sample size
sample_size <- sample_size %>% 
  mutate(n_6_child_outcomes = nrow(cohort_dat))


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))

#write sample size reduction table to scratch
sample_size <- sample_size %>% mutate(cohort_name = cohort)
write_rds(sample_size, file.path(loc$scratch_folder, "03_sample_size.rds"))
