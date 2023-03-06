# Kansenkaart data preparation pipeline - wealth




#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(RColorBrewer)


#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "01_cohort.rds"))


# create variable that reflects the year the child turned a specific age 
cohort_dat <- 
  cohort_dat %>%
  mutate(outcome_year = year(birthdate %m+% years(cfg$outcome_age)),
         birth_year = year(birthdate)) %>%
  ungroup()


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
  select(RINPERSOONMa, RINPERSOONSMa, RINPERSOONpa, RINPERSOONSpa)

parents <- c(cohort_dat$RINPERSOONMa, cohort_dat$RINPERSOONSMa,
             cohort_dat$RINPERSOONpa, cohort_dat$RINPERSOONSpa)

wealth_parents <- tibble(RINPERSOONSMa = factor(), RINPERSOONMa = character(),
                         RINPERSOONSpa = factor(), RINPERSOONpa = character(), 
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
    select(RINPERSOONSMa, RINPERSOONMa, RINPERSOONSpa, RINPERSOONpa, 
           wealth_parents, year) 
  
  wealth_parents <- bind_rows(wealth_parents, wealth_dat)
}
rm(veh_parents, koppel_hh, wealth_dat, parents_dat)


# Fig parental wealth rank stability 1
cohort_dat <- 
  cohort_dat %>% 
  mutate(age_25 = year(birthdate %m+% years(25))) 
  

# year add which the child turned 25 year
wealth_1_year <- 
  wealth_parents %>%
  inner_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, age_25 ,RINPERSOONMa, 
                                  RINPERSOONSMa, RINPERSOONpa, RINPERSOONSpa), 
             by = c("RINPERSOONMa" = "RINPERSOONMa", 
                    "RINPERSOONSMa" = "RINPERSOONSMa", 
                    "RINPERSOONpa" = "RINPERSOONpa", 
                    "RINPERSOONSpa" = "RINPERSOONSpa", 
                    "year" = "age_25")) %>%
  unique()


# keep parents wealth at child age 25
wealth_1_year <- 
  wealth_1_year %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  summarize(wealth_parents = sum(wealth_parents, na.rm = TRUE)) %>%
  inner_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, birth_year),
             by = c("RINPERSOONS", "RINPERSOON"))

# compute income transformations
wealth_1_year <- 
  wealth_1_year %>%
  group_by(birth_year) %>%
  mutate(
    wealth_parents_rank = rank(wealth_parents, ties.method = "average"),
    wealth_parents_perc = (wealth_parents_rank / max(wealth_parents_rank) * 100)
  ) %>%
  select(-wealth_parents_rank)


#############


# year child 25 to 29 years old
wealth_5_year <- 
  wealth_parents %>%
  left_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, age_25 ,RINPERSOONMa, 
                                   RINPERSOONSMa, RINPERSOONpa, RINPERSOONSpa), 
            by = c("RINPERSOONMa" = "RINPERSOONMa", 
                   "RINPERSOONSMa" = "RINPERSOONSMa", 
                   "RINPERSOONpa" = "RINPERSOONpa", 
                   "RINPERSOONSpa" = "RINPERSOONSpa")) %>%
  unique()


# keep children when children are age 25 to 29 
wealth_5_year <- 
  wealth_5_year %>%
  arrange(RINPERSOON) %>%
  mutate(age_25 = ifelse(year == age_25 | year == (age_25 + 1) | 
                           year == (age_25 + 2) | year == (age_25 + 3) | 
                           year == (age_25 + 4), age_25, NA)) %>%
  filter(!is.na(age_25)) %>%
  select(-age_25)   
    
# mean wealth parents
wealth_5_year <- 
  wealth_5_year %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  summarize(wealth_parents = mean(wealth_parents, na.rm = TRUE)) %>%
  inner_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, birth_year),
             by = c("RINPERSOONS", "RINPERSOON")) 


# compute income transformations
wealth_5_year <- 
  wealth_5_year %>%
  group_by(birth_year) %>%
  mutate(
    wealth_parents_rank = rank(wealth_parents, ties.method = "average"),
    wealth_parents_perc = (wealth_parents_rank / max(wealth_parents_rank) * 100)
  ) %>%
  select(-wealth_parents_rank)


#### CHILD INCOME ####


# get income data from each requested year into single data frame
get_ipi_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INTEGRAAL PERSOONLIJK INKOMEN/", year),
    pattern = paste0("PERSOONINK", year, "TABV[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}

get_inpa_filename <- function(year) {
  # function to get latest version of specified year
  fl <- list.files(
    path = file.path(loc$data_folder, "InkomenBestedingen/INPATAB/"),
    pattern = paste0("INPA", year, "TABV[0-9]+\\.sav"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


income_children <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                          income = double(), year = integer())
for (year in seq(as.integer(cfg$child_income_year_min), as.integer(cfg$child_income_year_max))) {
  if (year < 2011) {
    # use IPI tab
    income_children <- 
      # read file from disk
      read_sav(get_ipi_filename(year), col_select = c("RINPERSOONS", "RINPERSOON", "PERSBRUT")) %>% 
      rename(income = PERSBRUT) %>% 
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
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
      read_sav(get_inpa_filename(year), col_select = c("RINPERSOONS", "RINPERSOON", "INPPERSBRUT")) %>% 
      rename(income = INPPERSBRUT) %>% 
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
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
  mutate(income = ifelse(income == 9999999999, NA, income)) 

# deflate
income_children <- 
  income_children %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(income = income / (cpi / 100)) %>% 
  select(-cpi)


# add year variable at which the child is a specific age
# to compute the mean of child income in a period. 
income_children <-
  income_children %>%
  arrange(RINPERSOON) %>%
  left_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, outcome_year), 
            by = c("RINPERSOONS", "RINPERSOON"))


# compute the mean of the year the child is a specific age and the year before that
income_children <-
  income_children %>%
  arrange(RINPERSOON) %>%
  mutate(outcome_year = ifelse(year == (outcome_year - 1), (outcome_year - 1), outcome_year),
         outcome_year = ifelse(year == outcome_year, outcome_year, NA)) %>%
  filter(!is.na(outcome_year)) %>%
  select(-outcome_year)


# compute mean
income_children <- 
  income_children %>% 
  group_by(RINPERSOONS, RINPERSOON) %>% 
  summarize(income_n = sum(!is.na(income)),
            income = mean(income, na.rm = TRUE))

# table of the number of years the mean income is based on
print(table(`income years` = income_children$income_n))



# add to data
wealth_1_year <- 
  left_join(wealth_1_year, income_children %>% select(-income_n),
            by = c("RINPERSOONS", "RINPERSOON"))

wealth_5_year <- 
  left_join(wealth_5_year, income_children %>% select(-income_n),
            by = c("RINPERSOONS", "RINPERSOON"))


# free up memory
rm(income_children)


# if income is negative then income becomes NA
wealth_5_year <- 
  wealth_5_year %>%
  mutate(income = ifelse(income < 0, NA, income)) %>%
  # remove income if income is NA or nan
  filter(!is.na(income) | !is.nan(income))


# compute income transformations
wealth_5_year <- 
  wealth_5_year %>% 
  ungroup() %>%
  group_by(birth_year) %>%
  mutate(
    income_rank = rank(income,  ties.method = "average"),
    income_perc = income_rank / max(income_rank)
  ) %>%
  select(-income_rank) %>%
  ungroup()


# if income is negative then income becomes NA
wealth_1_year <- 
  wealth_1_year %>%
  mutate(income = ifelse(income < 0, NA, income)) %>%
  # remove income if income is NA or nan
  filter(!is.na(income) | !is.nan(income))


# compute income transformations
wealth_1_year <- 
  wealth_1_year %>% 
  ungroup() %>%
  group_by(birth_year) %>%
  mutate(
    income_rank = rank(income,  ties.method = "average"),
    income_perc = income_rank / max(income_rank)
  ) %>%
  select(-income_rank) %>%
  ungroup()


################################  
# Fig parental wealth rank stability 1

bins_1_year <- 
  wealth_1_year %>%
  select(RINPERSOONS, RINPERSOON, wealth_parents_perc,
         wealth_parents, income_perc) %>%
  ungroup() %>%
  mutate(wealth_parents_perc = ceiling(wealth_parents_perc)) 


bins_1_year <-
  bins_1_year %>%
    group_by(wealth_parents_perc) %>%
    summarize(income1 = mean(income_perc, na.rm = TRUE),
              income1_p5 = quantile(income_perc, na.rm = TRUE, c(0.05)), 
              income1_p95 = quantile(income_perc, na.rm = TRUE, c(0.95)), 
              wealth_parents1 = mean(wealth_parents, na.rm = TRUE))


bins_5_year <- 
  wealth_5_year %>%
  select(RINPERSOONS, RINPERSOON, wealth_parents_perc,
         wealth_parents, income_perc) %>%
  ungroup() %>%
  mutate(wealth_parents_perc = ceiling(wealth_parents_perc)) 


bins_5_year <-
  bins_5_year %>%
  group_by(wealth_parents_perc) %>%
  summarize(income5 = mean(income_perc, na.rm = TRUE),
            income5_p5 = quantile(income_perc, na.rm = TRUE, c(0.05)), 
            income5_p95 = quantile(income_perc, na.rm = TRUE, c(0.95)), 
            wealth_parents5 = mean(wealth_parents, na.rm = TRUE))
  


fig_dat <- inner_join(bins_1_year, bins_5_year, 
                      by = "wealth_parents_perc")



ggplot(fig_dat[1:99, ]) +
  geom_point(aes(x =wealth_parents_perc, y = income1, 
                 color = "1 year"), size = 3,) +
  geom_point(aes(x = wealth_parents_perc, y = income5, 
                 color = "5 year"),  size = 3) + 
  geom_abline(slope = 1, yintercept = 0) +
  scale_color_brewer("Wealth parents", palette = "Set1") +
  scale_y_continuous(limits = c(0, 100)) +
  ggtitle("Parental wealth rank stability 1 (1 year vs. 5 years)") +
  labs(x = "Parental wealth ranks", 
       y = "Child income ranks") +
  theme_minimal()




################################  

wealth_1_year <- 
  wealth_1_year %>%
  ungroup() %>%
  mutate(income_perc = income_perc * 100) 


## 1 year
# 10e 
new_wealth <- data.frame(wealth_parents_perc = 10)
tab <- bind_cols(data = "year 1 wealth at 10e percentile", 
  predict(lm(income_perc ~ wealth_parents_perc, 
           data = subset(wealth_1_year, 
                         wealth_1_year$wealth_parents_perc >=0 & 
                           wealth_1_year$wealth_parents_perc <=20), 
           na.action = na.omit), 
        newdata = new_wealth, interval = "confidence"))



# 90e
new_wealth <- data.frame(wealth_parents_perc = 90)
tab <- bind_rows(tab, 
  bind_cols(data = "year 1 wealth at 90e percentile", 
            predict(lm(income_perc ~ wealth_parents_perc, 
                       data = subset(wealth_1_year, 
                                     wealth_1_year$wealth_parents_perc >=80 & 
                                       wealth_1_year$wealth_parents_perc <=100), 
                       na.action = na.omit), 
                    newdata = new_wealth, interval = "confidence")))


## 5 year

wealth_5_year <- 
  wealth_5_year %>%
  ungroup() %>%
  mutate(income_perc = income_perc * 100) 


# 10e 
new_wealth <- data.frame(wealth_parents_perc = 10)
tab <- bind_rows(tab, 
                 bind_cols(data = "year 5 wealth at 10e percentile", 
                           predict(lm(income_perc ~ wealth_parents_perc, 
                                      data = subset(wealth_5_year, 
                                                    wealth_5_year$wealth_parents_perc >=0 & 
                                                      wealth_5_year$wealth_parents_perc <=20), 
                                      na.action = na.omit), 
                                   newdata = new_wealth, interval = "confidence")))

# 90e
new_wealth <- data.frame(wealth_parents_perc = 90)
tab <- bind_rows(tab, 
                 bind_cols(data = "year 5 wealth at 90e percentile", 
                           predict(lm(income_perc ~ wealth_parents_perc, 
                                      data = subset(wealth_5_year, 
                                                    wealth_5_year$wealth_parents_perc >=80 & 
                                                      wealth_5_year$wealth_parents_perc <=100), 
                                      na.action = na.omit), 
                                   newdata = new_wealth, interval = "confidence")))




