# Kansenkaart data preparation pipeline
#
# 4. Post-processing.
#   - Selecting variables of interest.
#   - Writing `scratch/kansenkaart_data.rds`.
#
# (c) ODISSEI Social Data Science team 2021

#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(readxl)

# load cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "03_outcomes.rds"))


cohort_dat <- 
  cohort_dat %>% 
  mutate(income_group = factor(case_when(
    (income_parents_perc >= .15 & income_parents_perc <= .35) ~ "Low", 
    (income_parents_perc >= .40 & income_parents_perc <= .60) ~ "Mid", 
    (income_parents_perc >= .65 & income_parents_perc <= .85) ~ "High",
    TRUE ~ NA_character_
  ), levels = c("Low", "Mid", "High"))) %>% 
  mutate(geslacht = fct_recode(as.factor(as.character(geslachtkind)), "Mannen" = "jongen", "Vrouwen" = "meisje"))


# rename columns
outcomes <- c("perinatal_mortality", "neonatal_mortality", 
              "infant_mortality")
suffix <- "c00_"

# rename outcomes
cohort_dat <- 
  cohort_dat %>%
  rename_with(~str_c(suffix, .), .cols = all_of(outcomes))


# rename gender variable
cohort_dat <- cohort_dat %>%
  rename(GBAGESLACHT = geslachtkind) %>%
  mutate(GBAGESLACHT = recode(GBAGESLACHT, "jongen" = "Mannen",
                              "meisje" = "Vrouwen"))



# save as cohort name
output_file <- file.path(loc$scratch_folder, paste0(cfg$cohort_name, "_cohort.rds"))
write_rds(cohort_dat, output_file)



