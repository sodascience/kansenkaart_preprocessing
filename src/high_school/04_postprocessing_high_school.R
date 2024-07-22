# Kansenkaart data preparation pipeline
#
# 4. Post-processing.
#   - Selecting variables of interest.
#   - Writing `scratch/kansenkaart_data.rds`.
#
# (c) ODISSEI Social Data Science team 2024



# load cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "03_outcomes.rds"))



#### INCOME GROUP ####
cohort_dat <- 
  cohort_dat %>% 
  mutate(income_group = factor(case_when(
    (income_parents_perc >= .15 & income_parents_perc <= .35) ~ "Low", 
    (income_parents_perc >= .40 & income_parents_perc <= .60) ~ "Mid", 
    (income_parents_perc >= .65 & income_parents_perc <= .85) ~ "High",
    TRUE ~ NA_character_
  ), levels = c("Low", "Mid", "High"))) 


#### WEALTH GROUP ####
cohort_dat <- 
  cohort_dat %>% 
  mutate(wealth_group = factor(case_when(
    (wealth_parents_perc >= .15 & wealth_parents_perc <= .35) ~ "Low", 
    (wealth_parents_perc >= .40 & wealth_parents_perc <= .60) ~ "Mid", 
    (wealth_parents_perc >= .65 & wealth_parents_perc <= .85) ~ "High",
    TRUE ~ NA_character_
  ), levels = c("Low", "Mid", "High"))) 


# save as cohort name
output_file <- file.path(loc$scratch_folder, paste0(cfg$cohort_name, "_cohort.rds"))
write_rds(cohort_dat, output_file)

