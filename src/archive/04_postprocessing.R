# Kansenkaart data preparation pipeline
#
# 4. Post-processing.
#   - Selecting variables of interest.
#   - Writing `scratch/kansenkaart_data.rds`.
#
# (c) ODISSEI Social Data Science team 2022



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
  mutate(geslacht = as.factor(as.character(GBAGESLACHT)),
         migration_third = as.factor(migration_third)) # EDIT EJ


# add c##_* to variable names
if (cfg$cohort_name == "main") {
  
  outcomes <- c("income", "hbo_attained", "wo_attained", "hourly_wage", "hrs_work_pw", 
            "permanent_contract", "has_worked", "hourly_wage_max_10", 
            "hourly_wage_max_11", "hourly_wage_max_14", "employed", "social_assistance", 
            "disability", "total_health_costs", "basic_mhc", "specialist_mhc", 
            "hospital", "pharma", "debt", "wealth", "home_owner")
  suffix <- "c30_"
 
} else if (cfg$cohort_name == "students")  {
  
  outcomes <- c("high_school_attained", "hbo_followed", "uni_followed")
  suffix <- "c21_"

} else if (cfg$cohort_name == "high_school")  {
  
  outcomes <- c("vmbo_gl", "havo", "vwo", 
                "youth_health_costs", "youth_protection", "living_space_pp")
  suffix <- "c16_"

} else if (cfg$cohort_name == "elementary_school")  {
  
  outcomes <- c("vmbo_gl_final", "havo_final", "vwo_final", 
                "vmbo_gl_test", "havo_test", "vwo_test",
                "over_advice", "under_advice", "math", "language", 
                "reading", "youth_health_costs", "youth_protection", 
                "living_space_pp")
  suffix <- "c11_"
  
} else if (cfg$cohort_name == "classroom")  {
  
  outcomes <- c("class_vmbo_gl_test", "class_havo_test", "class_vwo_test",
                "class_size", "class_foreign_born_parents", 
                "class_math", "class_language", "class_reading", 
                "class_income_below_25th", "class_income_above_75th")
  suffix <- "c11_"

} else if (cfg$cohort_name == "perinatal") {
    outcomes <- c("sga", "preterm_birth")
    suffix <- "c00_"

} 

# rename outcomes
cohort_dat <- 
  cohort_dat %>%
  rename_with(~str_c(suffix, .), .cols = all_of(outcomes)) %>% 
  ungroup() # EDIT EJ


  
# save as cohort name
output_file <- file.path(loc$scratch_folder, paste0(cfg$cohort_name, "_cohort.rds"))
write_rds(cohort_dat, output_file)

