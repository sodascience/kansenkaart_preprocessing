# Kansenkaart data preparation pipeline
#
# 4. Post-processing.
#   - Selecting variables of interest.
#   - Writing `scratch/kansenkaart_data.rds`.
#
# (c) ODISSEI Social Data Science team 2022



# load cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "03_outcomes.rds"))



  
# save as cohort name
output_file <- file.path(loc$scratch_folder, paste0(cfg$cohort_name, "_cohort.rds"))
write_rds(cohort_dat, output_file)

