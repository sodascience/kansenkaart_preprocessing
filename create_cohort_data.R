# Kansenkaart data preparation pipeline
#
# Full cohort creation script
#
# (c) ODISSEI Social Data Science team 2023


# input the desired config file here:
# yml: main, students, high_school, elementary_school, classroom, 
# perinatal, child_mortality
cohort <- "main"
cfg_file <- paste0("config/", cohort, ".yml")


#### CONFIGURATION ####
# load the configuration
cfg <- config::get("data_preparation", file = cfg_file)
loc <- config::get("file_locations",   file = cfg_file)

# create the scratch folder
if (!dir.exists(loc$scratch_folder)) dir.create(loc$scratch_folder)


#### RUN ####
# select cohort
source(list.files(file.path("src", cfg$cohort_name), "01_", full.names = TRUE))

# add predictors
source(list.files(file.path("src", cfg$cohort_name), "02_", full.names = TRUE))

# add outcomes
source(list.files(file.path("src", cfg$cohort_name), "03_", full.names = TRUE))

# post-process
source(list.files(file.path("src", cfg$cohort_name), "04_", full.names = TRUE))

# the pre-processed cohort data file is now available in the scratch folder!
