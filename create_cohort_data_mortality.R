# Kansenkaart data preparation pipeline
#
# child mortality cohort creation script
#
# (c) ODISSEI Social Data Science team 2021


# input the desired config file here:
cfg_file <- "config/child_mortality.yml"

#### CONFIGURATION ####
# load the configuration
cfg <- config::get("data_preparation", file = cfg_file)
loc <- config::get("file_locations",   file = cfg_file)

# create the scratch folder
if (!dir.exists(loc$scratch_folder)) dir.create(loc$scratch_folder)

#### RUN ####
# select cohort
source("src/child_mortality/01_cohort_child_mortality.R")

# add predictors
source("src/child_mortality/02_predictors_child_mortality.R")

# add outcomes
source("src/child_mortality/03f_outcomes_child_mortality.R")

# post-process
source("src/child_mortality/04_postprocessing_child_mortality.R")

# the pre-processed cohort data file is now available in the scratch folder!
