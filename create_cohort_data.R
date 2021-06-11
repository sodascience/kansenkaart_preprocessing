# Kansenkaart data preparation pipeline
#
# Full cohort creation script
#
# (c) ODISSEI Social Data Science team 2021

# input the desired config file here:

cfg_file <- "config/elementary_school.yml"

#### CONFIGURATION ####
# load the configuration
cfg <- config::get("data_preparation", file = cfg_file)
loc <- config::get("file_locations",   file = cfg_file)

# create the scratch folder
if (!dir.exists(loc$scratch_folder)) dir.create(loc$scratch_folder)

#### RUN ####
# select cohort
source("src/01_cohort.R")

# add predictors
source("src/02_predictors.R")

# add outcomes
source(list.files("src", cfg$cohort_name, full.names = TRUE))

# post-process
source("src/04_postprocessing.R")

# the pre-processed cohort data file is now available in the scratch folder!
