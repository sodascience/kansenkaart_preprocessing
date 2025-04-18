# Kansenkaart data preparation pipeline
#
# Full cohort creation script
#
# (c) ODISSEI Social Data Science team 2025


# input the desired config file here:
# yml: age35, age21, age16, prim8, newborns
cohort <- "age35"
cfg_file <- paste0("config/", cohort, ".yml")

excel_path <- 'H:/IGM project/Excel/output'


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
