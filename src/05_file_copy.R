# Kansenkaart data preparation pipeline
#
# Post-processing 
# - move final cohorts to data analysis
#
# (c) ODISSEI Social Data Science team 2024



# clean workspace
rm(list=ls())

#### PACKAGES ####
library(tidyverse)
library(haven)


# path to move
data_path <- "H:/IGM project/kansenkaart_preprocessing/scratch"
# analysis_path <- "H:/IGM project/KCO Dashboard/Data/Cohorts"
analysis_path <- "H:/IGM project/kansenkaart_analysis/raw_data"


#### COHORT ####

for (cohort in c("main", "students", "high_school", "elementary_school",
                 "perinatal", "child_mortality")) {
  
  file_path <- file.path(data_path, cohort, paste0(cohort, "_cohort.rds"))
  file.copy(from = file_path, to = analysis_path,  overwrite = TRUE)
  

}



# test whether columns are in the data
# test <- read_rds(file_path)
# if (all(c("income_group", "geslacht",
#           "migration_third", "income_parents_perc") %in% colnames(test))) {
#   print("Yes!")
# } else {
#   print("No!")
# }

