# Kansenkaart data preparation pipeline
#
# Post-processing 
# - combine all sample size reduction together in one table
#
# (c) ODISSEI Social Data Science team 2024



# clean workspace
rm(list=ls())

#### PACKAGES ####
library(tidyverse)
library(openxlsx)


# path to move
data_path <- "H:/IGM project/kansenkaart_preprocessing/scratch"
tab_path <- "H:/IGM project/Excel"



main_tab <- read_rds(file.path(data_path, 'main', "03_sample_size.rds"))



#### COHORT ####


for (cohort in c("students", "high_school", "elementary_school",
                 "perinatal")) {
  

  tmp <- read_rds(file.path(data_path, cohort, "03_sample_size.rds"))
  main_tab <- rbind(main_tab, tmp)
  
  
}


# save table
write.xlsx(main_tab, file.path(data_path, 'sample_size_reduction.xlsx'))

