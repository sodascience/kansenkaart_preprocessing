# Kansenkaart data preparation pipeline
#
# Post-processing 
# - combine all sample size reduction together in one table
#
# (c) ODISSEI Social Data Science team 2024



#### PACKAGES ####
library(tidyverse)
library(openxlsx)


# path to move
data_path <- "H:/IGM project/kansenkaart_preprocessing/scratch"
tab_path <- "H:/IGM project/Excel"



main_tab <- read_rds(file.path(data_path, 'main', "03_sample_size.rds"))



#### COHORT ####


for (cohort in c("students", "high_school")) {
  

  tmp <- read_rds(file.path(data_path, cohort, "03_sample_size.rds"))
  main_tab <- rbind(main_tab, tmp)
  
  
}

elementary_school <- read_rds(file.path(data_path, "elementary_school", "03_sample_size.rds"))

perinatal <- read_rds(file.path(data_path, "perinatal", "03_sample_size.rds"))



# write to excel
wb <- createWorkbook()

addWorksheet(wb, 'main_tab')
addWorksheet(wb, 'elementary_school')
addWorksheet(wb, 'perinatal')

writeDataTable(wb, 'main_tab', main_tab)
writeDataTable(wb, 'elementary_school', elementary_school)
writeDataTable(wb, 'perinatal', perinatal)


# save table
saveWorkbook(wb, file.path(tab_path, 'sample_size_reduction.xlsx'))

