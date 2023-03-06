# Kansenkaart data preparation pipeline
#
# 4. Post-processing.
#   - Selecting variables of interest.
#   - Writing `scratch/kansenkaart_data.rds`.
#
# (c) ODISSEI Social Data Science team 2022



#### PACKAGES ####
library(tidyverse)
library(haven)


# load cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "03_outcomes.rds")) %>%
  rename(migration = migration_third)



#### MIGRATION BACKGROUND ####
western_tab <- read_sav("K:/Utilities/Code_Listings/SSBreferentiebestanden/LANDAKTUEELREFV12.sav", 
                        col_select = c("LAND", "LANDTYPE")) %>%
  mutate(
    LAND = as_factor(LAND, levels = "labels"),
    LANDTYPE = as_factor(LANDTYPE, levels = "labels")
  )


# create migration variable with origin without third generation
cohort_dat <- 
  cohort_dat %>%
  left_join(western_tab, by = c("GBAHERKOMSTGROEPERING_third" = "LAND")) %>%
  mutate(
    migration_third = as.character(LANDTYPE),
    migration_third = ifelse(migration == "Nederland", "Nederland", migration_third),
    migration_third = ifelse(migration == "Turkije", "Turkije", migration_third),
    migration_third = ifelse(migration == "Marokko", "Marokko", migration_third),
    migration_third = ifelse(migration == "Suriname", "Suriname", migration_third),
    migration_third = ifelse(migration == "Nederlandse Antillen (oud)", "Nederlandse Antillen (oud)", migration_third),
    total_non_western_third = ifelse(migration_third == "NietWesters" |  migration_third == "Turkije" |
                                       migration_third == "Marokko" | migration_third == "Suriname" |
                                       migration_third == "Nederlandse Antillen (oud)", 1, 0)) %>%
  select(-c(LANDTYPE, migration))

# free up memory
rm(western_tab)


  
# save as cohort name
output_file <- file.path(loc$scratch_folder, paste0(cfg$cohort_name, "_cohort.rds"))
write_rds(cohort_dat, output_file)

