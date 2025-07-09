# Kansenkaart functions for preprocessing 
#
# - Function to record number of zeros, negatives, and missings
# - Function to record not merged and merged outcomes
# - Function for parents and children to record the number of zeros, negatives, and missings
#
# (c) ODISSEI Social Data Science team 2025



# add number of zeros and NA's
missings_tab <- data.frame()
negatives_tab <- data.frame()
zeros_tab <- data.frame()
not_merged_tab <- data.frame()

# parents not in the same household
parents_diff_hh <- data.frame()


#----------------------------------------------------

# missings
missings_function <- function(dat, vec) {
  
  tab <-
    dat %>%
    group_by(RINPERSOONS, RINPERSOON) %>%
    summarize(across(vec, 
                     ~ sum(is.na(.)), .names = 'missing_{.col}'))
  
  return(tab)
}

# negatives
negatives_function <- function(dat, vec) {
  
  tab <-
    dat %>%
    group_by(RINPERSOONS, RINPERSOON) %>%
    summarize(across(vec, 
                     ~ sum(. < 0), .names = 'negative_{.col}'))
  
  return(tab)
}


# zeros
zeros_function <- function(dat, vec) {
  
  tab <-
    dat %>%
    group_by(RINPERSOONS, RINPERSOON) %>%
    summarize(across(vec, 
                     ~ sum(. == 0), .names = 'zero_{.col}'))
  
  return(tab)
}


#----------------------------------------------------
# function for parents

parents_funct <- function(type_dat, stat_name) {
  
  parents_dat <- left_join(parents_dat, type_dat, 
                           by = c("RINPERSOONMa" = "RINPERSOON", 
                                  "RINPERSOONSMa" = "RINPERSOONS")) 
  parents_dat <- left_join(parents_dat, type_dat, 
                           by = c("RINPERSOONpa" = "RINPERSOON", 
                                  "RINPERSOONSpa" = "RINPERSOONS"), 
                           suffix = c('', '_pa'))

  main_dat <- data.frame()
  for (cols in names(parents_dat)[grepl(paste0('^', stat_name), 
                                        names(parents_dat))]) {
    
    tmp <- parents_dat %>%
      group_by(!!sym(cols)) %>%
      summarize(n = n()) %>%
      mutate(outcome = cols)
  names(tmp) <- c(stat_name, 'n', 'outcome')
  
  main_dat <- bind_rows(main_dat, tmp)
  }
  
  return(main_dat)
}


#----------------------------------------------------
# not merged

not_merged_func <- function(outcomes) {
  
  for (outcome_name in outcomes) {
    
    tmp <- tibble(
      outcome = outcome_name, 
      not_merged = sum(is.na(cohort_dat[outcome_name])), 
      merged = sum(!is.na(cohort_dat[outcome_name]))
    )
    
    not_merged_tab <- bind_rows(not_merged_tab, tmp)

  }
  return(not_merged_tab)
}

#----------------------------------------------------
# function for children


child_funct <- function(type_dat, stat_name) {

  main_dat <- data.frame()
  for (cols in names(type_dat)[grepl(paste0('^', stat_name), 
                                         names(type_dat))]) {

    tmp <- type_dat %>%
      group_by(!!sym(cols)) %>%
      summarize(n = n()) %>%
      mutate(outcome = cols)
    names(tmp) <- c(stat_name, 'n', 'outcome')
    
    main_dat <- bind_rows(main_dat, tmp)
  }
  return(main_dat)
}

