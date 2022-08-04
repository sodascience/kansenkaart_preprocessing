# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding elementary school outcomes to the cohort.
#
# (c) ODISSEI Social Data Science team 2021



#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(readxl)

#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "02_predictors.rds"))


#### ELEMENTARY SCHOOL OUTCOMES ####

# function to get latest inschrwpo version of specified year
get_inschrwpo_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "Onderwijs/INSCHRWPOTAB"),
    pattern = paste0("INSCHRWPOTAB", year, "V[0-9]+(?i)(.sav)"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


school_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), WPOLEERJAAR = character(), 
                     WPOGROEPSGROOTTE = character(),
                     WPOREKENEN = character(), WPOTAALLV = character(), WPOTAALTV = character(), 
                     WPOTOETSADVIES = character(), WPOADVIESVO = character(), WPOADVIESHERZ = character())
for (year in seq(as.integer(cfg$elementary_school_year_min), as.integer(cfg$elementary_school_year_max))) {
  school_dat <- 
    # read file from disk
    read_sav(get_inschrwpo_filename(year), 
             col_select = c("RINPERSOONS", "RINPERSOON", "WPOLEERJAAR", "WPOREKENEN",
                            "WPOTAALLV", "WPOTAALTV", "WPOTOETSADVIES", "WPOADVIESVO",
                            "WPOADVIESHERZ", "WPOGROEPSGROOTTE")) %>% 
    mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
    # select only children that are in the cohort
    filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>% 
    # add year
    mutate(year = year) %>% 
    # add to income children
    bind_rows(school_dat, .)
}

# only keep pupils who are in group 8
school_dat <- school_dat %>%
  filter(RINPERSOONS == "R") %>% 
  mutate(WPOLEERJAAR = trimws(as.character(WPOLEERJAAR))) %>%
  filter(WPOLEERJAAR == "8")


# keep unique observations
school_dat <- school_dat %>%
  # when duplicated pick the last year
  arrange(desc(year)) %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  filter(row_number() == 1)


# add to data
cohort_dat <- inner_join(cohort_dat, school_dat, by = c("RINPERSOONS", "RINPERSOON"))
rm(school_dat)


#### LIVE CONTINUOUSLY IN NL ####

# We only include children who live continuously in the Netherlands in the year of the outcom year
# children are only allowed to live up to 31 days not in the netherlands
adres_dat <- read_sav(file.path(loc$data_folder, loc$gbaao_data)) %>%
  mutate(
    RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
    SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"),
    GBADATUMAANVANGADRESHOUDING = ymd(GBADATUMAANVANGADRESHOUDING),
    GBADATUMEINDEADRESHOUDING = ymd(GBADATUMEINDEADRESHOUDING)
  )

# add the year we use for residency requirement
child_adres_date <- cohort_dat %>%
  select(RINPERSOON, RINPERSOONS, year) %>%
  mutate(adres_year = year)  %>%
  select(-year)

# remove addresses that end before or start after the residency year
adres_tab <- adres_dat %>%
  inner_join(child_adres_date, by = c("RINPERSOONS", "RINPERSOON")) %>%
  filter(!(format(GBADATUMEINDEADRESHOUDING, "%Y") < adres_year),
         !(format(GBADATUMAANVANGADRESHOUDING, "%Y") > adres_year))


# throw out anything with an end date before start_date, and anything with a start date after end_date
# then also set the start date of everything to start_date, and the end date of everything to end_date
# then compute the timespan of each record
adres_tab <- 
  adres_tab %>%
  mutate(
    start_date  = dmy(paste0("0101", adres_year)),
    end_date    = dmy(paste0("3112", adres_year)),
    cutoff_days = as.numeric(difftime(end_date, start_date, units = "days")) - cfg$child_live_slack_days,
    recordstart = as_date(ifelse(GBADATUMAANVANGADRESHOUDING < start_date, start_date, GBADATUMAANVANGADRESHOUDING)),
    recordend   = as_date(ifelse(GBADATUMEINDEADRESHOUDING > end_date, end_date, GBADATUMEINDEADRESHOUDING)),
    timespan    = difftime(recordend, recordstart, units = "days")
  )


# group by person and sum the total number of days
# then compute whether this person lived in the Netherlands continuously
days_tab <- 
  adres_tab %>% 
  select(RINPERSOONS, RINPERSOON, timespan, cutoff_days) %>% 
  mutate(timespan = as.numeric(timespan)) %>% 
  group_by(RINPERSOONS, RINPERSOON) %>% 
  summarize(total_days = sum(timespan),
            continuous_living = total_days >= cutoff_days) %>% 
  select(RINPERSOONS, RINPERSOON, continuous_living) %>%
  unique()


# add to cohort and filter
cohort_dat <- 
  left_join(cohort_dat, days_tab, by = c("RINPERSOONS", "RINPERSOON")) %>% 
  filter(continuous_living) %>% 
  select(-continuous_living)

rm(adres_tab, days_tab, child_adres_date)


#### OUTCOMES ####


# convert NA
cohort_dat <- 
  cohort_dat %>%
  mutate(across(c("WPOREKENEN", "WPOTAALLV", "WPOTAALTV", "WPOGROEPSGROOTTE",
                  "WPOTOETSADVIES", "WPOADVIESVO", "WPOADVIESHERZ"),
                as.character)) %>%
  mutate(across(c("WPOREKENEN", "WPOTAALLV", "WPOTAALTV", "WPOGROEPSGROOTTE",
                  "WPOTOETSADVIES", "WPOADVIESVO", "WPOADVIESHERZ"),
                as.numeric))


# create outcome variables
cohort_dat <- cohort_dat %>%
  mutate(
    # rekenen, lezen & taalverzorging
    math     = ifelse((WPOREKENEN == 3 | WPOREKENEN == 4), 1, 0),
    reading  = ifelse(WPOTAALLV == 4, 1, 0),
    language = ifelse(WPOTAALTV == 4, 1, 0),
    
    # cito test outcomes
    vmbo_gl_test = ifelse(WPOTOETSADVIES %in% c(42, 44, 60, 61, 70), 1, 0),
    havo_test      = ifelse(WPOTOETSADVIES %in% c(60, 61, 70), 1, 0),
    vwo_test       = ifelse(WPOTOETSADVIES == 70, 1, 0),
    
    # final high school advice 
    # replace 80 (= Geen specifiek advies mogelijk) with NA
    WPOADVIESVO = ifelse(WPOADVIESVO == 80, NA, WPOADVIESVO),
    # replace final school advice (wpoadviesvo) with wpoadviesherz if wpoadviesherz is not missing
    final_school_advice = ifelse(!is.na(WPOADVIESHERZ), WPOADVIESHERZ, WPOADVIESVO),
    
    vmbo_gl_final = ifelse(final_school_advice %in% c(40, 41, 42, 43, 44, 45, 50, 
                                                             51, 52, 53, 60, 61, 70), 1, 0),
    havo_final      = ifelse(final_school_advice %in% c(60, 61, 70), 1, 0),
    vwo_final       = ifelse(final_school_advice == 70, 1, 0)
    
  )

# replace NA
cohort_dat <- cohort_dat %>%
  mutate(
    math     = ifelse(is.na(WPOREKENEN), NA, math),
    reading  = ifelse(is.na(WPOTAALLV), NA, reading),
    language = ifelse(is.na(WPOTAALTV), NA, language),
    
    # cito test outcomes
    vmbo_gl_test = ifelse(is.na(WPOTOETSADVIES), NA, vmbo_gl_test),
    havo_test      = ifelse(is.na(WPOTOETSADVIES), NA, havo_test),
    vwo_test       = ifelse(is.na(WPOTOETSADVIES), NA, vwo_test),
    
    # final school advice outcomes
    vmbo_gl_final = ifelse(is.na(final_school_advice), NA, vmbo_gl_final),
    havo_final      = ifelse(is.na(final_school_advice), NA, havo_final),
    vwo_final       = ifelse(is.na(final_school_advice), NA, vwo_final)
  )

# under advice and over advice outcomes
# import under- and over advice table
advice_tab <- read_xlsx(loc$advice_data, sheet = loc$advice_data_sheet)
advice_mat <- as.matrix(advice_tab %>% select(-Teacher))

# change row and column names to numeric education categories
rownames(advice_mat) <- parse_number(advice_tab$Teacher)
colnames(advice_mat) <- parse_number(colnames(advice_tab)[-1])

#  create table with all possible combinations 
advice_type_tab <- expand_grid(
  final_advice = rownames(advice_mat), 
  test_advice = colnames(advice_mat)
) %>% 
  rowwise() %>% 
  mutate(advice_type  = advice_mat[final_advice, test_advice],
         final_advice = as.numeric(final_advice),
         test_advice  = as.numeric(test_advice)
  ) %>% 
  ungroup()


cohort_dat <- left_join(cohort_dat, advice_type_tab, 
                        by = c("final_school_advice" = "final_advice",
                               "WPOTOETSADVIES" = "test_advice")) 

rm(advice_type_tab, advice_mat, advice_tab)


# create under- and over advice outcomes
cohort_dat <- 
  cohort_dat %>%
  mutate(
    under_advice = ifelse(advice_type == "under", 1, 0),
    over_advice  = ifelse(advice_type == "over", 1, 0)
  )

# no missings in all outcomes
cohort_dat <- 
  cohort_dat %>%
  filter(
    !(is.na(math) & is.na(reading) & is.na(language) & 
        is.na(vmbo_gl_test) & is.na(havo_test) & 
        is.na(vwo_test) & is.na(vmbo_gl_final) & 
        is.na(havo_final) & is.na(vwo_final) &
        is.na(under_advice) & is.na(over_advice))
  )


#### YOUTH HEALTH COSTS ####


# load consumer price index data
cpi_tab <- 
  read_delim(loc$cpi_index_data, ";", skip = 5, 
             col_names = c("year", "cpi", "cpi_derived", "cpi_change", "cpi_change_derived"), 
             col_types = "ccccc") %>% 
  mutate(across(starts_with("cpi"), parse_number, locale = locale(decimal_mark = ","))) %>% 
  mutate(year = as.numeric(year))

# set cpi_base_year = 100
cpi_tab <- 
  cpi_tab %>% 
  mutate(
    cpi = cpi / cpi_tab %>% filter(year == cfg$cpi_base_year) %>% pull(cpi) * 100,
    cpi_derived = cpi_derived / cpi_tab %>% filter(year == cfg$cpi_base_year) %>% pull(cpi_derived) * 100
  )


# function to get latest inschrwpo version of specified year
get_health_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "GezondheidWelzijn/ZVWZORGKOSTENTAB", year),
    pattern = paste0("ZVWZORGKOSTEN", year, "TABV[0-9]+(?i)(.sav)"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


health_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                     youth_health_costs = double(), year = integer())
for (year in seq(as.integer(cfg$elementary_school_year_min), as.integer(cfg$elementary_school_year_max))) {
  
  if (year == 2014) {
    health_tab <- read_sav(get_health_filename(2014),
                           col_select = c("RINPERSOONS", "RINPERSOON", "ZVWKHUISARTS", "ZVWKGEBOORTEZORG",
                                          "ZVWKFARMACIE", "ZVWKMONDZORG", "ZVWKZIEKENHUIS",        
                                          "ZVWKPARAMEDISCH", "ZVWKHULPMIDDEL", "ZVWKZIEKENVERVOER", 
                                          "ZVWKBUITENLAND", "ZVWKOVERIG", "ZVWKGERIATRISCH", 
                                          "ZVWKWYKVERPLEGING", "NOPZVWKHUISARTSINSCHRIJF")) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values")) %>%
      # select only children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON)
    
  } else if (year == 2015 | year == 2016 | year == 2017) {
    health_tab <- read_sav(get_health_filename(year),
                           col_select = c("RINPERSOONS", "RINPERSOON", "ZVWKHUISARTS", "ZVWKMULTIDISC",
                                          "ZVWKFARMACIE", "ZVWKMONDZORG", "ZVWKZIEKENHUIS", "ZVWKGEBOORTEZORG",       
                                          "ZVWKPARAMEDISCH", "ZVWKHULPMIDDEL", "ZVWKZIEKENVERVOER", 
                                          "ZVWKBUITENLAND", "ZVWKOVERIG", "ZVWKGERIATRISCH", 
                                          "ZVWKWYKVERPLEGING", "NOPZVWKHUISARTSINSCHRIJF")) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values")) %>%
      # select only children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON)
    
  } else if (year == 2018) {
    health_tab <- read_sav(get_health_filename(year),
                           col_select = c("RINPERSOONS", "RINPERSOON", "ZVWKHUISARTS", "ZVWKMULTIDISC",
                                          "ZVWKFARMACIE", "ZVWKMONDZORG", "ZVWKZIEKENHUIS", "ZVWKEERSTELIJNSVERBLIJF",        
                                          "ZVWKPARAMEDISCH", "ZVWKHULPMIDDEL", "ZVWKZIEKENVERVOER", 
                                          "ZVWKBUITENLAND", "ZVWKOVERIG", "ZVWKGERIATRISCH", "ZVWKGEBOORTEZORG",
                                          "ZVWKWYKVERPLEGING", "NOPZVWKHUISARTSINSCHRIJF")) %>%
      mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values")) %>%
      # select only children
      filter(RINPERSOON %in% cohort_dat$RINPERSOON)
  }
  
  health_tab <- health_tab %>%
    # replace negative values with 0
    mutate(across(grep("^ZVWK", names(health_tab), value = TRUE),
                  function(x) ifelse(x < 0, 0, x))) %>%
    # sum of all healthcare costs
    mutate(
      youth_health_costs = rowSums(across(grep("^ZVWK", names(health_tab), value = TRUE)), na.rm = TRUE),
      youth_health_costs = youth_health_costs - (NOPZVWKHUISARTSINSCHRIJF + ZVWKGEBOORTEZORG), 
      year = year # add year
    ) %>%
    select(RINPERSOONS, RINPERSOON, youth_health_costs, year)
  
  # add to health dat
  health_dat <- bind_rows(health_dat, health_tab)
  
}
rm(health_tab)


# deflate
health_dat <- 
  health_dat %>% 
  left_join(cpi_tab %>% select(year, cpi), by = "year") %>% 
  mutate(youth_health_costs = youth_health_costs / (cpi / 100)) %>% 
  select(-cpi)


# add to data
cohort_dat <- left_join(cohort_dat, health_dat, 
                        by = c("RINPERSOONS", "RINPERSOON", "year"))

rm(health_dat)

# convert NA to 0
cohort_dat <- cohort_dat %>%
  mutate(youth_health_costs = ifelse(is.na(youth_health_costs), 0, youth_health_costs))



#### YOUTH PROTECTION ####

# function to get latest jgdbeschermbus version of specified year
get_protection_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "VeiligheidRecht/JGDBESCHERMBUS"),
    pattern = paste0("JGDBESCHERM", year, "BUSV[0-9]+(?i)(.sav)"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


protection_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), 
                         JGDBMAATREGEL = factor(), year = integer())
for (year in seq(2015, as.integer(cfg$elementary_school_year_max))) {
  
  protection_dat <-
    read_sav(get_protection_filename(year),
             col_select = c("RINPERSOONS", "RINPERSOON", "JGDBMAATREGEL")) %>%
    mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
           JGDBMAATREGEL = as_factor(JGDBMAATREGEL, levels = "labels"),
           year = year) %>%
    # add to protection dat
    bind_rows(protection_dat, .)
}

# use protection data 2015 dat to 2014 year since we do not have protection data 2014
protection_dat <-
  read_sav(get_protection_filename(2015),
           col_select = c("RINPERSOONS", "RINPERSOON", "JGDBMAATREGEL")) %>%
  mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
         JGDBMAATREGEL = as_factor(JGDBMAATREGEL, levels = "labels"),
         year = 2014) %>%
  # add to protection dat
  bind_rows(protection_dat, .)


# convert long format to wide format
protection_dat <- 
  protection_dat %>% 
  group_by(RINPERSOONS, RINPERSOON, year, JGDBMAATREGEL) %>%
  summarize(n = n()) %>%
  ungroup() %>%
  pivot_wider(
    names_from = JGDBMAATREGEL, 
    values_from = n,
    values_fill = 0
  )

# replace black space of column names
names(protection_dat) <- gsub(" ", "_", names(protection_dat)) 

# create youth protection outcome
protection_dat <- protection_dat %>%
  mutate(youth_protection = 1)

# add to data
cohort_dat <- left_join(cohort_dat, protection_dat,
                        by = c("RINPERSOONS", "RINPERSOON", "year"))
rm(protection_dat)

# convert NA to 0
cohort_dat <- 
  cohort_dat %>%
  mutate(
    youth_protection = ifelse(is.na(youth_protection), 0, youth_protection),
    voorlopige_ondertoezichtstelling = ifelse(is.na(voorlopige_ondertoezichtstelling), 0, voorlopige_ondertoezichtstelling),
    ondertoezichtstelling            = ifelse(is.na(ondertoezichtstelling), 0, ondertoezichtstelling),
    voorlopige_voordij               = ifelse(is.na(voorlopige_voordij), 0, voorlopige_voordij),
    voogdij                          = ifelse(is.na(voogdij), 0, voogdij),
    tijdelijke_voogdij               = ifelse(is.na(tijdelijke_voogdij), 0, tijdelijke_voogdij)
  )



#### LIVING SPACE PER HOUSEHOLD MEMBER ####


# load home addresses
adres_tab <- 
  adres_dat %>% 
  # select only children
  filter(RINPERSOON %in% cohort_dat$RINPERSOON) %>%
  filter(!(GBADATUMEINDEADRESHOUDING < ymd(paste0(cfg$elementary_school_year_min, "0101"))),
         !(GBADATUMAANVANGADRESHOUDING > ymd(paste0(cfg$elementary_school_year_max, "0101"))))


# create 1 january variable 
cohort_dat <- cohort_dat %>%
  mutate(january = ymd(paste0(year, "-01-01")))


# add 1 januari data to adres tab to filter for home addresses at 1 jan
adres_tab <- 
  adres_tab %>%
  left_join(cohort_dat %>% select(RINPERSOONS, RINPERSOON, january)) %>%
  filter(january %within% interval(GBADATUMAANVANGADRESHOUDING, GBADATUMEINDEADRESHOUDING)) 


# add home addresses to data
cohort_dat <- left_join(cohort_dat, adres_tab %>% 
                          select(RINPERSOONS, RINPERSOON,
                                 SOORTOBJECTNUMMER, RINOBJECTNUMMER),
                        by = c("RINPERSOONS", "RINPERSOON"))
rm(adres_tab)


# LIVING SPACE

woon_dat <-
  read_sav(file.path(loc$data_folder, loc$woon_data),
           col_select = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER", "VBOOPPERVLAKTE", 
                          "AANVLEVCYCLWOONNIETWOON", "EINDLEVCYCLWOONNIETWOON")) %>%
  mutate(SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"), 
         AANVLEVCYCLWOONNIETWOON = ymd(AANVLEVCYCLWOONNIETWOON), 
         EINDLEVCYCLWOONNIETWOON = ifelse(EINDLEVCYCLWOONNIETWOON == "88888888",
                                          paste0(cfg$elementary_school_year_max, "1231"),
                                          EINDLEVCYCLWOONNIETWOON),
         EINDLEVCYCLWOONNIETWOON = ymd(EINDLEVCYCLWOONNIETWOON)
  ) %>%
  filter(!(EINDLEVCYCLWOONNIETWOON < ymd(paste0(cfg$elementary_school_year_min, "-01-01"))),
         !(AANVLEVCYCLWOONNIETWOON > ymd(paste0(cfg$elementary_school_year_max, "-01-01"))))


# add 1 januari data to woon tab to filter for home addresses at 1 jan
woon_dat <- 
  woon_dat %>%
  left_join(cohort_dat %>% select(SOORTOBJECTNUMMER, RINOBJECTNUMMER, january), 
            by = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER")) %>%
  filter(january %within% interval(AANVLEVCYCLWOONNIETWOON, EINDLEVCYCLWOONNIETWOON)) %>%
  unique()



# add living space to data
cohort_dat <- left_join(cohort_dat,
                        woon_dat %>% select(SOORTOBJECTNUMMER, RINOBJECTNUMMER, VBOOPPERVLAKTE, january),
                        by = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER", "january"))  %>%
  mutate(VBOOPPERVLAKTE = as.numeric(VBOOPPERVLAKTE))

rm(woon_dat)



# NUMBER OF HOUSEHOLD MEMBERS


# function to get latest eigendom version of specified year
get_eigendom_filename <- function(year) {
  fl <- list.files(
    path = file.path(loc$data_folder, "BouwenWonen/EIGENDOMTAB"),
    pattern = paste0("EIGENDOM", year, "TABV[0-9]+(?i)(.sav)"), 
    full.names = TRUE
  )
  # return only the latest version
  sort(fl, decreasing = TRUE)[1]
}


household_members <- tibble(SOORTOBJECTNUMMER = factor(), RINOBJECTNUMMER = character(), 
                            AantalBewoners = double(), year = integer())
for (year in seq(as.integer(cfg$elementary_school_year_min), as.integer(cfg$elementary_school_year_max))) {
  
  household_members <- read_sav(get_eigendom_filename(year), 
                                col_select = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER",
                                               "AantalBewoners")) %>%
    mutate(SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"),
           AantalBewoners = as.numeric(AantalBewoners),
           year = year) %>%
    # add to household member dat
    bind_rows(household_members, .)
}

cohort_dat <- cohort_dat %>%
  left_join(household_members, by = c("SOORTOBJECTNUMMER", "RINOBJECTNUMMER", "year")) %>%
  mutate(AantalBewoners = as.numeric(AantalBewoners))


# create living space per household member outcome
cohort_dat <- cohort_dat %>%
  mutate(AantalBewoners = ifelse(AantalBewoners == 0, NA, AantalBewoners),
         living_space_pp = VBOOPPERVLAKTE / AantalBewoners)



#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))
