# Kansenkaart data preparation pipeline
#
# 3. Outcome creation.
#   - Adding elementary school outcomes to the cohort.
#
# (c) ODISSEI Social Data Science team 2024



#### PACKAGES ####
library(tidyverse)
library(lubridate)
library(haven)
library(readxl)


#### CONFIGURATION ####
# load main cohort dataset
cohort_dat <- read_rds(file.path(loc$scratch_folder, "02_predictors.rds"))

sample_size <- read_rds(file.path(loc$scratch_folder, "02_sample_size.rds"))



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
                     WPOBRIN_crypt = character(), WPOBRINVEST = character(), WPOGROEPSGROOTTE = character(),
                     WPOREKENEN = character(), WPOTAALLV = character(), WPOTAALTV = character(),
                     WPOTOETSADVIES = character(), WPOADVIESVO = character(), 
                     WPOADVIESHERZ = character(), WPOTYPEPO = character())

for (year in seq(as.integer(cfg$elementary_school_year_min), as.integer(cfg$elementary_school_year_max))) {
  school_dat <- 
    # read file from disk
    read_sav(get_inschrwpo_filename(year), 
             col_select = c("RINPERSOONS", "RINPERSOON", "WPOLEERJAAR", "WPOGROEPSGROOTTE",
                            "WPOTOETSADVIES","WPOADVIESVO", "WPOADVIESHERZ", 
                            "WPOREKENEN", "WPOTAALLV", "WPOTAALTV",
                            "WPOBRIN_crypt", "WPOBRINVEST", "WPOTYPEPO")) %>% 
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
  filter(WPOLEERJAAR == "8") %>%
  select(-WPOLEERJAAR)


# keep unique observations
school_dat <- school_dat %>%
  # when duplicated pick the last year
  arrange(desc(year)) %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  filter(row_number() == 1)


# add to data
cohort_dat <- inner_join(cohort_dat, school_dat, by = c("RINPERSOONS", "RINPERSOON"))
rm(school_dat)


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
  ) %>%
  select(-c(WPOBRIN_crypt, WPOBRINVEST, WPOTYPEPO, WPOGROEPSGROOTTE, WPOREKENEN, WPOTAALLV, WPOTAALTV, 
            WPOTOETSADVIES, WPOADVIESVO, WPOADVIESHERZ, advice_type, 
            final_school_advice))


# record sample size
sample_size <- sample_size %>% 
  mutate(n_5_child_outcomes = nrow(cohort_dat))

#### LIVE CONTINUOUSLY IN NL ####

# We only include children who live continuously in the Netherlands in the year of the outcome year
# children are only allowed to live up to 31 days not in the Netherlands
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

# record sample size
sample_size <- sample_size %>% 
  mutate(n_6_child_residency = nrow(cohort_dat))


#### YOUTH HEALTH COSTS ####


# create a table with incomes at the cpi_base_year level
# first, load consumer price index data (2015 = 100)
# source: CBS statline
cpi_tab <- read_excel(loc$cpi_index_data) %>% 
  mutate(year = as.numeric(year))

# set cpi_base_year = 100
cpi_tab <- cpi_tab %>%
  mutate(
    cpi = cpi / cpi_tab %>% filter(year == cfg$cpi_base_year) %>% pull(cpi) * 100)



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

rm(health_dat, cpi_tab)

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
                         year = integer())
for (year in seq(2015, as.integer(cfg$elementary_school_year_max))) {
  
  protection_dat <-
    read_sav(get_protection_filename(year),
             col_select = c("RINPERSOONS", "RINPERSOON")) %>%
    mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
           year = year) %>%
    # add to protection dat
    bind_rows(protection_dat, .)
}

# use protection data 2015 dat to 2014 year since we do not have protection data 2014
protection_dat <-
  read_sav(get_protection_filename(2015),
           col_select = c("RINPERSOONS", "RINPERSOON")) %>%
  mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
         year = 2014) %>%
  # add to protection dat
  bind_rows(protection_dat, .)


# create youth protection outcome
protection_dat <- protection_dat %>%
  mutate(youth_protection = 1) %>%
  unique()

# add to data
cohort_dat <- left_join(cohort_dat, protection_dat,
                        by = c("RINPERSOONS", "RINPERSOON", "year"))

rm(protection_dat)

# convert NA to 0
cohort_dat <- 
  cohort_dat %>%
  mutate(youth_protection = ifelse(is.na(youth_protection), 0, youth_protection))

#### LIVING SPACE PER HOUSEHOLD MEMBER ####

#load home addresses
adres_tab <- read_sav(file.path(loc$data_folder, loc$gbaao_data)) %>%
  mutate(
    RINPERSOONS = as_factor(RINPERSOONS, levels = "values"),
    SOORTOBJECTNUMMER = as_factor(SOORTOBJECTNUMMER, levels = "values"),
    GBADATUMAANVANGADRESHOUDING = ymd(GBADATUMAANVANGADRESHOUDING),
    GBADATUMEINDEADRESHOUDING = ymd(GBADATUMEINDEADRESHOUDING))


# load home addresses
adres_tab <- 
  adres_tab %>% 
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
cohort_dat <- 
  cohort_dat %>%
  mutate(AantalBewoners = ifelse(AantalBewoners == 0, NA, AantalBewoners),
         living_space_pp = VBOOPPERVLAKTE / AantalBewoners) %>%
  select(-c(january, SOORTOBJECTNUMMER, RINOBJECTNUMMER, 
            VBOOPPERVLAKTE, AantalBewoners))

rm(household_members)

#### CLASS COMPOSITION ####

# load class cohort data
class_cohort_dat <- read_rds(file.path(loc$scratch_folder, "class_cohort.rds"))

# combine the main sample and class sample
class_cohort_dat <- bind_rows(
  class_cohort_dat %>% select ("RINPERSOON", "RINPERSOONS", "GBAGEBOORTELANDMOEDER", "GBAGEBOORTELANDVADER", "income_parents_perc"), 
  cohort_dat %>% select ("RINPERSOON", "RINPERSOONS", "GBAGEBOORTELANDMOEDER", "GBAGEBOORTELANDVADER", "income_parents_perc"), 
)

school_dat <- tibble(RINPERSOONS = factor(), RINPERSOON = character(), WPOLEERJAAR = character(), 
                     WPOBRIN_crypt = character(), WPOBRINVEST = character(), WPOGROEPSGROOTTE = character(),
                     WPOREKENEN = character(), WPOTAALLV = character(), WPOTAALTV = character(),
                     WPOTOETSADVIES = character(), WPOADVIESVO = character(), 
                     WPOADVIESHERZ = character(), WPOTYPEPO = character())


for (year in seq(as.integer(cfg$elementary_school_year_min), as.integer(cfg$elementary_school_year_max))) {
  school_dat <- 
    # read file from disk
    read_sav(get_inschrwpo_filename(year), 
             col_select = c("RINPERSOONS", "RINPERSOON", "WPOLEERJAAR", "WPOGROEPSGROOTTE",
                            "WPOTOETSADVIES","WPOADVIESVO", "WPOADVIESHERZ", 
                            "WPOREKENEN", "WPOTAALLV", "WPOTAALTV",
                            "WPOBRIN_crypt", "WPOBRINVEST", "WPOTYPEPO")) %>% 
    mutate(RINPERSOONS = as_factor(RINPERSOONS, levels = "value")) %>%
    # add year
    mutate(year = year) %>% 
    # add to income children
    bind_rows(school_dat, .)
}

# keep group 8 pupils
school_dat <- school_dat %>%
  filter(RINPERSOONS == "R") %>%
  mutate(WPOLEERJAAR = trimws(as.character(WPOLEERJAAR))) %>%
  filter(WPOLEERJAAR == "8") %>%
  select(-WPOLEERJAAR)


# link to classroom sample
school_dat <- school_dat %>%
  left_join(class_cohort_dat, 
            by = c("RINPERSOON", "RINPERSOONS"))%>%
  # drop all children who are not in the large classroom sample 
  filter(!is.na(income_parents_perc))


# primary school ID
school_dat <- school_dat %>%
  mutate(across(c("WPOBRIN_crypt", "WPOBRINVEST"), as.character)) %>%
  mutate(school_ID = paste0(WPOBRIN_crypt, WPOBRINVEST)) %>%
  select(-c(WPOBRIN_crypt, WPOBRINVEST))

# convert NA
school_dat <-
  school_dat %>%
  mutate(across(c("WPOGROEPSGROOTTE", "WPOTOETSADVIES", "WPOADVIESVO", 
                  "WPOREKENEN", "WPOTAALLV", "WPOTAALTV",
                  "WPOADVIESHERZ"), as.character)) %>%
  mutate(across(c("WPOGROEPSGROOTTE", "WPOTOETSADVIES", 
                  "WPOREKENEN", "WPOTAALLV", "WPOTAALTV",
                  "WPOADVIESVO", "WPOADVIESHERZ"), as.numeric))


# create outcome variables
school_dat <-
  school_dat %>%
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
school_dat <-
  school_dat %>%
  mutate(
    math     = ifelse(is.na(WPOREKENEN), NA, math),
    reading  = ifelse(is.na(WPOTAALLV), NA, reading),
    language = ifelse(is.na(WPOTAALTV), NA, language),
    
    # cito test outcomes
    vmbo_gl_test = ifelse(is.na(WPOTOETSADVIES), NA, vmbo_gl_test),
    havo_test    = ifelse(is.na(WPOTOETSADVIES), NA, havo_test),
    vwo_test     = ifelse(is.na(WPOTOETSADVIES), NA, vwo_test)
  )




# create parents rank income outcomes
school_dat <- school_dat %>%
  mutate(
    # create dummy for below 25th 
    income_below_25th = ifelse(income_parents_perc < 0.25, 1, 0),
    # create dummy for below 50th 
    income_below_50th = ifelse(income_parents_perc < 0.50, 1, 0),
    # create dummy for above 75th
    income_above_75th = ifelse(income_parents_perc > 0.75, 1, 0)
  )


# create outcome for children with both parents born in a foreign country
school_dat <- school_dat %>%
  mutate(
    GBAGEBOORTELANDMOEDER = as_factor(GBAGEBOORTELANDMOEDER),
    GBAGEBOORTELANDVADER = as_factor(GBAGEBOORTELANDVADER)
  ) %>%
  mutate(
    foreign_born_parents = 
      ifelse((GBAGEBOORTELANDMOEDER != "Nederland" & 
                GBAGEBOORTELANDVADER != "Nederland"),  1, 0))


# CLASSROOM OUTCOMES
# 1.  class_vmbo_gl_test
# 2.  class_havo_test
# 3.  class_vwo_test
# 4.  class_foreign_born_parents
# 5.  class_income_below_25th
# 6.  class_income_below_50th
# 7.  class_income_above_75th
# 8.  class_math
# 9.  class_language
# 10. class_reading
# 11.  class_size (2014 - 2016)

# only keep classes with more than one student per class
school_dat <- school_dat %>%
  group_by(school_ID, year) %>%
  mutate(n = n()) %>%
  filter(n > 1)


# hold out mean function
hold_out_means <- function(x) {
  hold <- ((sum(x, na.rm = TRUE) - x) / (length(x) - 1))
  return(hold)
}

# hold out means = mean of the class without the child him/herself
school_dat <-
  school_dat %>%
  group_by(school_ID, year) %>%
  mutate(
    N_students_per_school = n(),
    
    class_math = hold_out_means(math),
    class_reading = hold_out_means(reading),
    class_language = hold_out_means(language),
    
    class_foreign_born_parents = hold_out_means(foreign_born_parents),
    
    class_vmbo_gl_test = hold_out_means(vmbo_gl_test),
    class_havo_test = hold_out_means(havo_test),
    class_vwo_test = hold_out_means(vwo_test),
    
    class_income_below_25th = hold_out_means(income_below_25th),
    class_income_below_50th = hold_out_means(income_below_50th),
    class_income_above_75th = hold_out_means(income_above_75th)
  ) %>% 
  rename(class_size = WPOGROEPSGROOTTE)


# keep unique observations,for duplicates select the last time the child is in 8th grade
school_dat <- school_dat %>%
  arrange(desc(year)) %>%
  group_by(RINPERSOONS, RINPERSOON) %>%
  filter(row_number() == 1)


# select relevant variables
school_dat <-
  school_dat %>%
  select(c(RINPERSOONS, RINPERSOON, school_ID, 
           N_students_per_school, class_foreign_born_parents,  
           class_vmbo_gl_test, class_havo_test, class_vwo_test, 
           class_income_below_25th, class_income_below_50th, class_income_above_75th,
           class_math, class_reading, class_language, class_size ))


# add to outcomes to cohort
cohort_dat <- left_join(cohort_dat, school_dat, 
                        by = c("RINPERSOONS", "RINPERSOON")) %>%
  select(-c(school_ID, N_students_per_school, birth_year))

rm(school_dat, class_cohort_dat)



#### PREFIX ####

# add prefix to outcomes
outcomes <- c("vmbo_gl_final", "havo_final", "vwo_final", 
              "vmbo_gl_test", "havo_test", "vwo_test",
              "over_advice", "under_advice", "math", "language", 
              "reading", "youth_health_costs", "youth_protection", 
              "living_space_pp","class_vmbo_gl_test", "class_havo_test", 
              "class_vwo_test", "class_foreign_born_parents", 
              "class_income_below_25th", "class_income_below_50th",
              "class_income_above_75th", "class_math", "class_language", 
              "class_reading", "class_size")

suffix <- "c11_"


# rename outcomes
cohort_dat <- 
  cohort_dat %>%
  rename_with(~str_c(suffix, .), .cols = all_of(outcomes)) %>% 
  ungroup() %>%
  #remove parents birth country
  select(-c(GBAGEBOORTELANDMOEDER, GBAGEBOORTELANDVADER))


#### WRITE OUTPUT TO SCRATCH ####
write_rds(cohort_dat, file.path(loc$scratch_folder, "03_outcomes.rds"))


#write sample size reduction table to scratch
sample_size <- sample_size %>% mutate(cohort_name = cohort)
write_rds(sample_size, file.path(loc$scratch_folder, "03_sample_size.rds"))
