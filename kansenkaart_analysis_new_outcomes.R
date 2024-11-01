outcome_source <- tribble(
  ~outcome,                         ~binary,    ~data_source,
  "c30_income",                     FALSE,      "main_cohort",
  "c30_hbo_attained",               TRUE,       "main_cohort",
  "c30_wo_attained",                TRUE,       "main_cohort",
  "c30_hourly_wage",                FALSE,      "main_cohort",
  "c30_hrs_work_pw",                FALSE,      "main_cohort",
  "c30_permanent_contract",         TRUE,       "main_cohort",
  "c30_employed",                   TRUE,       "main_cohort",
  "c30_social_assistance",          TRUE,       "main_cohort",
  "c30_disability",                 TRUE,       "main_cohort",
  "c30_pharma",                     TRUE,       "main_cohort",
  "c30_basic_mhc",                  TRUE,       "main_cohort",
  "c30_specialist_mhc",             TRUE,       "main_cohort",
  "c30_hospital",                   TRUE,       "main_cohort",
  "c30_total_health_costs",         FALSE,      "main_cohort",
  "c30_hourly_wage_max_11",         TRUE,       "main_cohort",
  "c30_hourly_wage_max_14",         TRUE,       "main_cohort",
  "c30_debt",                       FALSE,      "main_cohort",
  "c30_homeowner",                  TRUE,       "main_cohort",
  "c30_wealth",                     FALSE,      "main_cohort",
  "c30_wealth_no_home",             FALSE,      "main_cohort",
  "c30_home_wealth",                FALSE,      "main_cohort",
  "c30_gifts_received",             TRUE,       "main_cohort",
  "c30_sum_gifts",                  FALSE,      "main_cohort",
  "c30_teenage_birth",              TRUE,       "main_cohort",
  "c30_household_income",           FALSE,      "main_cohort",
  "c30_living_space_pp",            FALSE,      "main_cohort",

  "c21_high_school_attained",       TRUE,       "students_cohort",
  "c21_hbo_followed",               TRUE,       "students_cohort",
  "c21_uni_followed",               TRUE,       "students_cohort",
  
  "c16_vmbo_gl",                    TRUE,       "high_school_cohort",
  "c16_havo",                       TRUE,       "high_school_cohort",
  "c16_vwo",                        TRUE,       "high_school_cohort",
  "c16_youth_protection",           TRUE,       "high_school_cohort",
  "c16_youth_health_costs",         FALSE,      "high_school_cohort",
  "c16_living_space_pp",            FALSE,      "high_school_cohort",
  
  "c11_math",                       TRUE,       "elementary_school_cohort",
  "c11_reading",                    TRUE,       "elementary_school_cohort",
  "c11_language",                   TRUE,       "elementary_school_cohort",
  "c11_vmbo_gl_test",               TRUE,       "elementary_school_cohort",
  "c11_havo_test",                  TRUE,       "elementary_school_cohort",
  "c11_vwo_test",                   TRUE,       "elementary_school_cohort",
  "c11_vmbo_gl_final",              TRUE,       "elementary_school_cohort",
  "c11_havo_final",                 TRUE,       "elementary_school_cohort",
  "c11_vwo_final",                  TRUE,       "elementary_school_cohort",
  "c11_under_advice",               TRUE,       "elementary_school_cohort",
  "c11_over_advice",                TRUE,       "elementary_school_cohort",
  "c11_youth_protection",           TRUE,       "elementary_school_cohort",
  "c11_youth_health_costs",         FALSE,      "elementary_school_cohort",
  "c11_living_space_pp",            FALSE,      "elementary_school_cohort",
  
  "c11_class_vmbo_gl_test",         TRUE,       "classroom_cohort",
  "c11_class_havo_test",            TRUE,       "classroom_cohort",
  "c11_class_vwo_test",             TRUE,       "classroom_cohort",
  "c11_class_size",                 FALSE,      "classroom_cohort",
  "c11_class_foreign_born_parents", TRUE,       "classroom_cohort",
  "c11_class_math",                 TRUE,       "classroom_cohort",
  "c11_class_language",             TRUE,       "classroom_cohort",
  "c11_class_reading",              TRUE,       "classroom_cohort",
  "c11_class_income_below_25th",    TRUE,       "classroom_cohort",
  "c11_class_income_above_75th",    TRUE,       "classroom_cohort",
  
  "c00_sga",                        TRUE,       "perinatal_cohort",
  "c00_preterm_birth",              TRUE,       "perinatal_cohort",
  
  "c00_perinatal_mortality",        TRUE,       "child_mortality_cohort",
  "c00_neonatal_mortality",         TRUE,       "child_mortality_cohort",
  "c00_infant_mortality",           TRUE,       "child_mortality_cohort"
)

