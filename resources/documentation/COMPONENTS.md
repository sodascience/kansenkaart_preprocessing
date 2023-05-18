# KansenKaart preprocessing components
To create the cohorts, we utilize the KansenKaart preprocessing pipeline, which consists of four components. Below, we provide a detailed description of these four components.


## 1. Cohort creation
In the cohort creation component, we initiate the process by defining each cohort, which consists of Dutch individuals born within a specific birth date interval (`child_birth_date_min` - `child_birth_date_max`). To gather the required information, we utilize the GBAPERSOONTAB microdata file, which is a municipal population register containing data on individuals registered with a Dutch municipality.

During cohort creation, we exclude individuals who have either emigrated or passed away within a specified period (`child_live_start` - `child_live_end`). This ensures that we only include individuals who have continuously resided in the Netherlands during the period in which we measure the outcomes (`live_continuously`). To account for temporary absences, we allow for a certain number of days (`child_live_slack_days`) in which children may not reside in the Netherlands.

To establish parent-child relationships, we match individuals to their legal parents using the KINDOUDERTAB microdata. This dataset provides information on the legal parents of individuals. Individuals who lack at least one legal parent are excluded from our cohort. Additionally, we narrow down our cohort to individuals whose parents fall within a specific age interval at the time of childbirth (`parent_min_age_at_birth` - `parent_max_age_at_birth`).

To ensure consistency, we assign children to their home address registration using the GBAADRESOBJECTBUS microdata. The definition of a home address registration may vary for each cohort (childhood_home). Our focus is on the geographical locations where children spent their formative years, regardless of their current place of residence.

To enrich our cohort with geographical information, we link several measures of geographical location, including municipalities (VSLGWBTAB microdata), neighborhoods, postal codes (VSLPOSTCODEBUS microdata), and corop regions ([`gemeenten_corop_1981.xlsx`](https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/gemeenten_corop_1981.xlsx)). It's important to note that geographical location definitions in the Netherlands can vary from year to year. Therefore, we use a specific target date (`postcode_target_date` & `gwb_target_date`) to determine the geographical location. Any children who cannot be linked to their home addresses are excluded from the cohort.


## 2. Predictor creation
In the predictor component, we incorporate variables from various microdata sets into the cohort, which serve as predictors for our estimates. The primary predictor in our estimates is parental income, obtained from the IPI and INPATAB microdata. Parental income represents the average pretax income of the mother and/or father combined over a specific period interval (`parent_income_year_min` - `parent_income_year_max`). Individuals with parents who do not have observable income within the specified interval are excluded from the cohort.

We apply several restrictions to the income definition:
1. Income values of 999999999 (indicating a household with no perceived income) or negative income values (below 0) are converted to NA (not available).
2. Income values above the specified censoring limit in euros (`income_censoring_value`) are truncated.
3. Parental income is adjusted for inflation using the consumer price index (CPI) from CBS Statline (cpi_base_year).

Another predictor in our estimates is parental wealth, obtained from the IHI and INHATAB microdata. Parental wealth represents the average pretax wealth of the mother and/or father combined over a specific period interval (`parent_wealth_year_min` - `parent_wealth_year_max`). The same restrictions applied to parental income are also imposed on parental wealth.

We introduce a third generation to the variable `GBAGENERATIE` from the GBAPERSOONTAB microdata for individuals in the cohort. If a child is native (autochtoon) and at least one of their parents is a second-generation immigrant (tweede generatie allochtoon), we reclassify the child's generation as a third-generation immigrant (derde generatie allochtoon). Essentially, we change the child's generation from native to third generation if at least one parent is a second-generation immigrant and the child is native.

For children who are third-generation immigrants, we recode their origin (variable `GBAHERKOMSTGROEPRING`) as follows:

- If the child is third generation and the father's origin is native, the child adopts the mother's origin.
- If the child is third generation and the mother's origin is native, the child adopts the father's origin.
- If the child is third generation and both parents' origins are non-native, the child adopts the mother's origin.

In our analysis, we consider seven migration background categories based on the variable `GBAHERKOMSTGROEPERING` from the GBAPERSOONTAB microdata:

1. All: Total of all migration backgrounds
2. Native (Nederlands)
3. Turkey (Turkije)
4. Morocco (Marokko)
5. Surinam (Suriname)
6. Antilles (Nederlandse Antillen (oud))
7. has_migration

We utilize the CBS resource file [`LANDAKTUEELREFV10.sav`](https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/LANDAKTUEELREF10.sav) from the CBS environment to classify countries as either western or non-western.

To determine the child's household composition, we include data on the number of parents the child grows up with in the cohort. This information helps us identify whether the child was raised in a single-parent household or a two-parent household (`TYPHH` from GBAHUISHOUDENSBUS microdata).

Furthermore, we incorporate parental education into the cohort using the CBS microdata HOOGSTEOPLTAB. However, we can only ascertain the following levels of parental education: neither WO (academic education) nor HBO (higher professional education), HBO, and WO. This is due to the availability of data on parental education, which began in 1983 for WO, 1986 for HBO, and 2004 for MBO (intermediate vocational education). The specific level of education falling under "neither HBO nor WO" cannot be further differentiated.


## 3. Outcome creation
In the outcome creation component, we incorporate variables from various microdata sets that serve as outcomes for our estimates. The specific outcomes we include vary for each cohort. Further details regarding the outcomes used for each cohort can be found [here](https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/documentation/COHORTS.md). 


## 4. Post-processing
In the post-processing component, we select variables from the created cohort data sets that are relevant for our estimates.
