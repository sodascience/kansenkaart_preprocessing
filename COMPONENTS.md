# KansenKaart preprocessing components
To create the cohorts, we use the KansenKaart preprocessing pipeline that has four components. Here, we describe these four components in greater detail. 


## 1. Cohort creation
In the cohort creation component, we start with defining each cohort which consists of Dutch individuals who were between our target birth date interval (`child_birth_date_min` - `child_birth_date_max`). We use the GBAPERSOONTAB microdata file which is a municipal population register that has information on all individuals who were registered at a Dutch municipality. 

We exclude individuals who emigrated or died in a certain period (`child_live_start` - `child_live_end`) – that is, we only include individuals who live continuously in the Netherlands in the period we measure the outcomes (`live_continuously`). 

We match individuals to their parents using the KINDOUDERTAB cbsdata This microdata file provides information on the legal parents of the individuals. Individuals without at least one legal parent are removed from our cohort. We further restrict our cohort to individuals with parents that were between a certain age interval at childbirth (`parent_min_age_at_birth` - `parent_max_age_at_birth`). 

We permanently assign children to their home address registration using the GBAADRESOBJECTBUS. The home address registration definition is different for each cohort (`childhood_home`). We focus on geographical locations where children grew up, regardless of where they live later in life. We link a few measures of geographical location to our cohort, including municipalities (VSLGWBTAB), neighborhoods, postal codes (VSLPOSTCODEBUS), and corop regions ([`gemeenten_corop_1981.xlsx`]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/gemeenten_corop_1981.xlsx)). The geographical location definitions in the Netherlands may be different from year to year. Therefore, we use a geographical location definition at a certain target date (`postcode_target_date` & `gwb_target_date`). Children who cannot be linked to their home addresses are removed from the cohort. 


## 2. Predictor creation
In the predictor component, we add variables to the cohort that serve as predictors for our estimates. The primary predictor in our estimates is parental income. Parental income measures the average pretax income of the mother and/or the father combined between a period interval (`parent_income_year_min` - `parent_income_year_max`). 

We impose a few income definition restrictions: 1) we convert income with the value 999999999 (the value 999999999 means that the person belongs to a household with no perceived income) or with negative income (income below 0) to NA (not available), 2) we censor income above the limit of euros (`income_censoring_value`), and 3) we adjust parental income for inflation using the [consumer price index (CPI)]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/Consumentenprijzen__prijsindex_2015_100_07012021_123946.csv) from CBS Statline (`cpi_base_year`). We remove individuals with parents without an observable income within the period interval.

We add a third generation to the variable `GBAGENERATIE` from GBAPERSOONTAB for individuals from the cohort. If the child is native (autochtoon) and at least one of the parents of children is a second-generation immigrant (tweede generatie allochtoon), then we recode the generation of the child to third-generation immigrant (derde generatie allochtoon). In other words, we replace the generation of children from native to third-generation if at least one of the parents is a second-generation immigrant and the child is native.

If the child is a third-generation immigrant, we recode the origin of the child (the variable `GBAHERKOMSTGROEPRING`) as follow:
- If the child is third generation & dad origin is native then the child gets mom origin
- If the child is third generation & mom origin is native then the child gets dad origin
- If the child is third generation & both mom and dad origin are not native then the child gets mom origin

We have nine migration background categories in our analysis. Migration background is based on the variable `GBAHERKOMSTGROEPERING` from GBAPERSOONTAB. 
1. All = Total of all migration backgrounds
2. Native (Nederlands)
3. Turkey (Turkije)
4. Morocco (Marokko)
5. Surinam (Suriname)
6. Antilles (Nederlandse Antillen (oud))
7. Others western (Overig westers) – All other western countries except for the Netherlands
8. Others nonwestern (Overig niet-westers) = all other nonwestern countries except for Turkey, Morocco, Surinam, and the Antilles. 
9. Total nonwestern (Totaal niet-westers) = Turkey, Morocco, Surinam, the Antilles, and others nonwestern combined

We use the CBS resource file [`LANDAKTUEELREFV10.sav`]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/LANDAKTUEELREF10.sav) from the CBS environment to determine whether a country is a western or a non-western country


## 3. Outcome creation
In the outcome creation component, we add variables that serve as outcomes for our estimates. The outcomes we add are different for each cohort. More information on the outcomes we use for each cohort can be found [here](https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/COHORTS.md). 


## 4. Post-processing
In the post-processing component, we select variables from the cohort datasets that are relevant for the estimates. 
