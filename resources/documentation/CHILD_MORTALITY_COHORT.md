# KansenKaart preprocessing components – Child mortality cohort
To create the child mortality cohort, we use the KansenKaart preprocessing pipeline that has four components. Here, we describe these four components in greater detail. The child mortality cohort uses a different pipeline since this cohort is defined differently than the other cohorts. 


## 1. Cohort creation
In the cohort creation component, we start with defining each cohort which consists of Dutch individuals who were born within our target birth date interval (`child_birth_date_min` - `child_birth_date_max`). We use the PRNL microdata file which consists of all children born after a gestational age of 22 weeks or more, and whose mother was registered in the municipal population register. We restrict the cohort to gestational age between our chosen day interval (`cut_off_days_min` - ` cut_off_days_max`). 

To determine the date of death of the child, we use DO, DOODOORZTAB, and GBAOVERLIJDENSTAB microdata. We further restrict our cohort to individuals with mothers that were between the age interval at childbirth (`parent_min_age_at_birth` - `parent_max_age_at_birth`). 

We permanently assign children to their home address registration using the GBAADRESOBJECTBUS microdata. We assign these children to their home address where they live at their date of birth. We focus on geographical locations where children grew up, regardless of where they live later in life. For children who died, and therefore, cannot be assigned to a home address registration, we replace the home address registration with the one of the mothers. 

We link a few measures of geographical location to our cohort, including municipalities (VSLGWBTAB microdata), neighborhoods, postal codes (VSLPOSTCODEBUS microdata), and corop regions ([`gemeenten_corop_1981.xlsx`]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/gemeenten_corop_1981.xlsx)). The geographical location definitions in the Netherlands may be different from year to year. Therefore, we use a geographical location definition at a certain target date (`postcode_target_date` & `gwb_target_date`). Children who cannot be linked to their home addresses are removed from the cohort.


## 2. Predictor creation
In the predictor component, we add variables from several microdata to the cohort that serve as predictors for our estimates. The primary predictor in our estimates is mother’s household income (IHI and INHATAB microdata).  Household income measures the average household income of the mother over a period interval (`parent_income_year_min` - `parent_income_year_max`). To link household income to our cohort, we use data from INPATAB which serves as a ‘bridge’ between INHATAB and our cohort. Children with no observable mother’s household income are removed from our cohort.

We impose a few income definition restrictions: 
1. We convert household income with the value 999999999 (the value 999999999 means that the person belongs to a household with no perceived income) or with negative income (income below 0) to NA (not available).
2. We censor income above the limit of euros (`income_censoring_value`).
3. We adjust household income for inflation using the [consumer price index (CPI)]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/Consumentenprijzen__prijsindex_2015_100_07012021_123946.csv) from CBS Statline (`cpi_base_year`). 

We add a third generation to the variable `GBAGENERATIE` from GBAPERSOONTAB microdata for individuals from the cohort. If the child is native (autochtoon) and at least one of the parents of children is a second-generation immigrant (tweede generatie allochtoon), then we recode the generation of the child to third-generation immigrant (derde generatie allochtoon). In other words, we replace the generation of children from native to third-generation if at least one of the parents is a second-generation immigrant and the child is native.

If the child is a third-generation immigrant, we recode the origin of the child (the variable `GBAHERKOMSTGROEPRING`) as follows:
- If the child is third generation & dad origin is native then the child gets mom origin.
- If the child is third generation & mom origin is native then the child gets dad origin.
- If the child is third generation & both mom and dad origin are not native then the child gets mom origin.

We have nine migration background categories in our analysis. Migration background is based on the variable `GBAHERKOMSTGROEPERING` from GBAPERSOONTAB microdata. 
1. All = Total of all migration backgrounds
2. Native (Nederlands)
3. Turkey (Turkije)
4. Morocco (Marokko)
5. Surinam (Suriname)
6. Antilles (Nederlandse Antillen (oud))
7. Others western (Overig westers) = All other western countries except for the Netherlands
8. Others nonwestern (Overig niet-westers) = All other nonwestern countries except for Turkey, Morocco, Surinam, and the Antilles
9. Total nonwestern (Totaal niet-westers) = Turkey, Morocco, Surinam, the Antilles, and others nonwestern combined

We use the CBS resource file [`LANDAKTUEELREFV10.sav`]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/LANDAKTUEELREF10.sav) from the CBS environment to determine whether a country is a western or a non-western country.

## 3. Outcome creation
The child mortality cohort consists of three outcomes. We define perinatal mortality as a death that occurs between 24 completed weeks of gestation and up to 7 days after birth. We define neonatal mortality as a death that occurs between 24 completed weeks of gestation and up to 28 days after birth. We define infant mortality as a death that occurs between 24 completed weeks of gestation and up to 365 days after birth.
 

## 4. Post-processing
In the post-processing component, we select variables from the created cohort data sets that are relevant for our estimates.
