# KansenKaart preprocessing components
To create the cohorts, we use the KansenKaart preprocessing pipeline that has four components. Here, we describe these four components in greater detail. 


## 1. Cohort creation
In the cohort creation component, we start with defining each cohort which consists of Dutch individuals who were born within our target birth date interval (`child_birth_date_min` - `child_birth_date_max`). We use the GBAPERSOONTAB microdata file which is a municipal population register that has information on all individuals who were registered at a Dutch municipality. 

We exclude individuals who emigrated or died within a period (`child_live_start` - `child_live_end`) – that is, we only include individuals who live continuously in the Netherlands in the period we measure the outcomes (`live_continuously`). 

We match individuals to their parents using the KINDOUDERTAB microdata. This microdata file provides information on the legal parents of the individuals. Individuals without at least one legal parent are removed from our cohort. We further restrict our cohort to individuals with parents that were between the age interval at childbirth (`parent_min_age_at_birth` - `parent_max_age_at_birth`). 

We permanently assign children to their home address registration using the GBAADRESOBJECTBUS microdata. The home address registration definition is different for each cohort (`childhood_home`). We focus on geographical locations where children grew up, regardless of where they live later in life. 

We link a few measures of geographical location to our cohort, including municipalities (VSLGWBTAB microdata), neighborhoods, postal codes (VSLPOSTCODEBUS microdata), and corop regions ([`gemeenten_corop_1981.xlsx`]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/gemeenten_corop_1981.xlsx)). The geographical location definitions in the Netherlands may be different from year to year. Therefore, we use a geographical location definition at a certain target date (`postcode_target_date` & `gwb_target_date`). Children who cannot be linked to their home addresses are removed from the cohort. 


## 2. Predictor creation
In the predictor component, we add variables from several microdata to the cohort that serve as predictors for our estimates. The primary predictor in our estimates is parental income (IPI and INPATAB microdata). Parental income measures the average pretax income of the mother and/or the father combined over a period interval (`parent_income_year_min` - `parent_income_year_max`). 

We impose a few income definition restrictions: 1) we convert income with the value 999999999 (the value 999999999 means that the person belongs to a household with no perceived income) or with negative income (income below 0) to NA (not available), 2) we censor income above the limit of euros (`income_censoring_value`), and 3) we adjust parental income for inflation using the [consumer price index (CPI)]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/Consumentenprijzen__prijsindex_2015_100_07012021_123946.csv) from CBS Statline (`cpi_base_year`). We remove individuals with parents without an observable income within the period interval.

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
In the outcome creation component, we add variables from several microdata sets that serve as outcomes for our estimates. The outcomes we add are different for each cohort. More information on the outcomes we use for each cohort can be found [here](https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/COHORTS.md). 

## 1. Main cohort
We begin by defining child income in the same way as parental income. We average child income over the last two years in our data (`child_income_year_min` - `child_income_year_max`) when children are in their early thirties  (IPI and INPATAB microdata). 

Similar to parental income, we impose a few income definition restrictions: 1) we convert income with the value 999999999 (the value 999999999 means that the person belongs to a household with no perceived income) or with negative income (income below 0) to NA (not available), 2) we censor income above the limit of euros (`income_censoring_value`), and 3) we adjust parental income for inflation using the [consumer price index (CPI)]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/Consumentenprijzen__prijsindex_2015_100_07012021_123946.csv) from CBS Statline (`cpi_base_year`). We remove children with no observable child income from our cohort. 

We use HOOGSTEOPLTAB microdata to define our higher education outcomes. We define HBO education as an indicator for attaining at least an HBO degree (higher professional education). We define university education as an indicator for attaining at least a university degree (scientific education). 

We use SPOLISBUS microdata to define our economic productivity outcomes. We define hourly wage as the average wage one earned per hour in a period (`child_income_year_min` - `child_income_year_max `). We estimate the hourly wage by the sum of the pretax wage adjusted for inflation divided by the sum of the working hours. We define flex contract as an indicator for having a  temporary contract instead of a permanent contract. We keep the contract with the highest sum of hours worked. For individuals with the same total number of hours worked for both temporary and permanent contracts, we keep the flex contract observation and drop the permanent contract observation. We define hours worked per week as the average total hours one has worked per week. We estimate the hours worked per week by the sum of working hours divided by the total numbers of weeks. 

We create two additional indicators: we define working as an indicator for having a positive hourly wage, and we define hours worked per week with no missing values as the average total hours one has worked per week. The difference between this outcome and the hours per week outcome is that the hours per week outcome is based on the cohort of individuals who are employed, the hours per week with no missing values is based on a sample of both individuals who are employed and unemployed. 

We use SECMBUS microdata to define our socioeconomic outcomes. We measure the socioeconomic status of individuals at our target date (`secm_ref_date`). We define employed as an indicator for being employed. We define disability as an indicator for receiving disability benefits. We define social benefits as an indicator for receiving social benefits.

We use ZVWZORGKOSTENTAB microdata to define our health costs outcomes. We define specialist costs as an indicator for having costs of specialist care within the basic health insurance. We define basis GGZ costs as an indicator for having costs of generalist basic mental healthcare within the basic health insurance. We define pharmaceutical costs as an indicator for having costs of pharmacy within the basic health insurance. We define hospital costs as an indicator for having costs of hospital care within the basic health insurance. We define total health costs as the average total health costs. 


## 2. Perinatal cohort
We use PRNL microdata to define our perinatal outcomes. We restrict the cohort to gestational age between our chosen day interval (`cut_off_days_min` - ` cut_off_days_max`). We also remove deaths occurring up to a few days after birth (`cut_off_mortality_day`). To determine the date of death of the child, we use DO, DOODOORZTAB, and GBAOVERLIJDENSTAB microdata. The perinatal cohort consists of two outcomes. We define low birth weight as an indicator for having a birth weight below the 10th percentile of the birth weight percentile conditionally on gestational age and sex according to the [Hoftiezer curve]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/Hoftiezer_Geboortegewicht%20curves.xlsx). We define premature birth as a gestational age before 37 completed weeks of gestation.

Another predictor for the perinatal cohort is the household income of the mother (IHI and INHATAB microdata). Household income measures the average household income of the mother over a period interval (`parent_income_year_min` - ` parent_income_year_max`). To link household income to our cohort, we use data from INPATAB which serves as a ‘bridge’ between INHATAB and our cohort. Similar to parental income, we impose a few income definition restrictions: 1) we convert household income with the value 999999999 (the value 999999999 means that the person belongs to a household with no perceived income) or with negative income (income below 0) to NA (not available), 2) we censor income above the limit of euros (`income_censoring_value`), and 3) we adjust household income for inflation using the [consumer price index (CPI)]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/Consumentenprijzen__prijsindex_2015_100_07012021_123946.csv) from CBS Statline (`cpi_base_year`). We remove children with no observable mother’s household income from our cohort. 


## 3. High school cohort
We use HOOGSTEOPLTAB microdata to define our high school outcomes. We measure the high school education level of individuals at the age of 16. We define VMBO-high plus as an indicator for following at least a VMBO-high high school education. We define HAVO plus as an indicator for following at least an HAVO high school education. We define VWO plus as an indicator for following at least a VWO high school education.

## 4. Elementary school cohort
We use INSCHRWPOTAB microdata to define our elementary school outcomes. We restrict the cohort to students who are in group 8 of Dutch elementary school. We then pick the latest observation of duplicated students who repeated the school year in group 8. In other words, we remove the test scores of the first year and keep the test scores in the following year for students who appear twice in the data. The elementary school cohort consists of 11 outcomes.

We define WPO math as an indicator for having at least the required level of math. We define WPO reading as an indicator for having at least the required level of reading. We define WPO language as an indicator for having at least the required level of language.

In the Netherlands, the teacher determines first the school advice for the students before they take the final tests. The teacher can adjust the final school advice upwards after the final tests if the test results show a reason to do so. In our cohort, we replace the teacher’s school advice with the variable WPOADVIESHERZ if the teacher’s advice has been revised.

We define VMBO hoog plus test as an indicator for having test advice of at least VMBO-high or higher. We define HAVO plus test as an indicator for having test advice of at least HAVO or higher. We define VWO plus test as an indicator for having test advice of VWO. We define VMBO hoog plus final as an indicator for having a final school advice of at least VMBO hoog plus or higher. We define HAVO plus final as an indicator for having a final school advice of at least HAVO or higher. We define VWO plus final as an indicator for having a final school advice of VWO.

We define under advice as an indicator for having a final school advice that is lower than the test school advice. We define over advice as an indicator for having a final school advice that is higher than the test school advice. To determine whether the final school advice is higher/ lower than the test school advice, we have created our own [advice table]( https://github.com/sodascience/kansenkaart_preprocessing/blob/main/resources/vo_advisering.xlsx) in the [resource] folder. 


## 4. Post-processing
In the post-processing component, we select variables from the created cohort data sets that are relevant for our estimates.
