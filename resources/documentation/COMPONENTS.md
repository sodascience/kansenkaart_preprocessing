---
output:
  pdf_document: default
  html_document: default
---
# KansenKaart preprocessing components
To create the cohorts, we use the KansenKaart preprocessing pipeline that has four components. Here, we describe these four components in greater detail. 


## 1. Cohort creation
In the cohort creation component, we start with defining each cohort which consists of Dutch individuals who were born within our target birth date interval (`child_birth_date_min` - `child_birth_date_max`). We use the GBAPERSOONTAB microdata file which is a municipal population register that has information on all individuals who were registered at a Dutch municipality. 

We exclude individuals who emigrated or died within a period (`child_live_start` - `child_live_end`) – that is, we only include individuals who live continuously in the Netherlands in the period we measure the outcomes (`live_continuously`). We allow children to not live in the Netherlands up to a certain number of days (`child_live_slack_days`).

We match individuals to their parents using the KINDOUDERTAB microdata. This microdata file provides information on the legal parents of the individuals. Individuals without at least one legal parent are removed from our cohort. We further restrict our cohort to individuals with parents that were between the age interval at childbirth (`parent_min_age_at_birth` - `parent_max_age_at_birth`). 

We permanently assign children to their home address registration using the GBAADRESOBJECTBUS microdata. The home address registration definition is different for each cohort (`childhood_home`). We focus on geographical locations where children grew up, regardless of where they live later in life. 

We link a few measures of geographical location to our cohort, including municipalities (VSLGWBTAB microdata), neighborhoods, postal codes (VSLPOSTCODEBUS microdata), and corop regions ([`gemeenten_corop_1981.xlsx`]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/gemeenten_corop_1981.xlsx)). The geographical location definitions in the Netherlands may be different from year to year. Therefore, we use a geographical location definition at a certain target date (`postcode_target_date` & `gwb_target_date`). Children who cannot be linked to their home addresses are removed from the cohort. 


## 2. Predictor creation
In the predictor component, we add variables from several microdata to the cohort that serve as predictors for our estimates. The primary predictor in our estimates is parental income (IPI and INPATAB microdata). Parental income measures the average pretax income of the mother and/or the father combined over a period interval (`parent_income_year_min` - `parent_income_year_max`). Individuals with parents without an observable income within the period interval are removed from the cohort.

We impose a few income definition restrictions: 
1. We convert income with the value 999999999 (the value 999999999 means that the person belongs to a household with no perceived income) or with negative income (income below 0) to NA (not available).
2. We censor income above the limit of euros (`income_censoring_value`).
3. We adjust parental income for inflation using the [consumer price index (CPI)]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/Consumentenprijzen__prijsindex_2015_100_07012021_123946.csv) from CBS Statline (`cpi_base_year`).

Another predictor in our estimates is parental wealth (IHI and INHATAB microdata). Parental wealth measures the average pretax wealth of the mother and/or the father combined over a period interval (`parent_wealth_year_min` - `parent_wealth_year_max`). We impose the same restrictions on parental wealth as parental income. 

We add a third generation to the variable `GBAGENERATIE` from GBAPERSOONTAB microdata for individuals from the cohort. If the child is native (autochtoon) and at least one of the parents of children is a second-generation immigrant (tweede generatie allochtoon), then we recode the generation of the child to third-generation immigrant (derde generatie allochtoon). In other words, we replace the generation of children from native to third generation if at least one of the parents is a second-generation immigrant and the child is native.

If the child is a third-generation immigrant, we recode the origin of the child (the variable `GBAHERKOMSTGROEPRING`) as follows:
- If the child is third generation & dad's origin is native then the child gets mom origin.
- If the child is third generation & mom's origin is native then the child gets dad origin.
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

We add data on the number of parents the child grows up with to the cohort to determine whether the child grew up in a single-parent household or a two-parent household (`TYPHH` from GBAHUISHOUDENSBUS microdata). 

Lastly, we add parental education to the cohort from the CBS microdata HOOGSTEOPLTAB. We can only determine the following parental education level: neither WO nor HBO, HBO, and WO, since data on parental education has only been available from 1983 for WO, 1986 for HBO, and 2004 for MBO. The level of education, neither HBO nor WO, cannot be differentiated further.

## 3. Outcome creation
In the outcome creation component, we add variables from several microdata sets that serve as outcomes for our estimates. The outcomes we add are different for each cohort. More information on the outcomes we use for each cohort can be found [here](https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/documentation/COHORTS.md). 

### 1. Main cohort
We begin by defining child income in the same way as parental income. We average child income over the last two years in our data (`child_income_year_min` - `child_income_year_max`) when children are in their early thirties (IPI and INPATAB microdata). Children with no observable child income are removed from our cohort. 

Similar to parental income, we impose a few income definition restrictions: 
1. We convert income with the value 999999999 (the value 999999999 means that the person belongs to a household with no perceived income) or with negative income (income below 0) to NA (not available).
2. We censor income above the limit of euros (`income_censoring_value`).
3. We adjust parental income for inflation using the [consumer price index (CPI)]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/Consumentenprijzen__prijsindex_2015_100_07012021_123946.csv) from CBS Statline (`cpi_base_year`). 

We use HOOGSTEOPLTAB microdata to create our higher education outcomes. We define HBO education as an indicator for attaining at least an HBO degree (higher professional education). We define university education as an indicator for attaining at least a university degree (scientific education). 

We use SPOLISBUS microdata to create our economic productivity outcomes. We define hourly wage as the average wage one earned per hour in a period (`child_income_year_min` - `child_income_year_max `). We estimate the hourly wage by the sum of the pretax wage adjusted for inflation divided by the sum of the working hours. We define flex contract as an indicator of having a  temporary contract instead of a permanent contract. We keep the contract with the highest sum of hours worked. For individuals with the same total number of hours worked for both temporary and permanent contracts, we keep the flex contract observation and drop the permanent contract observation. We define hours worked per week as the average total hours one has worked per week. We estimate the hours worked per week by the sum of working hours divided by the total number of weeks. 

We create two additional indicators: we define working as an indicator of having a positive hourly wage, and we define hours worked per week with no missing values as the average total hours one has worked per week. The difference between this outcome and the hours per week outcome is that the hours per week outcome is based on the cohort of individuals who are employed, and the hours per week with no missing values is based on a sample of both individuals who are employed and unemployed. 

We use SECMBUS microdata to create our socioeconomic outcomes. We measure the socioeconomic status of individuals at our target date (`secm_ref_date`). We define employed as an indicator of being employed. We define disability as an indicator for receiving disability benefits. We define social benefits as an indicator of receiving social benefits.

We use ZVWZORGKOSTENTAB microdata to create our health cost outcomes. We define specialist costs as an indicator of having costs of specialist care within basic health insurance. We define basis GGZ costs as an indicator of having costs of generalist basic mental healthcare within the basic health insurance. We define pharmaceutical costs as an indicator of having costs of pharmacy within the basic health insurance. We define hospital costs as an indicator of having costs of hospital care within basic health insurance. We define total health costs as the sum of health costs. 

We use VEHTAB microdata to create the wealth and debt outcomes. We define wealth as the total wealth of a household. We define debt as an indicator of having debt. 

We use EIGENDOMTAB microdata to create the homeowner outcome. We define homeowner as people who own a house. 

### 2. students cohort
We use HOOGSTEOPLTAB microdata to create our students' outcomes. We define high school attained as an indicator of having obtained a basic high school qualification (havo, vwo, mbo-2). We define hbo followed as an indicator of having followed HBO or university education at age 21. We define wo followed as an indicator of having followed university education at age 21.

### 3. High school cohort
We use HOOGSTEOPLTAB microdata to create our high school outcomes. We measure the high school education level of individuals at the age of 16. We define VMBO-high plus as an indicator for following at least a VMBO-high high school education. We define HAVO plus as an indicator for following at least an HAVO high school education. We define VWO plus as an indicator for following at least a VWO high school education.

### 4. Elementary school cohort
We use INSCHRWPOTAB microdata to define our elementary school outcomes. We restrict the cohort to students who are in group 8 of Dutch elementary school. We then pick the latest observation of duplicated students who repeated the school year in group 8. In other words, we remove the test scores of the first year and keep the test scores in the following year for students who appear twice in the data. The elementary school cohort consists of 11 outcomes.

We define math as an indicator of having at least the required level of math. We define reading as an indicator of having at least the required level of reading. We define language as an indicator of having at least the required level of language.

In the Netherlands, the teacher determines first the school advice for the students before they take the final tests. The teacher can adjust the final school advice upwards after the final tests if the test results show a reason to do so. In our cohort, we replace the teacher’s school advice with the variable WPOADVIESHERZ if the teacher’s advice has been revised.

We define VMBO hoog plus test as an indicator of having test advice of at least VMBO-high or higher. We define HAVO plus test as an indicator of having test advice of at least HAVO or higher. We define VWO plus test as an indicator of having test advice of VWO. We define VMBO hoog plus final as an indicator of having a final school advice of at least VMBO hoog plus or higher. We define HAVO plus final as an indicator of having a final school advice of at least HAVO or higher. We define VWO plus final as an indicator of having a final school advice of VWO.

We define under advice as an indicator of having a final school advice that is lower than the test school advice. We define over advice as an indicator of having a final school advice that is higher than the test school advice. To determine whether the final school advice is higher/ lower than the test school advice, we have created our own [advice table]( https://github.com/sodascience/kansenkaart_preprocessing/blob/main/resources/vo_advisering.xlsx) in the [resource] folder. 

### 5. classroom cohort
We use INSCHRWPOTAB microdata to define our classroom outcomes.  These outcomes are based on the classroom of the child. We first create a separate sample with these classroom outcomes before we merge it with the elementary school sample. We restrict the cohort to students who are in group 8 of Dutch elementary school.

#### Hold-out means
We estimate the classroom outcomes (the variables: VMBO hoog plus test, HAVO plus test, VWO plus test, total non-western, below 25th, and above 75th, math, language, and reading, except for the classroom size outcome) with the hold-out means method. Hold-out means method is the average outcome of the classroom without the child her/himself in it. This means that we can compare the child to the average outcome of his/ her classroom without the child influencing the averages of the classroom. We estimate the hold-out means as follows:


\begin{equation}
\frac{sum(outcome_{c}) -  outcome_{i}}{N(outcome_{c}) - 1} 
\end{equation}
 
- $sum(outcome_{c})$ = the sum of an outcome estimates of the classroom
- $outcome_{i}$ = outcome estimates of the student 
- $N(outcome_{c})$ = total number of students in the classroom
- $minus 1$ = subtract the student her/himself from the total number of students in a classroom 

The classroom sample consists of 10 outcome variables. The outcomes VMBO hoog plus test, HAVO plus test, VWO plus test, math, reading, and language are defined the same way as the ones in the elementary school sample. We define class size as the number of students in a classroom. We define foreign-born parents as the percentage of classmates in group 8 whose both parents were born abroad. We define income below 25th as the percentage of classmates in group 8 whose parents earn less than the 25th income percentile. We define income above 75th as the percentage of classmates in group 8 whose parents earn more than the 75th income percentile.

### 6. Perinatal cohort
We use PRNL microdata to define our perinatal outcomes. We restrict the cohort to gestational age between our chosen day interval (`cut_off_days_min` - ` cut_off_days_max`). We also remove deaths occurring up to a few days after birth (`cut_off_mortality_day`). To determine the date of death of the child, we use DO, DOODOORZTAB, and GBAOVERLIJDENSTAB microdata. The perinatal cohort consists of two outcomes. We define low birth weight as an indicator of having a birth weight below the 10th percentile of the birth weight percentile conditionally on gestational age and sex according to the [Hoftiezer curve]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/Hoftiezer_Geboortegewicht%20curves.xlsx). We define premature birth as a gestational age before 37 completed weeks of gestation.

Another predictor for the perinatal cohort is the household income of the mother (IHI and INHATAB microdata). Household income measures the average household income of the mother over a period interval (`parent_income_year_min` - ` parent_income_year_max`). To link household income to our cohort, we use data from INPATAB which serves as a ‘bridge’ between INHATAB and our cohort. Children with no observable mother’s household income are removed from our cohort. Similar to parental income, we impose a few income definition restrictions: 
1. We convert household income with the value 999999999 (the value 999999999 means that the person belongs to a household with no perceived income) or with negative income (income below 0) to NA (not available).
2. We censor income above the limit of euros (`income_censoring_value`). 
3. We adjust household income for inflation using the [consumer price index (CPI)]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/Consumentenprijzen__prijsindex_2015_100_07012021_123946.csv) from CBS Statline (`cpi_base_year`). 


## 4. Post-processing
In the post-processing component, we select variables from the created cohort data sets that are relevant for our estimates.
