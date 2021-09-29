# Cohort description
So far, we have 34 outcomes derived from five cohorts. Here, we briefly describe the five cohorts and the outcomes. 

## 1. Main cohort
The main cohort consists of approximately one million Dutch individuals in their early 30s who were born between 1982 and 1987. We assign these individuals to their earliest known home address, that is, where they live in 1995. Parental income is measured between 2003 and 2007. There are 15 outcomes in the main cohort:

1. Child income: the average pretax income between 2017 and 2018 measured in 2018 euros.
2. HBO education: attained at least an HBO degree (higher professional education) in 2018.
3. University education: attained at least a university degree in 2018.
4. Employed: being employed in 2018.
5. Disability: received disability benefits in 2018. 
6. Social benefits: received social benefits in 2018.
7. Specialist costs: had costs of specialist care within the basic insurance in 2018.
8. Basis GGZ costs: had costs of generalist basic mental healthcare within the basic insurance in 2018. 
9. Pharmaceutical costs: had costs of pharmacy within the basic insurance in 2018.
10. Hospital costs: had costs of hospital care within the basic insurance in 2017.
11. Total health costs: the average total health costs in 2018 measured in 2018 euros.
12. Hourly wage: the average wage one earned per hour in 2017 and 2018 measured in 2018 euros.
13. Hours worked per week: the average total hours one has worked per week in 2017 and 2018.
14. Flex contract: had a temporary contract instead of a permanent contract in 2018. 
15. Working: had a positive hourly wage in 2017 and 2018, that is, one was working in 2017 and 2018 

## 2. Perinatal cohort
The perinatal cohort consists of approximately 1.5 million Dutch children who were born between 2008 and 2016. We assign these children to their home address where they live at birth. Parental income is measured between 2014 and 2018. There are two outcomes in the perinatal cohort:

1. Low birth weight/ small-for-gestational age: a birth weight below the 10th percentile adjusted for gestational age and gender, according to the [Hoftiezer curve]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/Hoftiezer_Geboortegewicht%20curves.xlsx). 
2. Premature birth: a gestational age before 37 completed weeks of gestation. 

## 3. High school cohort
The high school cohort consists of approximately one million Dutch 16-year-old high school students who were born between 1998 and 2002. We assign these high school students to their home address where they live on December 31, 2015. Parental income is measured between 2014 and 2018. There are three outcomes in the high school cohort that we measure when these students are at age 16:

1. VMBO-high plus: followed at least a VMBO-high level of high school education at age 16.
2. HAVO plus: followed at least a HAVO level of high school education at age 16.
3. VWO plus: followed at least a VWO level of high school education at age 16.

## 4. Elementary school cohort
The elementary school cohort consists of approximately 337 thousand elementary school students who were born between 2004 and 2005. We assign these elementary school students to their home address where they live on December 31, 2015. Parental income is measured between 2014 and 2018. There are 11 outcomes in the elementary school cohort that we measure when these students are in group 8 of the Dutch elementary school:

1. WPO math: had at least the required level of math in group 8.
2. WPO reading: had at least the required level of reading in group 8.
3. WPO language: had at least the required level of language in group 8.
4. VMBO high plus test: had test advice of at least VMBO-high or higher level of high school education.
5. HAVO plus test: had test advice of at least HAVO or higher level of high school education.
6. VWO plus test: had test advice of VWO level of high school education.
7. VMBO high plus final: had final school advice of at least VMBO-high plus or higher level of high school education.
8. HAVO plus final: had final school advice of at least HAVO or higher level of high school education.
9. VWO plus final: had final school advice of VWO level of high school education.
10. Under advice: had final school advice that is lower than the test school advice. To determine whether a student was under advised, we use the [advice table]( https://github.com/sodascience/kansenkaart_preprocessing/blob/main/resources/vo_advisering.xlsx) that we have created. 
11. Over advice: had final school advice that is higher than the test school advice. To determine whether a student was over advised, we use the [advice table]( https://github.com/sodascience/kansenkaart_preprocessing/blob/main/resources/vo_advisering.xlsx) that we have created.

## 5. Child mortality cohort
The child mortality cohort consists of approximately 1.5 million Dutch children who were born between 2008 and 2016. We assign these children to their home address where they live at birth. Parental income is measured between 2014 and 2018. There are three outcomes in the child mortality cohort:

1. Perinatal mortality: death that occurs between 24 completed weeks of gestation and up to 7 days after birth.
2. Neonatal mortality: death that occurs between 24 completed weeks of gestation and up to 28 days after birth.
3. Infant mortality: death that occurs between 24 completed weeks of gestation and up to 365 days after birth.

Note that the child mortality cohort uses different components that the other cohorts since the base data set of the child mortality cohort consists of individuals from the PRNL microdata instead of the GBAPERSOONTAB microdata. 
