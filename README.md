# Kansenkaart preprocessing
*Pipeline to create cohorts for the KansenKaart project*

![pipeline.png](pipeline.png)

A reproducible preprocessing pipeline for the [KansenKaart]( https://kansenkaart.nl/) project. The input is raw microdata from CBS (or a synthetic test-version from our repository [cbs_validationdata](https://github.com/sodascience/cbs_validationdata)). The output is several tidy, cleaned up cohort datasets. The cohort datasets serve as the input to the [kansenkaart analysis](https://github.com/sodascience/kansenkaart_analysis).

## What is KansenKaart?
The goal of the [KansenKaart]( https://kansenkaart.nl/) project is to show how circumstances in which Dutch people grew up are related to later-in-life outcomes. 

We begin by measuring the average later-life outcomes (e.g., income, education) of children for each neighborhood by demographic subgroups (gender and migration background) and socioeconomic status (parental income). We focus on municipalities, neighborhoods, and postal codes where children grew up, regardless of where they live later in life. We then map these geographical differences in later-life outcomes across areas within the Netherlands. We present a large number of outcomes in the field of health, education, and economics on the interactive website [KansenKaart.nl]( https://kansenkaart.nl/).

## Installation
- Clone this repository to a folder on your machine
- Unzip the latest version of the built cbs data into the `/cbsdata` folder from [here](https://github.com/sodascience/cbs_validationdata/releases) OR edit the file locations in the `config` files to point to the right files.
- Open the `.Rproj` file, install the `renv` package
- Run `renv::restore()` to install the right versions of all dependencies

## Configuration
Several cohorts can be created using this repository, from the raw microdata files at CBS. The configuration files for each of these cohorts can be found in the [`config`](./config) folder. Several selections and/or date values can also be changed there.

## Cohort creation
The cohort data files are created using the file `create_cohort_data.R`. At the top of this file, change the desired input configuration to one of the following:
- `config/main.yml`
- `config/youth_protection.yml`
- `config/perinatal.yml`
- `config/elementary_school.yml`
- `config/high_school.yml`

After this change, run the entire file to create the cohort dataset in the scratch folder!


# Details

## Preparation pipeline
The data preparation pipeline has 4 components:

1. Cohort creation. 
    - Selecting the cohort based on filtering criteria. 
    - Adding parent information to the cohort.
    - Adding postal code / region information to the cohort.
    - Writing `01_cohort.rds` to the scratch folder.
2. Predictor creation.
    - Adding parent income and income percentile to the cohort.
    - Adding migration background information to the cohort.
    - Writing `02_predictor.rds` to the scratch folder.
3. Outcome creation.
    - Adding outcomes of children to the cohort.
    - Writing `03_outcomes.rds` to the scratch folder.
4. Post-processing.
    - Selecting variables of interest.
    - Writing `kansenkaart_data.rds` to the scratch folder.


## Package management
The CBS server has specific versions of packages. A package version dump is available in the `resources` folder. For package management, we use `renv`. This ensures the same versions are installed as in CBS.

To use a new package, install it using `install.packages()`, and then run the file `resources/set_cbs_versions.R`. This will ensure that your package exists at cbs (it will warn & remove if it does not) and also that the package is at the right version.
