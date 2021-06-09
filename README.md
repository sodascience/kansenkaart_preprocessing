# Kansenkaart preprocessing

![pipeline.png](pipeline.png)

A reproducible pre-processing pipeline for the Dutch Opportunity atlas. The input is raw microdata from CBS (or a synthetic test-version from our repository [`cbs_validationdata`](https://github.com/sodascience/cbs_validationdata)). The output is several tidy, cleaned up "cohort datasets". The cohort datasets serve as the input to the [kansenkaart analysis](https://github.com/sodascience/kansenkaart_analysis).

## Installation
- Clone this repository to a folder on your machine
- Unzip the latest version of the built cbs data into the `/cbsdata` folder from [here](https://github.com/sodascience/cbs_validationdata/releases) OR edit the file locations in the `config` files to point to the right files.
- Open the `.Rproj` file, install the `renv` package
- Run `renv::restore()` to install the right versions of all dependencies

## Configuration
Several cohorts can be created using this repository, from the raw microdata files at CBS. The configuration files for each of these cohorts can be found in the [`config`](./config) folder. Several selection and/or date values can also be changed there.

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
    - Adding gender information to the cohort.
    - Writing `02_predictor.rds` to the scratch folder.
3. Outcome creation.
    - Adding (socio)economic outcomes to the cohort.
    - Adding education outcomes to the cohort.
    - Adding health outcomes to the cohort.
    - Writing `03_outcomes.rds` to the scratch folder.
4. Post-processing.
    - Selecting variables of interest.
    - Writing `kansenkaart_data.rds` to the scratch folder.



## Package management
The CBS server has specific versions of packages. A package version dump is available in the `resources` folder. For package management, we use `renv`. This ensures the same versions are installed as in CBS.

To use a new package, install it using `install.packages()`, and then run the file `resources/set_cbs_versions.R`. This will ensure that your package exists at cbs (it will warn & remove if it does not) and also that the package is at the right version. 
