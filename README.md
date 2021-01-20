# Kansenkaart pipeline

The processing pipeline for the Dutch Opportunity atlas.

## Structure
The pipeline has 2 main components: `data preparation` and `data analysis`. By separating data preparation and analysis, the analysis code can be rerun on different datasets, for example different countries or cohorts.

### Data preparation
The data preparation pipeline has 4 components. The behaviour of the data preparation pipeline can be tweaked in the `config.yml` file.

1. Cohort creation. 
    - Selecting the cohort based on filtering criteria. 
    - Adding parent information to the cohort.
    - Adding postal code / region information to the cohort.
    - Writing `scratch/01_cohort.rds`.
2. Predictor creation.
    - Adding parent income and income percentile to the cohort.
    - Adding migration background information to the cohort.
    - Adding gender information to the cohort.
    - Writing `scratch/02_predictor.rds`
3. Outcome creation.
    - Adding (socio)economic outcomes to the cohort.
    - Adding education outcomes to the cohort.
    - Adding health outcomes to the cohort.
    - Writing `scratch/03_outcomes.rds`.
4. Post-processing.
    - Selecting variables of interest.
    - Writing `scratch/kansenkaart_data.rds`.

### Data analysis
The data analysis component of the Kansenkaart pipeline takes in the prepared data and outputs everything needed for the kansenkaart website. Here, many regression models are precomputed and predictions are made. 




## Installation
- Clone this repository to a folder on your machine
- Unzip the latest version of the built cbs data into the `/data` folder from [here](https://github.com/sodascience/cbs_validationdata/releases)
- Open the `.Rproj` file, install the `renv` package
- Run `renv::restore()` to install the right versions of all dependencies

### Package management
The CBS server has specific versions of packages. A package version dump is available in the `resources` folder. For package management, we use `renv`. This ensures the same versions are installed as in CBS.

To use a new package, simply install it using `install.packages()`, and then run the file `resources/set_cbs_versions.R`. This will ensure that your package exists at cbs (it will warn & remove if it does not) and also that the package is at the right version. This way, any pipeline work is also certain to work!